package nats_test

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// startStoppable puts a proxy in front of upstream and hands back what Serve returns, for the cases
// where stopping the proxy is the behaviour under test.
func startStoppable(t *testing.T, upstream string) (*recorder, string, <-chan error) {
	t.Helper()

	sink := &recorder{}

	front, err := natsproxy.Listen(t.Context(), "127.0.0.1:0", upstream, sink)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	served := make(chan error, 1)
	go func() { served <- front.Serve(t.Context()) }()

	t.Cleanup(func() {
		if err := front.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return sink, front.Addr(), served
}

// stopped waits for Serve to return and requires it to have stopped as an unsupported bus client.
func stopped(t *testing.T, served <-chan error) error {
	t.Helper()

	select {
	case err := <-served:
		if !errors.Is(err, natsproxy.ErrUnsupportedBus) {
			t.Fatalf("Serve() error = %v, want ErrUnsupportedBus", err)
		}

		return err
	case <-time.After(startupTimeout):
		t.Fatal("Serve() kept serving a bus client the embedded server cannot stand in for")

		return nil
	}
}

// TestServerRequiringTLSStopsTheProxy covers the sign that arrives first: the bus tells the client to
// upgrade, so the rest of the connection will be unreadable before a byte of it is.
func TestServerRequiringTLSStopsTheProxy(t *testing.T) {
	t.Parallel()

	sink, addr, served := startStoppable(t, stubBus(t, `INFO {"tls_required":true}`+"\r\n"))
	conn := dial(t, addr)

	if _, err := io.ReadAll(conn); err != nil {
		t.Fatalf("read from the proxied connection: %v", err)
	}

	if err := stopped(t, served); !strings.Contains(err.Error(), "TLS") {
		t.Errorf("Serve() error = %v, want it to name TLS", err)
	}

	if got := sink.texts(); len(got) != 0 {
		t.Errorf("effects = %q, want none: an unreadable connection is a stop, not an effect", got)
	}
}

// TestClientUpgradingToTLSStopsTheProxy covers the other sign: the bus merely offers TLS and the
// client takes it, so what arrives where a CONNECT belongs is a handshake record.
//
// Stopping is the whole point. A connection the proxy cannot read reports a handler as having
// produced no side effects at all, which reads as "idempotent" — the most dangerous wrong answer
// this tool can give.
func TestClientUpgradingToTLSStopsTheProxy(t *testing.T) {
	t.Parallel()

	sink, addr, served := startStoppable(t, stubBus(t, `INFO {"tls_available":true}`+"\r\n"))
	conn := dial(t, addr)

	// The opening bytes of a TLS handshake record, where a CONNECT would otherwise be.
	if _, err := conn.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x01}); err != nil {
		t.Fatalf("write a handshake record: %v", err)
	}

	if _, err := io.ReadAll(conn); err != nil {
		t.Fatalf("read from the proxied connection: %v", err)
	}

	if err := stopped(t, served); !strings.Contains(err.Error(), "TLS") {
		t.Errorf("Serve() error = %v, want it to name TLS", err)
	}

	if got := sink.texts(); len(got) != 0 {
		t.Errorf("effects = %q, want none", got)
	}
}

// TestAJetStreamDomainRequestStopsTheProxy: the one embedded server has no domain, so a service
// addressing one gets no answer to anything it asks JetStream and never starts.
func TestAJetStreamDomainRequestStopsTheProxy(t *testing.T) {
	t.Parallel()

	_, addr, served := startStoppable(t, embeddedBus(t).Addr().String())

	domained, err := jetstream.NewWithDomain(connect(t, addr), "hub")
	if err != nil {
		t.Fatalf("NewWithDomain() error = %v", err)
	}

	if _, err := domained.AccountInfo(t.Context()); err == nil {
		t.Fatal("AccountInfo() in domain hub succeeded against a server with no domain")
	}

	if err := stopped(t, served); !strings.Contains(err.Error(), "hub") {
		t.Errorf("Serve() error = %v, want it to name the domain hub", err)
	}
}

// TestAReplicatedBucketStopsTheProxy: a single server refuses a replicated bucket, and a service that
// needs one cannot start against it.
func TestAReplicatedBucketStopsTheProxy(t *testing.T) {
	t.Parallel()

	_, addr, served := startStoppable(t, embeddedBus(t).Addr().String())

	proxied, err := jetstream.New(connect(t, addr))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	replicated := jetstream.KeyValueConfig{Bucket: bucket, Replicas: 3}
	if _, createErr := proxied.CreateKeyValue(t.Context(), replicated); createErr == nil {
		t.Fatal("CreateKeyValue() with three replicas succeeded on a single server")
	}

	stop := stopped(t, served)
	for _, want := range []string{"10074", bucket} {
		if !strings.Contains(stop.Error(), want) {
			t.Errorf("Serve() error = %v, want it to name %s", stop, want)
		}
	}
}

// TestABusURLNeedingTLSOrWebsocketsIsRefused: a service configured to reach the bus over TLS or
// websockets cannot reach the embedded server at all, so it is refused before any start — naming the
// variable, never its value, which may carry credentials.
func TestABusURLNeedingTLSOrWebsocketsIsRefused(t *testing.T) {
	t.Parallel()

	busNames := []string{"bus", "nats"}

	rows := []struct {
		value   string
		refused bool
	}{
		{value: "tls://bus:4222", refused: true},
		{value: "ws://user:secret@BUS:8080", refused: true},
		{value: "wss://nats:443", refused: true},
		{value: "nats://a:4222, tls://bus:4222", refused: true},
		{value: "nats://bus:4222"},
		{value: "tls://elsewhere:4222"},
	}

	t.Logf("%d rows", len(rows))

	if len(rows) == 0 {
		t.Fatal("no bus URLs to judge")
	}

	for _, row := range rows {
		err := natsproxy.RefuseURLs(map[string]string{"UNRELATED": "x", "BUS_URL": row.value}, busNames)

		if !row.refused {
			if err != nil {
				t.Errorf("RefuseURLs(%s) error = %v, want it accepted", row.value, err)
			}

			continue
		}

		if !errors.Is(err, natsproxy.ErrUnsupportedBus) || !strings.Contains(err.Error(), "BUS_URL") {
			t.Errorf("RefuseURLs(%s) error = %v, want ErrUnsupportedBus naming BUS_URL", row.value, err)

			continue
		}

		if strings.Contains(err.Error(), "://") {
			t.Errorf("RefuseURLs(%s) error = %v, want the value left out", row.value, err)
		}
	}
}
