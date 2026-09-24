package harness_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/harness"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
)

// tunnelRequest asks the cleartext stub for a tunnel, which hides the request inside it.
const tunnelRequest = "CONNECT rates.example.test:443 HTTP/1.1\r\nHost: rates.example.test:443\r\n\r\n"

// dialStub opens a connection to the stub's cleartext entry.
func dialStub(ctx context.Context, at harness.Addresses) (net.Conn, error) {
	var dialer net.Dialer

	return dialer.DialContext(ctx, "tcp", strings.TrimPrefix(at.HTTP, "http://"))
}

// stoppedOnEgress reports whether err carries an egress-policy stop.
func stoppedOnEgress(err error) bool {
	stop, found := errors.AsType[*httpproxy.EgressStop](err)

	return found && stop != nil
}

// tunnels is a service that asks the stub for a tunnel as it starts, then idles.
func tunnels(ctx context.Context, _ jetstream.JetStream, _ *nats.Conn, at harness.Addresses) int {
	stub, err := dialStub(ctx, at)
	if err != nil {
		return 2
	}

	defer func() { _ = stub.Close() }()

	if _, err := stub.Write([]byte(tunnelRequest)); err != nil {
		return 2
	}

	return idle(ctx)
}

// createsThenTunnels is a service that creates ORDERS on a connection already open to the stub, and asks
// for the tunnel straight after, so the stop lands once the stream exists.
func createsThenTunnels(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, at harness.Addresses) int {
	stub, err := dialStub(ctx, at)
	if err != nil {
		return 2
	}

	defer func() { _ = stub.Close() }()

	if createOrders(ctx, js) != nil {
		return 2
	}

	if _, err := stub.Write([]byte(tunnelRequest)); err != nil {
		return 2
	}

	return idle(ctx)
}

// TestAnEgressStopEndsTheProbeStartWithoutAnExit: an egress-policy stop before the stream exists ends
// the probe start as a target exit does — it is expected to fail, and ownership is read at that moment.
func TestAnEgressStopEndsTheProbeStartWithoutAnExit(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t)
	cfg := startConfig(store, checkpoint, &fakeService{script: tunnels})
	began := time.Now()

	probed, err := harness.ProbeStart(startContext(t), cfg)
	if err != nil {
		t.Fatalf("ProbeStart() error = %v", err)
	}

	elapsed := time.Since(began)
	t.Logf("the probe start ended after %s", elapsed)

	if probed.Created || probed.Discovery != nil {
		t.Errorf("probe = %+v, want the stream not created and no discovery", probed)
	}

	if elapsed >= cfg.Startup/2 {
		t.Errorf("the probe start ended after %s, want it inside half the %s startup limit", elapsed, cfg.Startup)
	}
}

// TestAnEgressStopAfterTheStreamExistsIsASetupError: once the stream exists the start is discovery, and
// an egress-policy stop in discovery stops the check.
func TestAnEgressStopAfterTheStreamExistsIsASetupError(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t)

	cfg := startConfig(store, checkpoint, &fakeService{script: createsThenTunnels})

	_, err := harness.ProbeStart(startContext(t), cfg)
	if !stoppedOnEgress(err) {
		t.Fatalf("ProbeStart() error = %v, want an egress stop", err)
	}
}

// TestAnEgressStopDuringDiscoveryIsASetupError: an egress-policy stop in discovery stops the check.
func TestAnEgressStopDuringDiscoveryIsASetupError(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))

	_, err := harness.Discover(startContext(t), startConfig(store, checkpoint, &fakeService{script: tunnels}))
	if !stoppedOnEgress(err) {
		t.Fatalf("Discover() error = %v, want an egress stop", err)
	}
}

// TestDiscoveryMarksTheTeardownPointBeforeRemovingTheService: a TLS connection the service holds open
// without a request, and closes only as it is removed, hid nothing. Discovery marks the teardown point
// before removing the service, so that close is the service going away, not a client that could not
// talk to the stub.
func TestDiscoveryMarksTheTeardownPointBeforeRemovingTheService(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{script: holdsTLS}

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	if got := names(found.Consumers); !slices.Equal(got, []string{reserve}) {
		t.Errorf("consumers = %q, want [reserve]", got)
	}
}

// holdsTLS is a service that completes a TLS handshake with the stub, trusting its authority, sends no
// request, creates durable reserve and holds the connection until it is stopped.
func holdsTLS(ctx context.Context, js jetstream.JetStream, conn *nats.Conn, at harness.Addresses) int {
	trusted := x509.NewCertPool()
	if !trusted.AppendCertsFromPEM(at.HTTPCACert) {
		return 2
	}

	dialer := tls.Dialer{Config: &tls.Config{
		ServerName: "rates.example.test",
		RootCAs:    trusted,
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"http/1.1"},
	}}

	held, err := dialer.DialContext(ctx, "tcp", strings.TrimPrefix(at.HTTPS, "https://"))
	if err != nil {
		return 2
	}

	defer func() { _ = held.Close() }()

	return idleAfter(reserve)(ctx, js, conn, at)
}
