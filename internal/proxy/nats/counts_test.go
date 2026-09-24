package nats_test

import (
	"bufio"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestARefusedRequestKeepsItsCodeAndDescription: a service whose startup the bus refuses never
// creates its consumer, and the refusal's code and description are what name the cause.
func TestARefusedRequestKeepsItsCodeAndDescription(t *testing.T) {
	t.Parallel()

	bus := embeddedBus(t)

	setup, err := jetstream.New(connect(t, bus.Addr().String()))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	existing := jetstream.StreamConfig{Name: "S", Subjects: []string{"s.>"}}
	if _, createErr := setup.CreateStream(t.Context(), existing); createErr != nil {
		t.Fatalf("CreateStream() error = %v", createErr)
	}

	sink, addr := startProxy(t, bus.Addr().String())

	proxied, err := jetstream.New(connect(t, addr))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	conflicting := jetstream.StreamConfig{Name: "S", Subjects: []string{"other.>"}}
	if _, err := proxied.CreateStream(t.Context(), conflicting); err == nil {
		t.Fatal("CreateStream() over an existing stream with other subjects succeeded; nothing was refused")
	}

	got := sink.declinedRefusals()
	if len(got) != 1 {
		t.Fatalf("refusals = %+v, want exactly one", got)
	}

	refusal := got[0]
	if refusal.Subject != "$JS.API.STREAM.CREATE.S" || refusal.ErrCode != 10058 || refusal.Code != 400 ||
		refusal.Description == "" {
		t.Errorf("refusal = %+v, want $JS.API.STREAM.CREATE.S, 400, 10058 and a description", refusal)
	}
}

// TestANoRespondersAnswerStaysAnEffect: a request nothing answered was still made, so it is kept as
// the service's work; the answer is counted, not read as a refusal.
func TestANoRespondersAnswerStaysAnEffect(t *testing.T) {
	t.Parallel()

	sink, addr := startProxy(t, embeddedBus(t).Addr().String())
	conn := connect(t, addr)

	_, err := conn.Request("nobody.listens", []byte(payload), startupTimeout)
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("Request() error = %v, want no responders", err)
	}

	flush(t, conn)

	if got := sink.matching("publish subject=nobody.listens"); len(got) != 1 {
		t.Errorf("effects = %q, want the request recorded once", sink.texts())
	}

	if got := sink.refusals(); len(got) != 0 {
		t.Errorf("rejected = %q, want none: nobody answering is not a refusal", got)
	}

	if got := sink.noResponderCount(); got != 1 {
		t.Errorf("no-responder answers = %d, want 1", got)
	}
}

// TestAConnectAndCloseIsCountedNotStopped: a client that hangs up after the greeting is a client that
// wanted TLS — or a script waiting for the port to open, which looks the same byte for byte. It is
// counted and the proxy carries on.
func TestAConnectAndCloseIsCountedNotStopped(t *testing.T) {
	t.Parallel()

	sink, addr := startProxy(t, embeddedBus(t).Addr().String())
	conn := dial(t, addr)

	if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
		t.Fatalf("read the greeting: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for sink.closedAfterInfoCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := sink.closedAfterInfoCount(); got != 1 {
		t.Errorf("connections closed after the greeting = %d, want 1", got)
	}

	if got := sink.texts(); len(got) != 0 {
		t.Errorf("effects = %q, want none", got)
	}
}
