package harness

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/effect"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/relay"
)

// DNS query types the signal carries, as the relay writes them.
const (
	queryTXT uint16 = 16
	queryMX  uint16 = 15
	querySRV uint16 = 33
)

// signalQuery writes one signal record, as the stub relay does before answering a query.
func signalQuery(t *testing.T, conn net.Conn, query relay.Query) {
	t.Helper()

	if err := relay.WriteQuery(conn, query); err != nil {
		t.Fatalf("write the signal record: %v", err)
	}
}

// stillOpen fails the test if the set has closed conn: a read either times out, or the peer is gone.
func stillOpen(t *testing.T, conn net.Conn, when string) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	_, err := conn.Read(make([]byte, 1))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("signal connection closed %s: read %v", when, err)
	}
}

// attachTo attaches a fresh start to set, recording into a recorder of its own.
func attachTo(t *testing.T, set *ListenerSet) *egress {
	t.Helper()

	sandbox, err := New(Config{Start: func(context.Context, Addresses) (Consumer, error) {
		return nil, nil //nolint:nilnil // never called.
	}, Listeners: set})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), make([]byte, 32))

	observed, err := sandbox.observe(t.Context(), recorder, natsproxy.Options{})
	if err != nil {
		t.Fatalf("observe() error = %v", err)
	}

	return observed
}

// awaitSignalled waits until the start has had want records attributed to it.
func awaitSignalled(t *testing.T, observed *egress, want int64) {
	t.Helper()

	deadline := time.Now().Add(setWait)
	for observed.attached.signalled.Load() < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := observed.attached.signalled.Load(); got != want {
		t.Fatalf("records attributed to the start = %d, want %d", got, want)
	}
}

// TestTheSignalConnectionOutlivesAttach holds the one signal connection a stub relay opens for its whole
// life: it is made before any start attaches, and every start after uses it. A set that closed it
// between starts would lose every later query and never stop on an SRV lookup.
func TestTheSignalConnectionOutlivesAttach(t *testing.T) {
	t.Parallel()

	cache := startLineServer(t, "+OK\n")
	cfg := testListenerConfig(t)
	cfg.Postgres = nil
	cfg.Opaque = []string{cacheKey}
	cfg.Upstreams = fixedUpstreams(map[string]netip.AddrPort{
		cacheKey: cache.address, KeyBus: cache.address, KeyBusMonitor: cache.address,
	})

	set := openTestSet(t, cfg)
	signal := dialKey(t, set, KeyDNSSignal, cfg.Token, relay.DNSPort)

	signalQuery(t, signal, relay.Query{Name: "before.test.", Type: queryTXT})
	awaitCounts(t, set, ListenerCounts{Unattached: 1})
	stillOpen(t, signal, "before the first attach")

	first := attachTo(t, set)

	signalQuery(t, signal, relay.Query{Name: "first.test.", Type: queryMX})
	awaitSignalled(t, first, 1)

	if err := first.close(t.Context()); err != nil {
		t.Fatalf("detach the first start: %v", err)
	}

	stillOpen(t, signal, "after the first detach")

	second := attachTo(t, set)

	signalQuery(t, signal, relay.Query{Name: "second.test.", Type: queryTXT})
	awaitSignalled(t, second, 1)

	if err := second.close(t.Context()); err != nil {
		t.Fatalf("detach the second start: %v", err)
	}

	want := []relay.Query{
		{Name: "before.test.", Type: queryTXT},
		{Name: "first.test.", Type: queryMX},
		{Name: "second.test.", Type: queryTXT},
	}

	if got := set.Queries(); !slices.Equal(got, want) {
		t.Errorf("Queries() = %v, want %v", got, want)
	}
}

// TestAnSRVQueryStopsTheStart stops the start on an SRV lookup: an SRV record names a service Stutter
// cannot stand in for, and the connection that follows it would go unseen.
func TestAnSRVQueryStopsTheStart(t *testing.T) {
	t.Parallel()

	cache := startLineServer(t, "+OK\n")
	fixture := attachFixture(t, map[string]netip.AddrPort{
		cacheKey: cache.address, KeyBus: cache.address, KeyBusMonitor: cache.address,
	})

	signal := dialKey(t, fixture.set, KeyDNSSignal, fixture.cfg.Token, relay.DNSPort)
	signalQuery(t, signal, relay.Query{Name: "_x._tcp.example.test.", Type: querySRV})

	select {
	case err := <-fixture.observed.served:
		// Put back for the close below, which drains one result per entry.
		fixture.observed.served <- err

		if err == nil || !strings.Contains(err.Error(), "_x._tcp.example.test.") {
			t.Errorf("the listeners entry ended with %v, want an error naming the SRV query", err)
		}
	case <-time.After(setWait):
		t.Fatal("an SRV query did not stop the start")
	}

	records := fixture.observed.attached.signalled.Load()
	t.Logf("signal records for the start: %d", records)

	if records < 1 {
		t.Error("no signal record was attributed to the start")
	}

	_ = signal.Close()

	if err := fixture.observed.close(t.Context()); err == nil {
		t.Error("the start's close did not report the SRV stop")
	}
}
