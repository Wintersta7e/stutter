package harness

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/relay"
	"github.com/Wintersta7e/stutter/internal/waits"
)

// cacheKey is the one unparsed dependency the attach tests serve.
var cacheKey = EndpointKey("cache", 6379)

// lineServer is an unparsed dependency: it answers every line with its reply, or with nothing when
// the reply is empty, and keeps what it received.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type lineServer struct {
	address  netip.AddrPort
	received strings.Builder
	mu       sync.Mutex
}

func startLineServer(t *testing.T, reply string) *lineServer {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := &lineServer{address: netip.MustParseAddrPort(listener.Addr().String())}

	var served sync.WaitGroup

	served.Go(func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			served.Go(func() { server.serve(conn, reply) })
		}
	})

	t.Cleanup(func() {
		_ = listener.Close()

		served.Wait()
	})

	return server
}

// serve reads lines until the peer goes, answering each one with reply; an empty reply stays silent.
func (l *lineServer) serve(conn net.Conn, reply string) {
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')

		l.mu.Lock()
		l.received.WriteString(line)
		l.mu.Unlock()

		if err != nil {
			return
		}

		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

func (l *lineServer) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.received.String()
}

// fixedUpstreams is an upstream source that always answers the same map.
func fixedUpstreams(upstreams map[string]netip.AddrPort) UpstreamSource {
	return func(context.Context) (map[string]netip.AddrPort, error) { return upstreams, nil }
}

// relayedFixture is a set with one opaque key, a sandbox on it, and an attached start recording into
// a recorder whose window is open, so every effect counts.
type relayedFixture struct {
	set      *ListenerSet
	recorder *effect.Recorder
	observed *egress
	cfg      ListenerConfig
}

func attachFixture(t *testing.T, upstreams map[string]netip.AddrPort) relayedFixture {
	t.Helper()

	cfg := testListenerConfig(t)
	cfg.Postgres = nil
	cfg.Opaque = []string{cacheKey}
	cfg.Upstreams = fixedUpstreams(upstreams)

	set := openTestSet(t, cfg)

	sandbox, err := New(Config{Start: func(context.Context, Addresses) (Consumer, error) {
		return nil, nil //nolint:nilnil // never called.
	}, Listeners: set})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), make([]byte, 32))
	recorder.Open("consumer", 1, nil)

	observed, err := sandbox.observe(t.Context(), recorder, natsproxy.Options{})
	if err != nil {
		t.Fatalf("observe() error = %v", err)
	}

	return relayedFixture{set: set, recorder: recorder, observed: observed, cfg: cfg}
}

// effectCount is every effect the recorder holds.
func effectCount(recorder *effect.Recorder) int {
	return len(recorder.Effects()) + recorder.SetupCount()
}

// TestForeignConnectionsAreCountedAndNeverRecorded is what makes the listeners safe to leave reachable
// from every container on the engine: only the check's own relays are ever served, and nothing any
// other connection sends reaches a proxy.
func TestForeignConnectionsAreCountedAndNeverRecorded(t *testing.T) {
	t.Parallel()

	cache := startLineServer(t, "+OK\n")
	fixture := attachFixture(t, map[string]netip.AddrPort{
		cacheKey: cache.address, KeyBus: cache.address, KeyBusMonitor: cache.address,
	})

	valid := dialKey(t, fixture.set, cacheKey, fixture.cfg.Token, 6379)

	if _, err := io.WriteString(valid, "hello\n"); err != nil {
		t.Fatalf("write: %v", err)
	}

	awaitText(t, cache, "hello\n")
	awaitEffects(t, fixture.recorder, 1)

	strangers(t, fixture)

	// Whatever became of them, nothing they sent may be recorded; the counts are checked after.
	settled := settleCounts(fixture.set, ListenerCounts{Foreign: 4})

	if got := effectCount(fixture.recorder); got != 1 {
		t.Errorf("wrong token: recorder holds %d effects, want 1", got)
	}

	if settled != (ListenerCounts{Foreign: 4}) {
		t.Fatalf("counts = %+v, want foreign=4", settled)
	}

	// The service goes before the start detaches, as it does in every run.
	_ = valid.Close()

	if err := fixture.observed.close(t.Context()); err != nil {
		t.Fatalf("detach: %v", err)
	}

	late := dialKey(t, fixture.set, cacheKey, fixture.cfg.Token, 6379)
	_, _ = io.WriteString(late, "late\n") //nolint:errcheck // the set refuses it either way.

	awaitCounts(t, fixture.set, ListenerCounts{Foreign: 4, Unattached: 1})

	if got := effectCount(fixture.recorder); got != 1 {
		t.Errorf("after the detach: recorder holds %d effects, want 1", got)
	}

	if text := cache.text(); text != "hello\n" {
		t.Errorf("the upstream received %q, want exactly %q", text, "hello\n")
	}

	counts := fixture.set.Counts()
	t.Logf("foreign=%d unattached=%d", counts.Foreign, counts.Unattached)
}

// strangers sends the four kinds of connection that are not this check's relay: wrong magic, wrong
// token, a preamble cut short and left idle past its bound, and a valid preamble for a port the key
// does not serve. Each writes a line a proxy would record.
func strangers(t *testing.T, fixture relayedFixture) {
	t.Helper()

	port, _ := fixture.set.Port(cacheKey)
	wrongToken := fixture.cfg.Token
	wrongToken[0] ^= 0xff

	preambles := [][]byte{
		append([]byte("HTTP/1.1"), make([]byte, relay.PreambleSize-8)...),
		preamble(t, wrongToken, 6379),
		preamble(t, fixture.cfg.Token, 6379)[:10],
		preamble(t, fixture.cfg.Token, 6380),
	}

	for _, opening := range preambles {
		var dialer net.Dialer

		conn, err := dialer.DialContext(t.Context(), "tcp4", netip.AddrPortFrom(loopbackAddr, port).String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		t.Cleanup(func() { _ = conn.Close() })

		if _, err := conn.Write(opening); err != nil {
			t.Fatalf("write: %v", err)
		}

		// The short preamble stays idle, so the set gives up on it at the preamble bound.
		if len(opening) == relay.PreambleSize {
			_, _ = io.WriteString(conn, "stranger\n") //nolint:errcheck // the set refuses it either way.
		}
	}
}

func preamble(t *testing.T, token relay.Token, port uint16) []byte {
	t.Helper()

	var buffer strings.Builder

	if err := relay.WritePreamble(&buffer, token, port); err != nil {
		t.Fatalf("WritePreamble() error = %v", err)
	}

	return []byte(buffer.String())
}

func awaitText(t *testing.T, server *lineServer, want string) {
	t.Helper()

	deadline := time.Now().Add(setWait)

	for time.Now().Before(deadline) && server.text() != want {
		time.Sleep(10 * time.Millisecond)
	}

	if got := server.text(); got != want {
		t.Fatalf("the upstream received %q, want %q", got, want)
	}
}

func awaitEffects(t *testing.T, recorder *effect.Recorder, want int) {
	t.Helper()

	deadline := time.Now().Add(setWait)

	for time.Now().Before(deadline) && effectCount(recorder) < want {
		time.Sleep(10 * time.Millisecond)
	}

	if got := effectCount(recorder); got != want {
		t.Fatalf("recorder holds %d effects, want %d", got, want)
	}
}

func TestASecondAttachIsAnInternalError(t *testing.T) {
	t.Parallel()

	cache := startLineServer(t, "+OK\n")
	fixture := attachFixture(t, map[string]netip.AddrPort{
		cacheKey: cache.address, KeyBus: cache.address, KeyBusMonitor: cache.address,
	})

	if _, err := fixture.set.attach(t.Context()); !errors.Is(err, errAttached) {
		t.Errorf("second attach: err = %v, want errAttached", err)
	}

	if err := fixture.observed.close(t.Context()); err != nil {
		t.Fatalf("detach: %v", err)
	}

	again, err := fixture.set.attach(t.Context())
	if err != nil {
		t.Fatalf("attach after a detach: %v", err)
	}

	again.release()
}

// TestAnOmittedUpstreamKeyIsASetupError refuses a start whose source forgot an endpoint: its proxy
// would have nowhere to dial, and the service would look as if it never used the dependency.
func TestAnOmittedUpstreamKeyIsASetupError(t *testing.T) {
	t.Parallel()

	cfg := testListenerConfig(t)
	cfg.Upstreams = fixedUpstreams(map[string]netip.AddrPort{
		EndpointKey("db", 5432): netip.MustParseAddrPort("127.0.0.1:5432"),
		KeyBus:                  netip.MustParseAddrPort("127.0.0.1:4222"),
		KeyBusMonitor:           netip.MustParseAddrPort("127.0.0.1:8222"),
	})

	set := openTestSet(t, cfg)

	if _, err := set.attach(t.Context()); err == nil || !strings.Contains(err.Error(), cacheKey) {
		t.Errorf("attach() = %v, want a setup error naming %s", err, cacheKey)
	}

	if set.current() != nil {
		t.Error("a refused attach left the set attached")
	}
}

// TestAnUnservedKeyStopsTheStart fails the start loudly when it is handed a connection nobody serves,
// rather than resetting it quietly: a quiet reset reads as a dependency the service never used. Every
// key the set opens is served by a start, so the connection is handed over directly.
func TestAnUnservedKeyStopsTheStart(t *testing.T) {
	t.Parallel()

	const unserved = "unserved:7"

	cache := startLineServer(t, "+OK\n")
	fixture := attachFixture(t, map[string]netip.AddrPort{
		cacheKey: cache.address, KeyBus: cache.address, KeyBusMonitor: cache.address,
	})

	fixture.observed.attached.take(unserved, relayedConn(t, fixture.cfg.Token))

	select {
	case err := <-fixture.observed.served:
		// Put back for the close below, which drains one result per entry.
		fixture.observed.served <- err

		if err == nil || !strings.Contains(err.Error(), keyListeners+":") || !strings.Contains(err.Error(), unserved) {
			t.Errorf("the listeners entry ended with %v, want an error naming %s", err, unserved)
		}
	case <-time.After(setWait):
		t.Fatal("a connection on an unserved key did not stop the start")
	}

	if err := fixture.observed.close(t.Context()); err == nil || !strings.Contains(err.Error(), unserved) {
		t.Errorf("close() = %v, want the unserved key named", err)
	}
}

// relayedConn is a loopback connection opened with the check's preamble, as the set hands one over.
func relayedConn(t *testing.T, token relay.Token) *relay.Conn {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	defer func() { _ = listener.Close() }()

	dialled, err := (&net.Dialer{}).DialContext(t.Context(), "tcp4", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { _ = dialled.Close() })

	if preambleErr := relay.WritePreamble(dialled, token, 7); preambleErr != nil {
		t.Fatalf("write the preamble: %v", preambleErr)
	}

	accepted, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}

	conn, err := relay.Accept(accepted, token)
	if err != nil {
		t.Fatalf("read the preamble: %v", err)
	}

	return conn
}

// TestDetachIsBounded keeps a stuck proxy from hanging the check: past the drain bound its connections
// are reset and the start fails naming the endpoint.
func TestDetachIsBounded(t *testing.T) {
	t.Parallel()

	silent := startLineServer(t, "")
	fixture := attachFixture(t, map[string]netip.AddrPort{
		cacheKey: silent.address, KeyBus: silent.address, KeyBusMonitor: silent.address,
	})

	held := dialKey(t, fixture.set, cacheKey, fixture.cfg.Token, 6379)

	if _, err := io.WriteString(held, "hold\n"); err != nil {
		t.Fatalf("write: %v", err)
	}

	awaitText(t, silent, "hold\n")

	began := time.Now()
	err := fixture.observed.close(t.Context())
	elapsed := time.Since(began)

	if err == nil || !strings.Contains(err.Error(), cacheKey) {
		t.Errorf("detach = %v, want an error naming %s", err, cacheKey)
	}

	if elapsed > waits.ProxyDrain+500*time.Millisecond {
		t.Errorf("detach took %s, want within %s", elapsed, waits.ProxyDrain+500*time.Millisecond)
	}
}

// TestTheBusMonitorIsPipedUnrecorded lets a service read the bus's monitoring endpoint, as it may on
// startup, without its request ever becoming an effect.
func TestTheBusMonitorIsPipedUnrecorded(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	cache := startLineServer(t, "+OK\n")
	fixture := attachFixture(t, map[string]netip.AddrPort{
		cacheKey:      cache.address,
		KeyBus:        netip.MustParseAddrPort(strings.TrimPrefix(store.URL(), "nats://")),
		KeyBusMonitor: store.MonitorAddr(),
	})

	conn := dialKey(t, fixture.set, KeyBusMonitor, fixture.cfg.Token, MonitorPort)

	if _, writeErr := io.WriteString(conn, "GET /varz HTTP/1.0\r\nHost: monitor\r\n\r\n"); writeErr != nil {
		t.Fatalf("write: %v", writeErr)
	}

	if deadlineErr := conn.SetReadDeadline(time.Now().Add(setWait)); deadlineErr != nil {
		t.Fatalf("SetReadDeadline() error = %v", deadlineErr)
	}

	body, err := io.ReadAll(conn)
	if err != nil || !strings.Contains(string(body), `"server_id"`) {
		t.Fatalf("GET /varz through the monitor pipe = %.200q, %v; want the server's JSON", body, err)
	}

	if got := effectCount(fixture.recorder); got != 0 {
		t.Errorf("the monitor request was recorded: %d effects, want 0", got)
	}

	// The service goes before the start detaches, as it does in every run.
	_ = conn.Close()

	if err := fixture.observed.close(t.Context()); err != nil {
		t.Errorf("detach: %v", err)
	}
}
