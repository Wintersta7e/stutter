package harness

import (
	"context"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/relay"
)

// setWait bounds every wait these tests make on the listener set.
const setWait = 5 * time.Second

var loopbackAddr = netip.MustParseAddr("127.0.0.1")

func mintToken(t *testing.T) relay.Token {
	t.Helper()

	token, err := relay.NewToken()
	if err != nil {
		t.Fatalf("NewToken() error = %v", err)
	}

	return token
}

// noUpstreams is an upstream source for tests that never attach.
func noUpstreams(context.Context) (map[string]netip.AddrPort, error) {
	return map[string]netip.AddrPort{}, nil
}

// testListenerConfig is a complete configuration: one Postgres key, one opaque key, one bus port.
func testListenerConfig(t *testing.T) ListenerConfig {
	t.Helper()

	return ListenerConfig{
		Upstreams: noUpstreams,
		Postgres:  []string{EndpointKey("db", 5432)},
		Opaque:    []string{EndpointKey("cache", 6379)},
		Bus:       []uint16{4222},
		Bind:      loopbackAddr,
		Token:     mintToken(t),
		Mode:      ModeHostAlias,
	}
}

func openTestSet(t *testing.T, cfg ListenerConfig) *ListenerSet {
	t.Helper()

	set, err := OpenListeners(t.Context(), cfg)
	if err != nil {
		t.Fatalf("OpenListeners() error = %v", err)
	}

	t.Cleanup(func() {
		if err := set.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return set
}

// dialKey connects to one of the set's listeners and sends a preamble for port, with token.
func dialKey(t *testing.T, set *ListenerSet, key string, token relay.Token, port uint16) net.Conn {
	t.Helper()

	listened, open := set.Port(key)
	if !open {
		t.Fatalf("the set has no %s listener", key)
	}

	var dialer net.Dialer

	conn, err := dialer.DialContext(t.Context(), "tcp4", netip.AddrPortFrom(loopbackAddr, listened).String())
	if err != nil {
		t.Fatalf("dial %s: %v", key, err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	if err := relay.WritePreamble(conn, token, port); err != nil {
		t.Fatalf("write the preamble: %v", err)
	}

	return conn
}

// awaitCounts waits until the set's counts reach want.
func awaitCounts(t *testing.T, set *ListenerSet, want ListenerCounts) {
	t.Helper()

	deadline := time.Now().Add(setWait)

	for time.Now().Before(deadline) {
		if set.Counts() == want {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("counts = %+v, want %+v", set.Counts(), want)
}

// TestTheListenerSetOpensEveryKeyOnBindHost holds the set to one listener per key, every one on the
// address it was given and never a wildcard: the listeners are reachable from every container on the
// engine, so where they listen is the whole of their exposure.
func TestTheListenerSetOpensEveryKeyOnBindHost(t *testing.T) {
	t.Parallel()

	cfg := testListenerConfig(t)
	set := openTestSet(t, cfg)

	keys := []string{
		EndpointKey("db", 5432), EndpointKey("cache", 6379),
		KeyBus, KeyBusMonitor, KeyHTTP, KeyHTTPS, KeyCatchAll, KeyDNSSignal, KeyVerify,
	}

	for _, key := range keys {
		if _, open := set.Port(key); !open {
			t.Errorf("no listener for %s", key)

			continue
		}

		bound := netip.MustParseAddrPort(set.endpoints[key].listener.Addr().String())
		if bound.Addr() != cfg.Bind {
			t.Errorf("%s listens on %s, want %s", key, bound, cfg.Bind)
		}
	}

	if _, open := set.Port(KeySeedBus); open {
		t.Error("the seed-bus listener is open before the seed phase")
	}

	t.Logf("keys checked: %d", len(keys))
}

func TestAValidConnectionWithNothingAttachedIsUnattached(t *testing.T) {
	t.Parallel()

	cfg := testListenerConfig(t)
	set := openTestSet(t, cfg)

	conn := dialKey(t, set, EndpointKey("cache", 6379), cfg.Token, 6379)

	awaitCounts(t, set, ListenerCounts{Unattached: 1})

	if err := conn.SetReadDeadline(time.Now().Add(setWait)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	if read, err := conn.Read(make([]byte, 1)); read != 0 || err == nil {
		t.Errorf("the unattached connection was not closed: read %d, %v", read, err)
	}
}

// TestTheVerifyListenerAcksOnlyAProbe is the verifier's whole conversation with the host: a probe on
// the verification listener gets the magic back, and a probe anywhere else is a stranger.
func TestTheVerifyListenerAcksOnlyAProbe(t *testing.T) {
	t.Parallel()

	cfg := testListenerConfig(t)
	set := openTestSet(t, cfg)

	probe := dialKey(t, set, KeyVerify, cfg.Token, 0)

	if err := probe.SetReadDeadline(time.Now().Add(setWait)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	ack := make([]byte, 4)
	if _, err := io.ReadFull(probe, ack); err != nil || string(ack) != "STU\x01" {
		t.Errorf("verify answered % x, %v; want the magic", ack, err)
	}

	dialKey(t, set, KeyBus, cfg.Token, 0)
	awaitCounts(t, set, ListenerCounts{Foreign: 1})

	verifyPort, _ := set.Port(KeyVerify)

	if err := set.CloseVerify(); err != nil {
		t.Fatalf("CloseVerify() error = %v", err)
	}

	var dialer net.Dialer

	if conn, err := dialer.DialContext(
		t.Context(),
		"tcp4",
		netip.AddrPortFrom(loopbackAddr, verifyPort).String(),
	); err == nil {
		_ = conn.Close()

		t.Error("the verification listener still accepts after CloseVerify")
	}

	if _, open := set.Port(KeyVerify); open {
		t.Error("Port(verify) still reports a listener after CloseVerify")
	}
}

func TestOpenListenersRefusesAnIncompleteConfig(t *testing.T) {
	t.Parallel()

	cases := map[string]func(*ListenerConfig){
		"no upstream source":     func(c *ListenerConfig) { c.Upstreams = nil },
		"no mode":                func(c *ListenerConfig) { c.Mode = 0 },
		"no bind address":        func(c *ListenerConfig) { c.Bind = netip.Addr{} },
		"a wildcard bind":        func(c *ListenerConfig) { c.Bind = netip.IPv4Unspecified() },
		"an IPv6 bind":           func(c *ListenerConfig) { c.Bind = netip.IPv6Loopback() },
		"a zero token":           func(c *ListenerConfig) { c.Token = relay.Token{} },
		"no bus port":            func(c *ListenerConfig) { c.Bus = nil },
		"bus port 0":             func(c *ListenerConfig) { c.Bus = []uint16{0} },
		"the monitor port":       func(c *ListenerConfig) { c.Bus = []uint16{4222, MonitorPort} },
		"a malformed pg key":     func(c *ListenerConfig) { c.Postgres = []string{"db"} },
		"a malformed opaque key": func(c *ListenerConfig) { c.Opaque = []string{"cache:0"} },
		"a key in both lists":    func(c *ListenerConfig) { c.Opaque = append(c.Opaque, c.Postgres[0]) },
		"a key twice":            func(c *ListenerConfig) { c.Postgres = append(c.Postgres, c.Postgres[0]) },
	}

	for name, spoil := range cases {
		cfg := testListenerConfig(t)
		spoil(&cfg)

		set, err := OpenListeners(t.Context(), cfg)
		if err == nil {
			_ = set.Close(context.Background())

			t.Errorf("%s: OpenListeners() succeeded, want a refusal", name)
		}
	}

	t.Logf("refusals checked: %d", len(cases))
}

// TestNewRefusesListenersBesideGoCallerFields keeps one owner for every endpoint: a sandbox on the
// invocation listeners takes its addresses, its host and its authority from the set.
func TestNewRefusesListenersBesideGoCallerFields(t *testing.T) {
	t.Parallel()

	set := openTestSet(t, testListenerConfig(t))
	start := func(context.Context, Addresses) (Consumer, error) { return nil, nil }  //nolint:nilnil // never called.
	connect := func(context.Context, Addresses) (Service, error) { return nil, nil } //nolint:nilnil // never called.

	cases := map[string]Config{
		"PostgresDSN":   {Start: start, Listeners: set, PostgresDSN: "postgres://127.0.0.1:5432/db"},
		"Opaque":        {Start: start, Listeners: set, Opaque: map[string]string{"cache": "127.0.0.1:6379"}},
		"BindHost":      {Start: start, Listeners: set, BindHost: "127.0.0.1"},
		"AdvertiseHost": {Start: start, Listeners: set, AdvertiseHost: "127.0.0.1"},
		"Connect":       {Connect: connect, Listeners: set},
		"HTTPHost":      {Start: start, Listeners: set, HTTPHost: "api.test"},
	}

	for field, cfg := range cases {
		_, err := New(cfg)
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("Listeners beside %s: err = %v, want a refusal naming %s", field, err, field)
		}
	}
}
