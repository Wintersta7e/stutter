package harness_test

import (
	"context"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/relay"
)

// busClientPort is the bus port the service dials, as a relay's preamble names it.
const busClientPort = 4222

// startSet opens the listener set a start attaches to, in front of store. Its upstream source reads the
// bus's addresses at every attach: a restore brings the bus back on new ports.
func startSet(t *testing.T, store *corpus.Corpus) (*harness.ListenerSet, relay.Token) {
	t.Helper()

	token, err := relay.NewToken()
	if err != nil {
		t.Fatalf("NewToken() error = %v", err)
	}

	set, err := harness.OpenListeners(t.Context(), harness.ListenerConfig{
		Bind: localhost,
		Upstreams: func(context.Context) (map[string]netip.AddrPort, error) {
			return map[string]netip.AddrPort{
				harness.KeyBus:        netip.MustParseAddrPort(strings.TrimPrefix(store.URL(), "nats://")),
				harness.KeyBusMonitor: store.MonitorAddr(),
			}, nil
		},
		Bus:   []uint16{busClientPort},
		Token: token,
		Mode:  harness.ModeHostAlias,
	})
	if err != nil {
		t.Fatalf("OpenListeners() error = %v", err)
	}

	t.Cleanup(func() {
		if err := set.Close(context.Background()); err != nil {
			t.Errorf("close the listener set: %v", err)
		}
	})

	return set, token
}

// relayDialer reaches the bus the way a relayed service does: through the set's bus listener, opening
// with the check's preamble. The address the client asks for is ignored, as a relay ignores it.
type relayDialer struct {
	set   *harness.ListenerSet
	token relay.Token
}

func (d relayDialer) Dial(string, string) (net.Conn, error) {
	port, _ := d.set.Port(harness.KeyBus)

	var dialer net.Dialer

	conn, err := dialer.DialContext(context.Background(), "tcp",
		net.JoinHostPort(localhost.String(), strconv.Itoa(int(port))))
	if err != nil {
		return nil, err
	}

	if err := relay.WritePreamble(conn, d.token, busClientPort); err != nil {
		_ = conn.Close()

		return nil, err
	}

	return conn, nil
}

// relayedService is a service that reaches the bus only through the set, and creates ORDERS unless the
// stream is already there, and durable reserve.
func relayedService(set *harness.ListenerSet, token relay.Token) *fakeService {
	dialer := relayDialer{set: set, token: token}

	return &fakeService{
		dial: func(harness.Addresses) (*nats.Conn, error) {
			return nats.Connect("nats://bus:4222", nats.SetCustomDialer(dialer), nats.NoReconnect())
		},
		script: func(ctx context.Context, js jetstream.JetStream, conn *nats.Conn, at harness.Addresses) int {
			if _, err := js.Stream(ctx, "ORDERS"); err != nil && createOrders(ctx, js) != nil {
				return 2
			}

			return idleAfter(reserve)(ctx, js, conn, at)
		},
	}
}

// relayedConfig is a start's configuration on the set: every endpoint is the set's.
func relayedConfig(store *corpus.Corpus, checkpoint *corpus.Checkpoint, set *harness.ListenerSet,
	service *fakeService,
) harness.Config {
	cfg := startConfig(store, checkpoint, service)
	cfg.Listeners = set

	return cfg
}

// assertDetached checks nothing foreign reached the set, then that a valid connection after the start
// is refused as unattached: the start detached at its end.
func assertDetached(t *testing.T, set *harness.ListenerSet, token relay.Token) {
	t.Helper()

	if foreign := set.Counts().Foreign; foreign != 0 {
		t.Errorf("foreign connections: %d, want 0", foreign)
	}

	late, err := relayDialer{set: set, token: token}.Dial("tcp", "bus:4222")
	if err != nil {
		t.Fatalf("dial the set after the start: %v", err)
	}

	defer func() { _ = late.Close() }()

	deadline := time.Now().Add(5 * time.Second)
	for set.Counts().Unattached == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	counts := set.Counts()
	t.Logf("foreign: %d, unattached: %d", counts.Foreign, counts.Unattached)

	if counts.Unattached < 1 {
		t.Errorf("unattached connections: %d, want at least 1 once the start detached", counts.Unattached)
	}
}

// TestAProbeStartAttachesThroughTheListenerSet: the probe start reaches a relayed service exactly as a
// run does, through the invocation's listener set, and lets go of it when it ends.
func TestAProbeStartAttachesThroughTheListenerSet(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t)
	set, token := startSet(t, store)

	cfg := relayedConfig(store, checkpoint, set, relayedService(set, token))

	probed, err := harness.ProbeStart(startContext(t), cfg)
	if err != nil {
		t.Fatalf("ProbeStart() error = %v", err)
	}

	if !probed.Created || probed.Discovery == nil {
		t.Fatalf("probe = %+v, want the stream created and a discovery", probed)
	}

	if got := names(probed.Discovery.Consumers); !slices.Equal(got, []string{reserve}) {
		t.Errorf("consumers = %q, want [reserve]", got)
	}

	assertDetached(t, set, token)
}

// TestDiscoveryAttachesThroughTheListenerSet: discovery does too.
func TestDiscoveryAttachesThroughTheListenerSet(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	set, token := startSet(t, store)

	cfg := relayedConfig(store, checkpoint, set, relayedService(set, token))

	found, err := harness.Discover(startContext(t), cfg)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	if got := names(found.Consumers); !slices.Equal(got, []string{reserve}) {
		t.Errorf("consumers = %q, want [reserve]", got)
	}

	assertDetached(t, set, token)
}
