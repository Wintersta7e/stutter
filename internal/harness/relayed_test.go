package harness_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/relay"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// relayPorts hands out the ports this package's in-process relays listen on, each once, from
// [65000, 65100): above the kernel's source-port range and every other package's test range.
var relayPorts atomic.Uint32

var localhost = netip.MustParseAddr("127.0.0.1")

func relayPort(t *testing.T) uint16 {
	t.Helper()

	var config net.ListenConfig

	for offset := relayPorts.Add(1); offset < 100; offset = relayPorts.Add(1) {
		port := uint16(65000 + offset)

		listener, err := config.Listen(t.Context(), "tcp4", netip.AddrPortFrom(localhost, port).String())
		if err == nil {
			_ = listener.Close()

			return port
		}
	}

	t.Fatal("no free port in [65000, 65100)")

	return 0
}

// startInProcessRelay runs relay.Run with spec until the test ends, once it has printed its ready line.
func startInProcessRelay(t *testing.T, spec relay.Spec) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	stdoutReader, stdoutWriter := io.Pipe()
	done := make(chan int, 1)

	var stderr strings.Builder

	go func() {
		code := relay.Run(ctx, spec.Args(), stdoutWriter, &stderr)
		_ = stdoutWriter.Close()

		done <- code
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	ready := make(chan string, 1)

	go func() {
		line, _ := bufio.NewReader(stdoutReader).ReadString('\n') //nolint:errcheck // an empty line fails below.
		ready <- line

		_, _ = io.Copy(io.Discard, stdoutReader) //nolint:errcheck // nothing more is printed.
	}()

	select {
	case line := <-ready:
		if !strings.HasPrefix(line, relay.Ready) {
			t.Fatalf("relay printed %q, want its ready line", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the relay was not ready within 5s")
	}
}

// relayedRig is a check's host side and relay side in one process: a corpus, a listener set, and one
// relay pipe each for an unparsed dependency and the bus, in front of the set.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type relayedRig struct {
	cacheAt  netip.AddrPort
	busAt    netip.AddrPort
	store    *corpus.Corpus
	set      *harness.ListenerSet
	recorded []uint64
	token    relay.Token
}

func startRig(t *testing.T) relayedRig {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	orders := []string{"ORD-RELAY-1", "ORD-RELAY-2"}
	recorded := make([]uint64, 0, len(orders))

	for _, order := range orders {
		seq, publishErr := store.Publish(t.Context(), toy.SubjectOrderCreated, orderPayload(order, "WIDGET-RELAY"))
		if publishErr != nil {
			t.Fatalf("Publish() error = %v", publishErr)
		}

		recorded = append(recorded, seq)
	}

	token, err := relay.NewToken()
	if err != nil {
		t.Fatalf("NewToken() error = %v", err)
	}

	rig := relayedRig{
		store:    store,
		token:    token,
		cacheAt:  netip.AddrPortFrom(localhost, relayPort(t)),
		busAt:    netip.AddrPortFrom(localhost, relayPort(t)),
		recorded: recorded,
	}

	rig.set = rig.openSet(t, netip.MustParseAddrPort(startLineDependency(t)))
	rig.startRelays(t)

	return rig
}

func (r relayedRig) cacheKey() string {
	return harness.EndpointKey(opaqueCache, r.cacheAt.Port())
}

// openSet opens the listener set. Its upstream source reads the bus's address at every attach: a
// restore brings the bus back on a new port.
func (r relayedRig) openSet(t *testing.T, cache netip.AddrPort) *harness.ListenerSet {
	t.Helper()

	set, err := harness.OpenListeners(t.Context(), harness.ListenerConfig{
		Bind: localhost,
		Upstreams: func(context.Context) (map[string]netip.AddrPort, error) {
			return map[string]netip.AddrPort{
				r.cacheKey():          cache,
				harness.KeyBus:        netip.MustParseAddrPort(strings.TrimPrefix(r.store.URL(), "nats://")),
				harness.KeyBusMonitor: r.store.MonitorAddr(),
			}, nil
		},
		Opaque: []string{r.cacheKey()},
		Bus:    []uint16{r.busAt.Port()},
		Token:  r.token,
		Mode:   harness.ModeHostAlias,
	})
	if err != nil {
		t.Fatalf("OpenListeners() error = %v", err)
	}

	// Registered before the relays, so it runs after they are gone: a listener's close waits for its
	// connections.
	t.Cleanup(func() {
		if err := set.Close(context.Background()); err != nil {
			t.Errorf("close the listener set: %v", err)
		}
	})

	return set
}

func (r relayedRig) startRelays(t *testing.T) {
	t.Helper()

	hostPort := func(key string) netip.AddrPort {
		port, open := r.set.Port(key)
		if !open {
			t.Fatalf("the set has no %s listener", key)
		}

		return netip.AddrPortFrom(localhost, port)
	}

	startInProcessRelay(t, relay.Spec{
		Bind: netip.MustParsePrefix("127.0.0.1/32"),
		Listeners: []relay.Listener{
			{Upstream: hostPort(r.cacheKey()), Port: r.cacheAt.Port()},
			{Upstream: hostPort(harness.KeyBus), Port: r.busAt.Port()},
		},
		Token: r.token,
	})
}

// sandbox is the sandbox around a pulling service that reaches everything through the relays. Before
// each start, beforeStart is told which start it is, counting from one.
func (r relayedRig) sandbox(t *testing.T, beforeStart func(start int)) *harness.Sandbox {
	t.Helper()

	key := make([]byte, hashKeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate hash key: %v", err)
	}

	config := observedConfig()

	var starts atomic.Int32

	built, err := harness.New(harness.Config{
		Corpus:    r.store,
		Listeners: r.set,
		HashKey:   key,
		Policy:    config,
		Quiesce:   toy.DefaultQuiesce,
		Start: func(ctx context.Context, _ harness.Addresses) (harness.Consumer, error) {
			if beforeStart != nil {
				beforeStart(int(starts.Add(1)))
			}

			// The service is told nothing by Stutter: it dials the relays, as a container dials names. It
			// notes every order on the bus as well as the unparsed dependency, so both proxies record.
			return startPulling(ctx, harness.Addresses{
				Opaque: map[string]string{opaqueCache: r.cacheAt.String()},
				NATS:   "nats://" + r.busAt.String(),
			}, config, quirks{filter: toy.SubjectOrderCreated, audit: true})
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built
}

// sequence renders a run's compared effects, in order.
func sequence(effects []effect.Effect) []string {
	rendered := make([]string, 0, len(effects))
	for _, observed := range effect.Compared(effects) {
		rendered = append(rendered, fmt.Sprintf("%d %s %s", observed.MessageSeq, observed.Kind, observed.Canonical))
	}

	return rendered
}

func kinds(effects []effect.Effect) map[effect.Kind]int {
	counted := make(map[effect.Kind]int)
	for _, observed := range effect.Compared(effects) {
		counted[observed.Kind]++
	}

	return counted
}

func cleanRun(t *testing.T, built *harness.Sandbox, name string) replay.Result {
	t.Helper()

	result, err := built.Run(t.Context(), name, replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run(%s) error = %v", name, err)
	}

	return result
}

// TestAnObservedRunThroughRelays is a check's whole host side, driven the way a provisioned container
// drives it: the service dials relays, the relays pipe to the invocation listeners, and each run's
// proxies serve what the set hands them. Two clean runs must agree.
func TestAnObservedRunThroughRelays(t *testing.T) {
	t.Parallel()

	rig := startRig(t)
	built := rig.sandbox(t, nil)

	first := cleanRun(t, built, "clean-1")
	second := cleanRun(t, built, "clean-2")

	if a, b := strings.Join(sequence(first.Effects), "\n"), strings.Join(sequence(second.Effects), "\n"); a != b {
		t.Errorf("two clean runs through the relays differ:\n%s\n---\n%s", a, b)
	}

	counted := kinds(first.Effects)
	if counted[effect.KindNATS] == 0 || counted[effect.KindOpaque] == 0 {
		t.Errorf("effects by kind = %v, want at least one bus and one unparsed effect", counted)
	}

	if first.Setup != second.Setup {
		t.Errorf("Setup = %d then %d, want equal", first.Setup, second.Setup)
	}

	t.Logf("effects per run: %d; delivered: %d", len(effect.Compared(first.Effects)), first.Delivered)
}

// TestAForeignPublishIsNeverRecorded is the set refusing a container that is not one of the check's
// relays: its publish to the bus listener reaches no proxy, and the run it lands in is the run it
// would have been.
func TestAForeignPublishIsNeverRecorded(t *testing.T) {
	t.Parallel()

	rig := startRig(t)
	built := rig.sandbox(t, func(start int) {
		if start == 2 {
			rig.publishAsStranger(t)
		}
	})

	undisturbed := cleanRun(t, built, "undisturbed")
	disturbed := cleanRun(t, built, "disturbed")

	if a, b := strings.Join(
		sequence(undisturbed.Effects),
		"\n",
	), strings.Join(
		sequence(disturbed.Effects),
		"\n",
	); a != b {
		t.Errorf("sequence differs from the undisturbed run:\n%s\n---\n%s", a, b)
	}

	if undisturbed.Setup != disturbed.Setup {
		t.Errorf("Setup = %d then %d, want equal", undisturbed.Setup, disturbed.Setup)
	}

	foreign := rig.set.Counts().Foreign
	t.Logf("foreign connections: %d", foreign)

	if foreign < 1 {
		t.Error("the stranger's connection was not counted Foreign")
	}
}

// publishAsStranger dials the bus listener directly, as another container on the engine could, with
// no token, and publishes.
func (r relayedRig) publishAsStranger(t *testing.T) {
	t.Helper()

	port, _ := r.set.Port(harness.KeyBus)

	var dialer net.Dialer

	conn, err := dialer.DialContext(t.Context(), "tcp4", netip.AddrPortFrom(localhost, port).String())
	if err != nil {
		t.Errorf("dial the bus listener: %v", err)

		return
	}

	// It writes, gives the set a moment to judge it, and leaves, as a stranger container would.
	defer func() { _ = conn.Close() }()

	if err := relay.WritePreamble(conn, relay.Token{}, r.busAt.Port()); err != nil {
		t.Errorf("write a preamble: %v", err)
	}

	_, _ = io.WriteString(conn, "PUB corpus.x 2\r\nhi\r\n") //nolint:errcheck // the set refuses it either way.

	deadline := time.Now().Add(time.Second)
	for r.set.Counts().Foreign == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// calling is a service that consumes for itself and, for every message, calls an HTTP dependency at
// one path. Its Close reports closeErr, as a service whose relay died during the run would.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type calling struct {
	closeErr   error
	connection *nats.Conn
	consumer   jetstream.Consumer
	done       chan struct{}
	stopped    chan struct{}
	url        string
}

func startCalling(
	ctx context.Context,
	at harness.Addresses,
	config policy.Config,
	path string,
	closeErr error,
) (*calling, error) {
	connection, err := nats.Connect(at.NATS)
	if err != nil {
		return nil, fmt.Errorf("connect to the bus: %w", err)
	}

	stream, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("open jetstream: %w", err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, corpus.StreamName, jetstream.ConsumerConfig{
		Name:          observedConsumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       config.AckWait,
		MaxDeliver:    config.MaxDeliver,
		MaxAckPending: config.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("create the consumer: %w", err)
	}

	service := &calling{
		closeErr: closeErr, connection: connection, consumer: consumer,
		done: make(chan struct{}), stopped: make(chan struct{}), url: at.HTTP + path,
	}

	go service.pump(ctx)

	return service, nil
}

func (c *calling) Close(context.Context) (replay.Exit, error) {
	close(c.done)
	<-c.stopped
	c.connection.Close()

	return replay.Exit{}, c.closeErr
}

func (*calling) Exited() <-chan struct{} { return nil }

func (c *calling) pump(ctx context.Context) {
	defer close(c.stopped)

	for {
		select {
		case <-c.done:
			return
		default:
		}

		batch, err := c.consumer.Fetch(1, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			continue
		}

		for msg := range batch.Messages() {
			c.call(ctx)

			_ = msg.Ack() //nolint:errcheck // a lost ack is a redelivery, which the run observes anyway.
		}
	}
}

func (c *calling) call(ctx context.Context) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return
	}

	_, _ = io.Copy(io.Discard, response.Body) //nolint:errcheck // the call is the effect; the reply is not read.
	_ = response.Body.Close()
}

// TestACloseErrorAbortsTheScript holds the order that keeps a failed run's HTTP replies from becoming
// the script every later run replays: a service's Close error — a relay or a dependency that died
// during the run — fails the run before its HTTP replies are settled, so they are discarded.
func TestACloseErrorAbortsTheScript(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	if _, publishErr := store.Publish(
		t.Context(),
		toy.SubjectOrderCreated,
		orderPayload("ORD-SCRIPT-1", "WIDGET"),
	); publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	config := observedConfig()

	var starts atomic.Int32

	built, err := harness.New(harness.Config{
		Corpus: store, HashKey: key, Policy: config, Quiesce: toy.DefaultQuiesce,
		Start: func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			if starts.Add(1) == 1 {
				return startCalling(ctx, at, config, "/first", errRelayGone)
			}

			return startCalling(ctx, at, config, "/second", nil)
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	if _, err := built.Run(t.Context(), "dies", replay.Clean{}, nil); err == nil {
		t.Fatal("a run whose Close reported an error succeeded")
	}

	after := cleanRun(t, built, "after")

	calls := 0

	for _, observed := range after.Effects {
		if observed.Kind != effect.KindHTTP {
			continue
		}

		calls++

		if observed.OffScript {
			t.Errorf("script committed after a liveness failure: %s was off-script", observed.Printable)
		}
	}

	if calls == 0 {
		t.Fatal("the run after made no HTTP call, so it proves nothing about the script")
	}
}
