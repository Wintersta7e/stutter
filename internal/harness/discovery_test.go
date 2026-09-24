package harness_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/replay"
)

const (
	// startQuiesce is the quiesce every start in these tests uses, so the settle period is 100 ms.
	startQuiesce = 20 * time.Millisecond
	// startDeadline bounds each test, so a broken end rule fails at its own deadline instead of hanging.
	startDeadline = 20 * time.Second
	// ordersFilter is what the ORDERS stream captures.
	ordersFilter = "orders.>"
)

// errStartRefused is a Start that fails; errResetRefused a Reset that does.
var (
	errStartRefused = errors.New("the service would not start")
	errResetRefused = errors.New("the dependency would not restore")
)

// newBus opens a bus bound to ORDERS in a store of its own, runs each prepare step on a direct
// connection, and checkpoints it beside the store. With no step it is B0 before any job: no stream.
func newBus(t *testing.T, prepare ...func(jetstream.JetStream)) (*corpus.Corpus, *corpus.Checkpoint) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "bus")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	store, err := corpus.Open(t.Context(), filepath.Join(dir, "store"), "ORDERS")
	if err != nil {
		t.Fatalf("corpus.Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	direct := directStream(t, store)

	for _, step := range prepare {
		step(direct)
	}

	direct.Conn().Close()

	checkpoint, err := store.Checkpoint(t.Context(), filepath.Join(dir, "B"))
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	return store, &checkpoint
}

// withOrders is a job creating the ORDERS stream on orders.>, as a migration would.
func withOrders(t *testing.T) func(jetstream.JetStream) {
	t.Helper()

	return func(direct jetstream.JetStream) {
		if _, err := direct.CreateStream(t.Context(), jetstream.StreamConfig{
			Name:     "ORDERS",
			Subjects: []string{ordersFilter},
		}); err != nil {
			t.Fatalf("CreateStream() error = %v", err)
		}
	}
}

// directStream is Stutter's side of the bus: a connection straight to it, never through a proxy.
//
//nolint:ireturn // the client library models JetStream as an interface.
func directStream(t *testing.T, store *corpus.Corpus) jetstream.JetStream {
	t.Helper()

	conn, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	t.Cleanup(conn.Close)

	direct, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	return direct
}

// script is what a fake service does once started, on its own connection through the proxied bus.
// Returning is the service exiting by itself with that code; it must return once ctx is done.
type script func(ctx context.Context, js jetstream.JetStream, conn *nats.Conn, at harness.Addresses) int

// journal is an ordered log of what happened, written from several goroutines.
type journal struct {
	entries []string
	mu      sync.Mutex
}

func (j *journal) note(entry string) {
	if j == nil {
		return
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	j.entries = append(j.entries, entry)
}

func (j *journal) read() []string {
	j.mu.Lock()
	defer j.mu.Unlock()

	return slices.Clone(j.entries)
}

// fakeService is a service that consumes for itself: each start runs its script over a connection of
// its own. It counts its starts.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type fakeService struct {
	script script
	// dial connects the service to the bus; nil dials the proxied bus address it is handed.
	dial func(at harness.Addresses) (*nats.Conn, error)
	// closeErr is what every Close reports beside the exit.
	closeErr error
	journal  *journal
	starts   atomic.Int32
}

// starter is the fake as a Config.Start.
func (f *fakeService) starter() harness.Start {
	return func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
		started, err := f.begin(ctx, at)
		if err != nil {
			return nil, err
		}

		return started, nil
	}
}

// begin starts the script over a connection of the fake's own.
func (f *fakeService) begin(ctx context.Context, at harness.Addresses) (*fakeRun, error) {
	f.starts.Add(1)
	f.journal.note("start")

	dial := f.dial
	if dial == nil {
		dial = func(at harness.Addresses) (*nats.Conn, error) {
			return nats.Connect(at.NATS, nats.NoReconnect())
		}
	}

	conn, err := dial(at)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, err
	}

	running, cancel := context.WithCancel(context.WithoutCancel(ctx))
	started := &fakeRun{
		cancel:   cancel,
		exited:   make(chan struct{}),
		done:     make(chan struct{}),
		closeErr: f.closeErr,
	}

	go func() {
		code := f.script(running, js, conn, at)
		conn.Close()

		started.code, started.byItself = code, running.Err() == nil
		if started.byItself {
			close(started.exited)
		}

		close(started.done)
	}()

	return started, nil
}

// fakeRun is one start of a fake service.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type fakeRun struct {
	closeErr error
	cancel   context.CancelFunc
	exited   chan struct{}
	done     chan struct{}
	code     int
	byItself bool
}

func (r *fakeRun) Exited() <-chan struct{} {
	return r.exited
}

// Close stops the script unless it already ended by itself, and reports how the start ended.
func (r *fakeRun) Close(context.Context) (replay.Exit, error) {
	r.cancel()
	<-r.done

	if !r.byItself {
		return replay.Exit{}, r.closeErr
	}

	return replay.Exit{Exited: true, Code: r.code}, r.closeErr
}

// idle is how a fake service waits to be stopped: it returns 0 once asked to.
func idle(ctx context.Context) int {
	<-ctx.Done()

	return 0
}

// createDurable is a fake service creating a durable pull consumer on stream, as its startup does.
func createDurable(ctx context.Context, js jetstream.JetStream, stream, name string) error {
	_, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:   name,
		AckPolicy: jetstream.AckExplicitPolicy,
	})

	return err
}

// idleAfter creates each named durable on ORDERS, then idles; a create that fails exits with code 2.
func idleAfter(names ...string) script {
	return func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		for _, name := range names {
			if err := createDurable(ctx, js, "ORDERS", name); err != nil {
				return 2
			}
		}

		return idle(ctx)
	}
}

// startConfig is a start that publishes nothing, on store from checkpoint, with the test timings.
func startConfig(store *corpus.Corpus, checkpoint *corpus.Checkpoint, service *fakeService) harness.Config {
	return harness.Config{
		Corpus:   store,
		Baseline: checkpoint,
		Start:    service.starter(),
		Quiesce:  startQuiesce,
		Startup:  5 * time.Second,
	}
}

// startContext bounds one test's start.
func startContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), startDeadline)
	t.Cleanup(cancel)

	return ctx
}

// names lists what discovery found, in its order.
func names(found []harness.Found) []string {
	listed := make([]string, 0, len(found))
	for _, consumer := range found {
		listed = append(listed, consumer.Name)
	}

	return listed
}

// TestDiscoveryStartsFromItsBaseline: discovery resets the dependencies and restores the bus to its
// checkpoint before the service starts, so the service reads what the checkpoint holds and never what
// a previous start left behind.
func TestDiscoveryStartsFromItsBaseline(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t), func(direct jetstream.JetStream) {
		claims, err := direct.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: "claims"})
		if err != nil {
			t.Fatalf("CreateKeyValue() error = %v", err)
		}

		if _, err := claims.Put(t.Context(), "k", []byte("first")); err != nil {
			t.Fatalf("Put() error = %v", err)
		}
	})

	// The previous run's write, after the checkpoint.
	claims, err := directStream(t, store).KeyValue(t.Context(), "claims")
	if err != nil {
		t.Fatalf("KeyValue() error = %v", err)
	}

	if _, putErr := claims.Put(t.Context(), "k", []byte("second")); putErr != nil {
		t.Fatalf("Put() error = %v", putErr)
	}

	log := &journal{}

	var revision atomic.Uint64

	service := &fakeService{journal: log}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		bucket, bucketErr := js.KeyValue(ctx, "claims")
		if bucketErr != nil {
			return 2
		}

		entry, getErr := bucket.Get(ctx, "k")
		if getErr != nil {
			return 2
		}

		revision.Store(entry.Revision())

		return idleAfter("reserve")(ctx, js, nil, harness.Addresses{})
	}

	cfg := startConfig(store, checkpoint, service)
	cfg.Reset = func(context.Context) error {
		log.note("reset")

		return nil
	}

	found, err := harness.Discover(startContext(t), cfg)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	if got := revision.Load(); got != 1 {
		t.Errorf("the service read revision %d of k, want 1 (the checkpoint's)", got)
	}

	if got := log.read(); len(got) < 2 || got[0] != "reset" || got[1] != "start" {
		t.Errorf("journal = %q, want it to begin [reset start]", got)
	}

	if got := names(found.Consumers); !slices.Equal(got, []string{"reserve"}) {
		t.Errorf("consumers = %q, want [reserve]", got)
	}
}

// TestDiscoverySettlesOnceTheServiceIsQuiet: a consumer existing is not enough; discovery waits until
// the service has stopped doing things, so a start still busy is never read half way.
func TestDiscoverySettlesOnceTheServiceIsQuiet(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))

	const busyFor = 400 * time.Millisecond

	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, conn *nats.Conn, _ harness.Addresses) int {
		if err := createDurable(ctx, js, "ORDERS", "reserve"); err != nil {
			return 2
		}

		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()

		for busy := time.Now(); time.Since(busy) < busyFor; {
			select {
			case <-ctx.Done():
				return 0
			case <-ticker.C:
				if err := conn.Publish("busy.tick", []byte("tick")); err != nil {
					return 2
				}
			}
		}

		return idle(ctx)
	}

	began := time.Now()

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	elapsed := time.Since(began)
	t.Logf("discovery settled after %s", elapsed)

	if elapsed < busyFor || elapsed >= 5*time.Second {
		t.Errorf("discovery took %s, want at least %s and under the 5s startup limit", elapsed, busyFor)
	}

	if got := names(found.Consumers); !slices.Equal(got, []string{"reserve"}) {
		t.Errorf("consumers = %q, want [reserve]", got)
	}
}

// TestDiscoveryWithNoConsumerGivesUpAtTheStartupLimit: a service that creates no consumer anywhere is
// given up on at the startup limit, and that is data rather than an error — the check still runs, and
// its observation gate says what never happened.
func TestDiscoveryWithNoConsumerGivesUpAtTheStartupLimit(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{script: func(ctx context.Context, _ jetstream.JetStream, _ *nats.Conn,
		_ harness.Addresses,
	) int {
		return idle(ctx)
	}}

	const limit = 600 * time.Millisecond

	cfg := startConfig(store, checkpoint, service)
	cfg.Startup = limit

	began := time.Now()

	found, err := harness.Discover(startContext(t), cfg)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	elapsed := time.Since(began)
	t.Logf("discovery gave up after %s", elapsed)

	if elapsed < limit {
		t.Errorf("discovery gave up after %s, before the %s startup limit", elapsed, limit)
	}

	if len(found.Consumers) != 0 || len(found.Elsewhere) != 0 {
		t.Errorf("consumers = %q, elsewhere = %q, want none", names(found.Consumers), found.Elsewhere)
	}
}

// TestAStartWhoseServiceFailsToStartIsASetupError: a service that cannot be started stops the check,
// naming the start it failed in.
func TestAStartWhoseServiceFailsToStartIsASetupError(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	cfg := startConfig(store, checkpoint, &fakeService{})
	cfg.Start = func(context.Context, harness.Addresses) (harness.Consumer, error) {
		return nil, errStartRefused
	}

	_, err := harness.Discover(startContext(t), cfg)
	if !errors.Is(err, errStartRefused) {
		t.Fatalf("Discover() error = %v, want it to wrap %v", err, errStartRefused)
	}

	if !strings.Contains(err.Error(), "discovery") {
		t.Errorf("error %q does not name discovery", err)
	}
}

// TestAFailedResetStopsDiscoveryBeforeTheServiceStarts: a dependency that cannot be restored stops the
// check before the service ever starts against it.
func TestAFailedResetStopsDiscoveryBeforeTheServiceStarts(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{script: idleAfter("reserve")}
	cfg := startConfig(store, checkpoint, service)
	cfg.Reset = func(context.Context) error { return errResetRefused }

	_, err := harness.Discover(startContext(t), cfg)
	if !errors.Is(err, errResetRefused) {
		t.Fatalf("Discover() error = %v, want it to wrap %v", err, errResetRefused)
	}

	if got := service.starts.Load(); got != 0 {
		t.Errorf("the service was started %d times, want 0", got)
	}
}
