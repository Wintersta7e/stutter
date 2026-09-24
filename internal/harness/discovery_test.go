package harness_test

import (
	"bufio"
	"context"
	"errors"
	"net"
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
	"github.com/Wintersta7e/stutter/internal/effect"
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
	// auditStream is a stream the service consumes from beside the corpus stream.
	auditStream = "AUDIT"
	// reserve is the durable consumer most of these services create.
	reserve = "reserve"
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

// silent is a service that connects and does nothing until it is stopped.
func silent(ctx context.Context, _ jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
	return idle(ctx)
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

		return idleAfter(reserve)(ctx, js, nil, harness.Addresses{})
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

	if got := names(found.Consumers); !slices.Equal(got, []string{reserve}) {
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
		if err := createDurable(ctx, js, "ORDERS", reserve); err != nil {
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

	if got := names(found.Consumers); !slices.Equal(got, []string{reserve}) {
		t.Errorf("consumers = %q, want [reserve]", got)
	}
}

// TestDiscoveryWithNoConsumerGivesUpAtTheStartupLimit: a service that creates no consumer anywhere is
// given up on at the startup limit, and that is data rather than an error — the check still runs, and
// its observation gate says what never happened.
func TestDiscoveryWithNoConsumerGivesUpAtTheStartupLimit(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{script: silent}

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
	service := &fakeService{script: idleAfter(reserve)}
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

// consumerNamed finds one consumer discovery found, by name.
func consumerNamed(t *testing.T, found []harness.Found, name string) harness.Found {
	t.Helper()

	for _, consumer := range found {
		if consumer.Name == name {
			return consumer
		}
	}

	t.Fatalf("discovery found no consumer %q among %q", name, names(found))

	return harness.Found{}
}

// TestDiscoveryReadsTheConfigBeforeSerialise: each consumer's configuration is read back as the server
// holds it, defaults applied, before any run rewrites it to one message in flight — the read is the one
// legality input, and reading the rewrite would license faults against a contract the service never had.
func TestDiscoveryReadsTheConfigBeforeSerialise(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		for _, config := range []jetstream.ConsumerConfig{
			{Durable: "slow", AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: 10},
			{
				Durable:    "curved",
				AckPolicy:  jetstream.AckExplicitPolicy,
				AckWait:    2 * time.Minute,
				BackOff:    []time.Duration{time.Second, 2 * time.Second},
				MaxDeliver: 3,
			},
		} {
			if _, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", config); err != nil {
				return 2
			}
		}

		return idle(ctx)
	}

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	slow, curved := consumerNamed(t, found.Consumers, "slow"), consumerNamed(t, found.Consumers, "curved")
	t.Logf("slow: MaxAckPending %d, Deadline(1) %s; curved: Deadline(1) %s",
		slow.Policy.MaxAckPending, slow.Policy.Deadline(1), curved.Policy.Deadline(1))

	if slow.Policy.MaxAckPending != 10 || slow.Policy.Deadline(1) != 30*time.Second {
		t.Errorf("slow reads MaxAckPending %d, Deadline(1) %s; want 10 and the server's 30s default",
			slow.Policy.MaxAckPending, slow.Policy.Deadline(1))
	}

	if curved.Policy.Deadline(1) != time.Second {
		t.Errorf("curved reads Deadline(1) %s, want BackOff[0] = 1s", curved.Policy.Deadline(1))
	}
}

// TestDiscoveryListsEveryConsumerTheServerHolds: discovery finds exactly the consumers the server holds
// on the corpus stream, names the ones on other streams, and leaves out the client library's own
// machinery on a key/value bucket's stream.
func TestDiscoveryListsEveryConsumerTheServerHolds(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		audit := jetstream.StreamConfig{Name: auditStream, Subjects: []string{"audit.>"}}
		if _, err := js.CreateStream(ctx, audit); err != nil {
			return 2
		}

		claims, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "claims"})
		if err != nil {
			return 2
		}

		watcher, err := claims.WatchAll(ctx)
		if err != nil {
			return 2
		}

		defer func() { _ = watcher.Stop() }() //nolint:errcheck // the service is stopping either way.

		if err := createDurable(ctx, js, auditStream, "audit"); err != nil {
			return 2
		}

		return idleAfter("alpha", "beta", "gamma")(ctx, js, nil, harness.Addresses{})
	}

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	listed := listedOn(t, store, "ORDERS")
	discovered := names(found.Consumers)
	t.Logf("discovered: %d, listed: %d", len(discovered), len(listed))

	if len(discovered) == 0 || len(listed) == 0 {
		t.Fatalf("discovered: %d, listed: %d; want both above zero", len(discovered), len(listed))
	}

	if !slices.Equal(discovered, listed) || !slices.Equal(discovered, []string{"alpha", "beta", "gamma"}) {
		t.Errorf("discovered %q, listed %q; want both [alpha beta gamma]", discovered, listed)
	}

	if !slices.Equal(found.Elsewhere, []string{"AUDIT/audit"}) {
		t.Errorf("elsewhere = %q, want [AUDIT/audit] and no key/value bucket's consumer", found.Elsewhere)
	}
}

// listedOn lists a stream's consumers on a direct connection of the test's own.
func listedOn(t *testing.T, store *corpus.Corpus, stream string) []string {
	t.Helper()

	held, err := directStream(t, store).Stream(t.Context(), stream)
	if err != nil {
		t.Fatalf("Stream(%s) error = %v", stream, err)
	}

	lister := held.ConsumerNames(t.Context())

	var listed []string
	for name := range lister.Name() {
		listed = append(listed, name)
	}

	if err := lister.Err(); err != nil {
		t.Fatalf("ConsumerNames() error = %v", err)
	}

	slices.Sort(listed)

	return listed
}

// TestDiscoveryMarksConsumersThatCannotBeTargets: a consumer whose configuration has no legality row,
// or whose kind can neither be faulted nor held to one message in flight, is found and marked excluded,
// naming why; a checkable one beside them is not.
func TestDiscoveryMarksConsumersThatCannotBeTargets(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{script: targetKinds}

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	for _, consumer := range found.Consumers {
		t.Logf("%s: kind %s, excluded %q", consumer.Name, consumer.Kind, consumer.Excluded)
	}

	if len(found.Consumers) == 0 {
		t.Fatal("discovered: 0")
	}

	if ok := consumerNamed(t, found.Consumers, "ok"); ok.Kind != corpus.KindPull || ok.Excluded != "" {
		t.Errorf("ok: kind %s, excluded %q; want a pull consumer, not excluded", ok.Kind, ok.Excluded)
	}

	fc := consumerNamed(t, found.Consumers, "fc")
	if fc.Kind != corpus.KindPush || !strings.Contains(fc.Excluded, "AckFlowControl") {
		t.Errorf("fc: kind %s, excluded %q; want a push consumer excluded for AckFlowControl", fc.Kind, fc.Excluded)
	}

	if quiet := consumerNamed(t, found.Consumers, "quiet"); !strings.Contains(quiet.Excluded, "AckNone") {
		t.Errorf("quiet: excluded %q, want it to name AckNone", quiet.Excluded)
	}

	ordered := 0

	for _, consumer := range found.Consumers {
		if consumer.Kind == corpus.KindOrdered && strings.Contains(consumer.Excluded, "ordered") {
			ordered++
		}
	}

	if ordered != 1 {
		t.Errorf("%d ordered consumers excluded as ordered, want 1", ordered)
	}
}

// targetKinds is a service creating one consumer of every kind on ORDERS, and consuming from the
// ordered one until it is stopped.
func targetKinds(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
	if createDurable(ctx, js, "ORDERS", "ok") != nil {
		return 2
	}

	if _, err := js.CreateOrUpdatePushConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:        "fc",
		AckPolicy:      jetstream.AckFlowControlPolicy,
		DeliverSubject: "fc.inbox",
		MaxAckPending:  10,
	}); err != nil {
		return 2
	}

	if _, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:   "quiet",
		AckPolicy: jetstream.AckNonePolicy,
	}); err != nil {
		return 2
	}

	ordered, err := js.OrderedConsumer(ctx, "ORDERS", jetstream.OrderedConsumerConfig{})
	if err != nil {
		return 2
	}

	consuming, err := ordered.Consume(func(jetstream.Msg) {})
	if err != nil {
		return 2
	}

	defer consuming.Stop()

	return idle(ctx)
}

// errRemovalFailed is a service whose removal failed.
var errRemovalFailed = errors.New("the service could not be removed")

// hangUpAfterGreeting opens a raw connection to the proxied bus, reads the greeting and hangs up without
// a byte, as a client that requires TLS or a script waiting for the port does.
func hangUpAfterGreeting(ctx context.Context, at harness.Addresses) error {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", strings.TrimPrefix(at.NATS, "nats://"))
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
		return err
	}

	return nil
}

// TestATargetThatExitsBeforeAnyConsumerIsASetupError: a service that stops before creating any
// consumer leaves discovery nothing to read, and the check stops with everything that says why: how it
// exited, what the bus refused it, and how many connections hung up after the greeting.
func TestATargetThatExitsBeforeAnyConsumerIsASetupError(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, at harness.Addresses) int {
		clash := jetstream.StreamConfig{Name: "CLASH", Subjects: []string{"orders.created"}}
		if _, err := js.CreateStream(ctx, clash); err == nil {
			return 2
		}

		if err := hangUpAfterGreeting(ctx, at); err != nil {
			return 2
		}

		return 1
	}

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if !errors.Is(err, harness.ErrExitedBeforeConsumer) {
		t.Fatalf("Discover() error = %v, want %v", err, harness.ErrExitedBeforeConsumer)
	}

	t.Logf("E11: %v", err)

	if !found.Exit.Exited || found.Exit.Code != 1 {
		t.Errorf("exit = %+v, want exited by itself with code 1", found.Exit)
	}

	refused := slices.IndexFunc(found.Refusals, func(refusal effect.Refusal) bool {
		return refusal.ErrCode == 10065 && refusal.Description != ""
	})
	if refused < 0 {
		t.Errorf("refusals = %+v, want err_code 10065 with its description", found.Refusals)
	}

	if found.ClosedAfterInfo != 1 {
		t.Errorf("closed after the greeting: %d, want 1", found.ClosedAfterInfo)
	}

	if !strings.Contains(err.Error(), "10065") {
		t.Errorf("error %q does not name err_code 10065", err)
	}
}

// TestATargetThatExitsAfterItsConsumerIsRecordedNotRefused: a durable consumer outlives its client, so
// a service that exits after creating one is read, and its exit recorded; whether it keeps exiting is
// for the runs to find.
func TestATargetThatExitsAfterItsConsumerIsRecordedNotRefused(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		if createDurable(ctx, js, "ORDERS", reserve) != nil {
			return 2
		}

		return 0
	}

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	if got := names(found.Consumers); !slices.Equal(got, []string{reserve}) {
		t.Errorf("consumers = %q, want [reserve]", got)
	}

	if !found.Exit.Exited {
		t.Errorf("exit = %+v, want exited by itself", found.Exit)
	}
}

// TestConsumersOnlyOnAnotherStreamAreASetupError: a service whose consumers are all on another stream
// is not consuming from the stream the check was pointed at, and the check stops naming where they are.
func TestConsumersOnlyOnAnotherStreamAreASetupError(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{script: auditOnly}

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if !errors.Is(err, harness.ErrConsumesElsewhere) {
		t.Fatalf("Discover() error = %v, want %v", err, harness.ErrConsumesElsewhere)
	}

	if !strings.Contains(err.Error(), "AUDIT/audit") {
		t.Errorf("error %q does not name AUDIT/audit", err)
	}

	if !slices.Equal(found.Elsewhere, []string{"AUDIT/audit"}) {
		t.Errorf("elsewhere = %q, want [AUDIT/audit]", found.Elsewhere)
	}
}

// auditOnly is a service consuming from AUDIT alone.
func auditOnly(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
	audit := jetstream.StreamConfig{Name: auditStream, Subjects: []string{"audit.>"}}
	if _, err := js.CreateStream(ctx, audit); err != nil {
		return 2
	}

	if createDurable(ctx, js, auditStream, "audit") != nil {
		return 2
	}

	return idle(ctx)
}

// TestAnEnvironmentStopDuringDiscoveryIsASetupError: a dependency the proxy cannot reach stops
// discovery at once, naming it, rather than at the startup limit.
func TestAnEnvironmentStopDuringDiscoveryIsASetupError(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{script: dialsCache}
	cfg := startConfig(store, checkpoint, service)
	cfg.Opaque = map[string]string{"cache": closedPort(t).String()}

	began := time.Now()

	_, err := harness.Discover(startContext(t), cfg)

	elapsed := time.Since(began)
	t.Logf("discovery stopped after %s: %v", elapsed, err)

	if err == nil || !strings.Contains(err.Error(), "cache") {
		t.Fatalf("Discover() error = %v, want one naming cache", err)
	}

	if elapsed >= 2500*time.Millisecond {
		t.Errorf("discovery stopped after %s, want it well inside the 5s startup limit", elapsed)
	}
}

// dialsCache is a service that dials its cache as it starts and writes one line to it.
func dialsCache(ctx context.Context, _ jetstream.JetStream, _ *nats.Conn, at harness.Addresses) int {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", at.Opaque["cache"])
	if err == nil {
		_, _ = conn.Write([]byte("GET warm\n")) //nolint:errcheck // the dependency is unreachable either way.
		_ = conn.Close()
	}

	return idle(ctx)
}

// TestAFailedServiceRemovalIsASetupError: a service that cannot be removed cleanly stops the check.
func TestAFailedServiceRemovalIsASetupError(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{script: idleAfter(reserve), closeErr: errRemovalFailed}

	if _, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service)); !errors.Is(
		err, errRemovalFailed) {
		t.Fatalf("Discover() error = %v, want it to wrap %v", err, errRemovalFailed)
	}
}

// createUnnamed is a service creating a consumer on stream with neither a name nor a durable name, so
// every start gives it a new one.
func createUnnamed(ctx context.Context, js jetstream.JetStream, stream string) error {
	_, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})

	return err
}

// unnamedBeside creates one unnamed consumer on ORDERS and each named durable, then idles.
func unnamedBeside(durables ...string) script {
	return func(ctx context.Context, js jetstream.JetStream, conn *nats.Conn, at harness.Addresses) int {
		if createUnnamed(ctx, js, "ORDERS") != nil {
			return 2
		}

		return idleAfter(durables...)(ctx, js, conn, at)
	}
}

// TestAServerNamedConsumerIsMarkedUnstable: a consumer whose name a start makes up cannot be found by
// name on the next start, so no run can be scoped to it by name; it is marked, sole or not.
func TestAServerNamedConsumerIsMarkedUnstable(t *testing.T) {
	t.Parallel()

	cases := map[string][]string{"sole": nil, "beside-a-durable": {reserve}}

	for name, durables := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, checkpoint := newBus(t, withOrders(t))
			service := &fakeService{script: unnamedBeside(durables...)}

			found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
			if err != nil {
				t.Fatalf("Discover() error = %v", err)
			}

			t.Logf("starts: %d", service.starts.Load())

			if len(found.Consumers) != 1+len(durables) {
				t.Fatalf("consumers = %q, want one unnamed beside %q", names(found.Consumers), durables)
			}

			for _, consumer := range found.Consumers {
				if want := !slices.Contains(durables, consumer.Name); consumer.Unstable != want {
					t.Errorf("%s: unstable %t, want %t", consumer.Name, consumer.Unstable, want)
				}
			}

			if got := service.starts.Load(); got != 2 {
				t.Errorf("starts: %d, want 2", got)
			}
		})
	}
}

// TestANameOnlyConsumerIsStable: a consumer the service names without making it durable reads back
// with no durable name yet keeps its name from one start to the next; the second look proves it.
func TestANameOnlyConsumerIsStable(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t, withOrders(t))
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, conn *nats.Conn, at harness.Addresses) int {
		if _, err := js.CreateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
			Name:      "named",
			AckPolicy: jetstream.AckExplicitPolicy,
		}); err != nil {
			return 2
		}

		return idleAfter(reserve)(ctx, js, conn, at)
	}

	found, err := harness.Discover(startContext(t), startConfig(store, checkpoint, service))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}

	t.Logf("starts: %d", service.starts.Load())

	for _, consumer := range found.Consumers {
		if consumer.Unstable {
			t.Errorf("%s: unstable, want stable", consumer.Name)
		}
	}

	if got := names(found.Consumers); !slices.Equal(got, []string{"named", reserve}) {
		t.Errorf("consumers = %q, want [named reserve]", got)
	}

	if got := service.starts.Load(); got != 2 {
		t.Errorf("starts: %d, want 2 (named has no durable name, so it needed the second look)", got)
	}
}

// TestAStartIsAddedOnlyForADurableLessName: a service whose every consumer is durable under its own
// name costs no second start, and neither does one with no consumer on the corpus stream.
func TestAStartIsAddedOnlyForADurableLessName(t *testing.T) {
	t.Parallel()

	cases := map[string]script{
		"all-durable":    idleAfter("alpha", "beta"),
		"no-consumer":    silent,
		"elsewhere-only": auditOnly,
	}

	for name, current := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, checkpoint := newBus(t, withOrders(t))
			service := &fakeService{script: current}
			cfg := startConfig(store, checkpoint, service)
			cfg.Startup = 600 * time.Millisecond

			found, err := harness.Discover(startContext(t), cfg)
			if err != nil && !errors.Is(err, harness.ErrConsumesElsewhere) {
				t.Fatalf("Discover() error = %v", err)
			}

			t.Logf("starts: %d", service.starts.Load())

			if got := service.starts.Load(); got != 1 {
				t.Errorf("starts: %d, want 1", got)
			}

			for _, consumer := range found.Consumers {
				if consumer.Unstable {
					t.Errorf("%s: unstable, want stable", consumer.Name)
				}
			}
		})
	}
}
