package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/policy"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
)

var (
	// errAmbiguousConsumer means the service runs several consumers on the corpus stream and nothing
	// said which one is under test.
	errAmbiguousConsumer = errors.New("the service under test runs more than one consumer on the corpus stream; " +
		"set Config.Consumer to the one under test")
	// errNoSuchConsumer means the consumer named as under test is not one the service created.
	errNoSuchConsumer = errors.New("the consumer under test is not one the service created on the corpus stream")
	// errUnscoped means a consumer that was not under test took deliveries anyway, so its effects are
	// mixed into the run's with no way to separate them.
	errUnscoped = errors.New("a consumer other than the one under test took deliveries during the run")
	// errLateConsumer means the service had created no consumer when the corpus was published, and one
	// it created afterwards took deliveries. Nothing held it to one message in flight, so its effects
	// cannot be attributed to messages.
	errLateConsumer = errors.New("a consumer created after the corpus was published took deliveries")
	// errFedBack means the service was handed a message Stutter did not publish — its own output, fed
	// back through the stream it consumes — and did work while handling it. That work belongs to no
	// corpus message, so there is nothing honest to compare it against.
	errFedBack = errors.New("a message Stutter did not publish reached the consumer under test, " +
		"and handling it did work")
	// errRestarted means the engine restarted the service under test during the run.
	errRestarted = errors.New("the service under test restarted")
	// errDrifted means the consumer under test is not the configuration legality was read from, or
	// stopped being the one Serialise left behind: faults would be licensed by a contract it does not
	// have, or its effects could no longer be attributed.
	errDrifted = errors.New("the consumer under test differs from the configuration discovery read")
)

// fedBack is the identity a delivery takes when Stutter did not publish its message. No corpus
// message has it, because stream sequences start at one, so its window is never mistaken for one's and
// no fault is ever aimed at it: its acknowledgement always goes through.
const fedBack uint64 = 0

// DefaultStartup is how long a service may take to create a consumer, from the moment its Start
// returns: the one statement of the startup limit, which Config.Startup overrides. Generous, because it
// is spent in full only on a failure path — a service that never creates its consumer, whose run has
// already failed.
const DefaultStartup = 60 * time.Second

const (
	// settleMargin multiplies the quiesce to get the settle period: how long a starting service must
	// stay quiet before the corpus is published, and how long a run whose messages are all done must
	// stay quiet before it ends. Startup work is the same scale of thing as a handler's trailing writes,
	// and a few of them in a row is what separates a finished startup from a pause inside one.
	settleMargin = 5
	// drainMargin multiplies the longest redelivery deadline to get the owed-silence limit's floor. A
	// redelivery lands at the deadline, not before it, so the margin is what stops a run that is
	// merely waiting from being called finished.
	drainMargin = 2
	// drainPoll is how often a wait checks its condition. Well below any redelivery deadline a consumer
	// would be configured with, so it costs the run no accuracy.
	drainPoll = 25 * time.Millisecond
	// ownCheckpoint names the sandbox's own bus checkpoint, beside the store.
	ownCheckpoint = "observed"
)

// Consumer is a service that pulls from the bus for itself.
//
// It has no Handle: Stutter never delivers to it. A provisioned container creates its own JetStream
// consumer and acknowledges on its own connection, which is why the run is watched and faulted on
// the wire rather than driven.
type Consumer interface {
	// Close stops the service and releases its connections, and reports how it ended. It runs BEFORE
	// the proxies are closed. It inspects the service BEFORE stopping it: after a kill the exit code is
	// always the kill's own. Its error carries anything that died beside the service during the run.
	Close(ctx context.Context) (replay.Exit, error)
	// Exited closes when the service stops by itself, and never for one Stutter stopped. A service that
	// cannot stop by itself returns nil, which never fires.
	Exited() <-chan struct{}
}

// Start builds a service that consumes for itself, against the proxied addresses.
type Start func(ctx context.Context, at Addresses) (Consumer, error)

// runObserved replays the corpus into a service Stutter does not dispatch to.
//
// The shape is the driven run's, with three substitutions: the bus is restored to its starting point
// and only the run's messages are published, because the service cannot be told to skip a message;
// the fault is injected by swallowing acknowledgements on the wire; and the run ends by settlement,
// not silence — every admitted message done, or nothing moving for the owed-silence limit — rather than
// when the driver stops delivering.
func (s *Sandbox) runObserved(
	ctx context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	verdict, err := replay.Admit(s.cfg.Policy, mutation)
	if err != nil {
		return replay.Result{}, fmt.Errorf("admit %s: %w", mutation.Fault(), err)
	}

	wire, err := replay.NewWirePolicy(mutation)
	if err != nil {
		return replay.Result{}, fmt.Errorf("express %s on the wire: %w", mutation.Fault(), err)
	}

	messages, err := s.prepare(ctx, retain)
	if err != nil {
		return replay.Result{}, err
	}

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), s.cfg.HashKey)
	run := newObservedRun(name, s.cfg.Corpus.Topic().Stream, wire, recorder, s.quiesce())

	observed, err := s.observe(ctx, recorder, run.options())
	if err != nil {
		return replay.Result{}, err
	}

	service, err := s.cfg.Start(ctx, observed.at)
	if err != nil {
		return replay.Result{}, errors.Join(
			fmt.Errorf("start the service under test: %w", err),
			observed.close(ctx),
			observed.httpRun.Abort(),
		)
	}

	exit, runErr := s.watch(ctx, observed, service, run, recorder, messages)

	if settleErr := observed.settle(ctx, runErr); settleErr != nil {
		return replay.Result{}, settleErr
	}

	result, err := run.result(verdict.Clause)
	if err != nil {
		return replay.Result{}, err
	}

	result.Exit = exit
	result.Stopped = observed.halted

	return result, nil
}

// watch takes one run from the service's start to its end: the corpus published once the service has
// started, the run's end awaited, and the service stopped. It reports how the service ended.
func (s *Sandbox) watch(
	ctx context.Context,
	observed *egress,
	service Consumer,
	run *observedRun,
	recorder *effect.Recorder,
	messages []corpus.Message,
) (replay.Exit, error) {
	ended, runErr := s.begin(ctx, observed, service.Exited(), run, recorder, messages)
	if runErr == nil && ended == finished {
		ended, runErr = s.drain(ctx, observed, service.Exited(), run)
	}

	// Recorded before the window closes, so the stop lands with the message whose handling raised it.
	if ended == proxyStopped {
		runErr = observed.halt(runErr, recorder)
	}

	if runErr == nil {
		runErr = s.stillSerialised(ctx, run, ended)
	}

	run.finish()

	// Ordered deliberately: the forwarding proxies wait for in-flight connections, so a service
	// holding an idle connection open would make teardown hang rather than fail. The teardown point is
	// marked first: a connection the service closes as it goes away hid nothing.
	observed.markTeardown()

	exit, closeErr := service.Close(ctx)
	if closeErr != nil {
		closeErr = fmt.Errorf("close the service under test: %w", closeErr)
	}

	return exit, errors.Join(runErr, run.ended(&exit, ended == targetExited), closeErr)
}

// prepare returns the part of the corpus this run replays, and restores the whole bus to its starting
// point to receive it.
//
// The restore is the bus-side reset: whatever the previous run's service created — buckets, streams,
// consumers, pauses — is gone, and a key a job seeded reads at the job's revision again. Nothing is
// published yet: the service starts against the starting point, and the corpus arrives only once it
// has finished starting.
//
// The corpus is taken once, before the first run: from Recorded, or else read out of the stream. Sandbox
// runs are serial — a check compares one run against the next — so neither needs a guard of its own.
func (s *Sandbox) prepare(ctx context.Context, retain []uint64) ([]corpus.Message, error) {
	if s.recorded == nil {
		if err := s.load(ctx); err != nil {
			return nil, err
		}
	}

	baseline, err := s.startingPoint(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.cfg.Corpus.Restore(ctx, baseline); err != nil {
		return nil, fmt.Errorf("restore the bus: %w", err)
	}

	return scope(s.recorded, retain), nil
}

// load takes the corpus the runs replay: Recorded when it is set, else the stream's contents.
func (s *Sandbox) load(ctx context.Context) error {
	if s.cfg.Recorded != nil {
		s.recorded = s.cfg.Recorded

		return nil
	}

	snapshot, err := s.cfg.Corpus.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read the corpus: %w", err)
	}

	// Never nil once taken, so an empty stream is not read again after a run has written to it.
	s.recorded = append([]corpus.Message{}, snapshot...)

	return nil
}

// startingPoint is the checkpoint every observed run restores: Baseline, or the sandbox's own.
//
// The sandbox's own is taken once, at the first observed run: the corpus stream cleared — its
// messages are already in hand — and the whole bus copied beside the store, never inside the store a
// restore replaces.
func (s *Sandbox) startingPoint(ctx context.Context) (corpus.Checkpoint, error) {
	if s.cfg.Baseline != nil {
		return *s.cfg.Baseline, nil
	}

	if s.checkpoint != nil {
		return *s.checkpoint, nil
	}

	if err := s.cfg.Corpus.Clear(ctx); err != nil {
		return corpus.Checkpoint{}, fmt.Errorf("clear the corpus: %w", err)
	}

	dir := filepath.Join(filepath.Dir(s.cfg.Corpus.StoreDir()), ownCheckpoint)

	taken, err := s.cfg.Corpus.Checkpoint(ctx, dir)
	if err != nil {
		return corpus.Checkpoint{}, fmt.Errorf("checkpoint the bus: %w", err)
	}

	s.checkpoint = &taken

	return taken, nil
}

// begin publishes the corpus once the service has finished starting, with every consumer but the one
// under test paused.
//
// Both halves exist because a real service does not start the way a test fixture does. It runs its
// own startup work — a cleanup timer's first pass fired inside the first message's window on a real
// target — and it may run several consumers in one process, whose effects interleave on shared
// connections with no way to tell them apart. Holding the corpus back makes the first a setup effect;
// pausing makes the second a service with one consumer.
//
// A service that stops by itself while starting ends the wait at once, and nothing is published: there
// is nothing left to hand the corpus to.
func (s *Sandbox) begin(
	ctx context.Context,
	observed *egress,
	exited <-chan struct{},
	run *observedRun,
	recorder *effect.Recorder,
	messages []corpus.Message,
) (interruption, error) {
	consumers, ended, err := s.awaitStartup(ctx, observed, exited, recorder)
	if err != nil || ended != finished {
		return ended, err
	}

	target, err := s.target(consumers)
	if err != nil {
		return finished, err
	}

	for _, other := range consumers {
		if other == target {
			continue
		}

		if err := s.cfg.Corpus.Pause(ctx, other); err != nil {
			return finished, fmt.Errorf("scope the run to consumer %q: %w", target, err)
		}
	}

	// One message in flight, whatever batch the service asks for: across two connections, the order the
	// proxy sees an acknowledgement and the next message's write is not the order they happened in. The
	// same call caps deliveries, so a message the service refuses forever still ends. A named target
	// exists by now; an empty one means the service created no consumer at all, and scope then refuses
	// whatever consumer it creates later.
	if target != "" {
		if err := s.serialise(ctx, run, target); err != nil {
			return finished, err
		}
	}

	admitted := s.openLedger(run, target, messages)

	run.scope(target, s.startupLimit())

	return finished, s.fill(ctx, run, target, messages, admitted)
}

// openLedger opens the run's settlement ledger over the messages the consumer under test admits, and
// reports how many that is. It opens before the first publish, because the service is handed a message
// the moment it lands.
//
// With no consumer named, the run's deliveries follow the configuration the caller declared.
func (s *Sandbox) openLedger(run *observedRun, target string, messages []corpus.Message) int {
	if target == "" {
		run.contract = s.cfg.Policy
	}

	admitted := corpus.Admitted(messages, run.contract.FilterSubjects)

	seqs := make([]uint64, 0, len(admitted))
	for _, message := range admitted {
		seqs = append(seqs, message.Seq)
	}

	run.ledger.Store(newSettlement(run.contract, seqs))

	return len(admitted)
}

// serialise holds the consumer under test to one message in flight and to the delivery cap, and
// refuses one that is not the configuration legality was read from.
func (s *Sandbox) serialise(ctx context.Context, run *observedRun, target string) error {
	discovered, err := s.cfg.Corpus.Serialise(ctx, target, DeliveryCap)
	if err != nil {
		return fmt.Errorf("serialise the consumer under test: %w", err)
	}

	if differs := drift(s.cfg.Policy, discovered); len(differs) > 0 {
		return fmt.Errorf("%w: %s", errDrifted, strings.Join(differs, ", "))
	}

	run.contract = discovered

	return nil
}

// stillSerialised reads the consumer under test back once its run is over, and refuses the run if the
// rewrite that held it to one message in flight and to the delivery cap is no longer in force: the
// service re-created its consumer mid-run, and nothing it did after that can be attributed.
func (s *Sandbox) stillSerialised(ctx context.Context, run *observedRun, ended interruption) error {
	target := run.serialised()
	if target == "" {
		return nil
	}

	after, err := s.cfg.Corpus.Policy(ctx, target)
	if err != nil {
		// A service that exited can take its consumer with it, and then the exit is the outcome.
		if ended == targetExited && s.gone(ctx, target) {
			return nil
		}

		return fmt.Errorf("%w: read it back after the run: %w", errDrifted, err)
	}

	capped := run.contract.EffectiveCap(DeliveryCap)

	var changed []string
	if after.MaxAckPending != 1 {
		changed = append(changed, fmt.Sprintf("MaxAckPending %d, want 1", after.MaxAckPending))
	}

	if after.MaxDeliver != capped {
		changed = append(changed, fmt.Sprintf("MaxDeliver %d, want %d", after.MaxDeliver, capped))
	}

	if len(changed) > 0 {
		return fmt.Errorf("%w: it was re-created during the run (%s)", errDrifted, strings.Join(changed, "; "))
	}

	return nil
}

// gone reports whether a consumer is no longer on the corpus stream.
func (s *Sandbox) gone(ctx context.Context, consumer string) bool {
	listing, err := s.cfg.Corpus.Consumers(ctx)

	return err == nil && !slices.Contains(listing.Corpus, consumer)
}

// drift names every field in which got differs from want, in policy.Config's order. Filter subjects are
// compared as sets and every non-positive MaxDeliver is the same unlimited value, because the delivery
// contract reads them so; nothing else is normalised.
func drift(want, got policy.Config) []string {
	var fields []string

	if want.AckMode != got.AckMode {
		fields = append(fields, "AckMode")
	}

	if !slices.Equal(want.BackOff, got.BackOff) {
		fields = append(fields, "BackOff")
	}

	if !sameSubjects(want.FilterSubjects, got.FilterSubjects) {
		fields = append(fields, "FilterSubjects")
	}

	if want.AckWait != got.AckWait {
		fields = append(fields, "AckWait")
	}

	if want.MaxDeliver != got.MaxDeliver && (want.MaxDeliver > 0 || got.MaxDeliver > 0) {
		fields = append(fields, "MaxDeliver")
	}

	if want.MaxAckPending != got.MaxAckPending {
		fields = append(fields, "MaxAckPending")
	}

	return fields
}

// sameSubjects compares two filters as sets.
func sameSubjects(first, second []string) bool {
	first, second = slices.Clone(first), slices.Clone(second)
	slices.Sort(first)
	slices.Sort(second)

	return slices.Equal(slices.Compact(first), slices.Compact(second))
}

// awaitStartup waits for the service to create a consumer on the corpus stream — the named one, when
// Config.Consumer names it — and then fall quiet, and returns the consumers it created.
//
// Quiet means no effect for the settle period. Nothing is attributed to a message yet, so whatever
// the service does meanwhile is counted as setup, and a pull request is bookkeeping rather than an
// effect, so a service polling an empty stream is already quiet.
//
// With no consumer named, a service that never creates one is given up on at the startup limit and
// the corpus is published regardless: the run then observes nothing, and the observation gate says so
// and says where to look. Returning an error instead would bury that diagnosis under a timeout. A named
// consumer still absent at the limit stops the run in target, before anything is published.
func (s *Sandbox) awaitStartup(
	ctx context.Context,
	observed *egress,
	exited <-chan struct{},
	recorder *effect.Recorder,
) ([]string, interruption, error) {
	started := time.Now()
	quietSince, setup := started, recorder.SetupCount()

	var consumers []string

	ended, err := observed.await(ctx, exited, func(ctx context.Context) (bool, error) {
		listing, err := s.cfg.Corpus.Consumers(ctx)
		if err != nil {
			return false, fmt.Errorf("list the consumers: %w", err)
		}

		consumers = listing.Corpus

		if count := recorder.SetupCount(); count != setup {
			quietSince, setup = time.Now(), count
		}

		created := len(consumers) > 0
		if s.cfg.Consumer != "" {
			created = slices.Contains(consumers, s.cfg.Consumer)
		}

		settled := created && time.Since(quietSince) >= s.settle()

		return settled || time.Since(started) >= s.startupLimit(), nil
	})
	if err != nil {
		return nil, ended, fmt.Errorf("watch the service under test start: %w", err)
	}

	return consumers, ended, nil
}

// target picks the consumer under test from those the service created.
//
// A service with one consumer needs no declaration, which keeps local setup at zero for the common
// case. One with several has to be told which: guessing would put a verdict on the wrong handler.
func (s *Sandbox) target(consumers []string) (string, error) {
	switch {
	case s.cfg.Consumer != "":
		// Absent at the startup limit, it cannot be held to one message in flight, so nothing is
		// published for it to find later.
		if !slices.Contains(consumers, s.cfg.Consumer) {
			found := "none"
			if len(consumers) > 0 {
				found = strings.Join(consumers, ", ")
			}

			return "", fmt.Errorf("%w: %q did not appear within the startup limit of %s (found: %s)",
				errNoSuchConsumer, s.cfg.Consumer, s.startupLimit(), found)
		}

		return s.cfg.Consumer, nil
	case len(consumers) > 1:
		return "", fmt.Errorf("%w: it created %s", errAmbiguousConsumer, strings.Join(consumers, ", "))
	case len(consumers) == 1:
		return consumers[0], nil
	default:
		// None at all: the service never got as far as consuming, and nothing needs pausing.
		return "", nil
	}
}

// settle is how long the service must stay quiet before its startup is considered over.
func (s *Sandbox) settle() time.Duration {
	return s.quiesce() * settleMargin
}

// startupLimit is how long the service may take to create a consumer before the corpus is published
// regardless.
func (s *Sandbox) startupLimit() time.Duration {
	if s.cfg.Startup > 0 {
		return s.cfg.Startup
	}

	return DefaultStartup
}

// scope narrows a snapshot to the retained sequences, keeping recorded order. An empty retain set is
// the whole corpus, which is the driven runner's rule too.
func scope(messages []corpus.Message, retain []uint64) []corpus.Message {
	if len(retain) == 0 {
		return messages
	}

	kept := make(map[uint64]struct{}, len(retain))
	for _, seq := range retain {
		kept[seq] = struct{}{}
	}

	scoped := make([]corpus.Message, 0, len(retain))

	for _, message := range messages {
		if _, wanted := kept[message.Seq]; wanted {
			scoped = append(scoped, message)
		}
	}

	return scoped
}

// drain waits for an observed run to end by settlement: every admitted message done and the service
// quiet for the settle period, or the owed-silence limit reached with messages still owed, or the
// service gone.
//
// The limit's static part is Config.Drain when set, and otherwise the floor derived from the run's
// delivery configuration: long enough for any redelivery the cap allows. A withheld acknowledgement
// produces nothing at all until its deadline passes, so a run that gave up first would report the
// fault as a service that simply stopped.
func (s *Sandbox) drain(
	ctx context.Context,
	observed *egress,
	exited <-chan struct{},
	run *observedRun,
) (interruption, error) {
	static := s.cfg.Drain
	if static <= 0 {
		static = floor(run.contract, s.quiesce())
	}

	ended, err := observed.await(ctx, exited, run.settledOrOwed(s.settle(), static, s.quiesce()))

	run.end()

	if err != nil {
		return ended, fmt.Errorf("wait for the run to end: %w", err)
	}

	return ended, nil
}

func (s *Sandbox) quiesce() time.Duration {
	if s.cfg.Quiesce > 0 {
		return s.cfg.Quiesce
	}

	return replay.DefaultQuiesce
}

// observedRun watches one replay of a service that pulls for itself. It opens an attribution window
// on each delivery the bus hands over, decides the fate of each acknowledgement, and reports when
// the bus has gone quiet.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type observedRun struct {
	policy   *replay.WirePolicy
	windows  *windows
	recorder *effect.Recorder
	// staged is the set of subjects this run publishes. Installed by stage.
	staged atomic.Pointer[map[string]struct{}]
	// core counts, by subject, staged messages handed to a core subscription.
	core map[string]int
	// ledger is the run's settlement ledger, opened before the corpus is published.
	ledger atomic.Pointer[settlement]
	// contract is the configuration the run's deliveries follow: the consumer under test as Serialise
	// read it, before the rewrite — what the service created, with the server's defaults filled in — or,
	// with no consumer named, the configuration the caller declared.
	contract policy.Config
	// recorded translates a sequence the stream is using back to the one the message was recorded
	// under. Installed by stage before the corpus is published, and read from the proxy's goroutines.
	recorded atomic.Pointer[map[uint64]uint64]
	// began anchors activity to the monotonic clock. A wall-clock timestamp will not do: measured on
	// WSL2, the wall clock steps forward by one to two seconds every thirty, and a step inside the drain
	// period ends the run before a withheld acknowledgement's redelivery arrives — a clean sequence
	// reported for a fault that never got to land.
	began time.Time
	// target is the consumer under test. Nil until the service has started, and while nil every
	// consumer on the stream counts; empty once it has, if it had created none.
	target atomic.Pointer[string]
	// strangers names the consumers that took deliveries without being under test.
	strangers sync.Map
	// unstaged holds the stream sequences of fed-back deliveries: messages the consumer under test
	// was handed that Stutter never staged.
	unstaged sync.Map
	// stream is the corpus stream. A delivery or acknowledgement on any other stream is the service's
	// own bus work and is none of this run's business.
	stream string
	// hold keeps deliveries waiting while the corpus is published, and times itself.
	hold fillHold
	// startup is the limit the service had to create its consumer by, which a consumer that turns up
	// later is told it missed.
	startup time.Duration
	// activity is when the bus was last active, as an offset from began.
	activity atomic.Int64
	// published is when the corpus began to be published, and endedAt when the run's wait ended, as
	// offsets from began. Between them is the run's span on the bus.
	published time.Duration
	endedAt   time.Duration
	// lastDelivered is the recorded sequence of the last staged message delivered to the consumer
	// under test, 0 before the first.
	lastDelivered atomic.Uint64
	delivered     atomic.Int64
	failed        atomic.Int64
	// foreign counts deliveries to a consumer that is not under test.
	foreign atomic.Int64
	// elsewhere counts deliveries on another stream while a window was open.
	elsewhere atomic.Int64
	coreMu    sync.Mutex
}

func newObservedRun(
	consumer string,
	stream string,
	wire *replay.WirePolicy,
	recorder *effect.Recorder,
	quiesce time.Duration,
) *observedRun {
	run := &observedRun{
		policy:   wire,
		windows:  &windows{recorder: recorder, consumer: consumer, quiesce: quiesce},
		recorder: recorder,
		began:    time.Now(),
		stream:   stream,
	}

	run.touch()

	return run
}

// Delivered opens the attribution window for a message the bus has just handed over.
//
// The proxy reports it before forwarding the bytes, so the window is already open when the service
// acts on the message. A delivery from any other stream is the service's own bus work and is none of
// this run's business.
func (r *observedRun) Delivered(delivery natsproxy.Delivery) {
	if delivery.Ack.Stream != r.stream {
		// Neither paused nor checked, but how much of it happened inside the run's windows is noted.
		if r.windows.opened() {
			r.elsewhere.Add(1)
		}

		return
	}

	// Opening a window for it would hand the consumer under test's window to a message it never saw.
	if !r.targets(delivery.Ack.Consumer) {
		r.foreign.Add(1)
		r.strangers.Store(delivery.Ack.Consumer, struct{}{})

		return
	}

	r.touch()
	r.delivered.Add(1)

	seq := r.sequence(delivery.Ack.StreamSeq)
	if seq == fedBack {
		r.unstaged.Store(delivery.Ack.StreamSeq, struct{}{})
	} else {
		r.lastDelivered.Store(seq)
	}

	if ledger := r.ledger.Load(); ledger != nil {
		ledger.delivered(seq, delivery.Ack.Deliveries, r.offset())
	}

	r.windows.open(seq, delivery.Payload)
}

// CoreDelivered counts a staged message handed to a core subscription. The subscriber receives the
// corpus beside the consumer under test, and whatever it does lands in the consumer's windows with no
// way to tell the two apart.
func (r *observedRun) CoreDelivered(subject string) {
	staged := r.staged.Load()
	if staged == nil {
		return
	}

	if _, isStaged := (*staged)[subject]; !isStaged {
		return
	}

	r.coreMu.Lock()
	defer r.coreMu.Unlock()

	if r.core == nil {
		r.core = make(map[string]int)
	}

	r.core[subject]++
}

// Pulled takes a pull request on the run's stream into account for the Fill hold's bound. A pull on
// any other stream is the service's own bus work.
func (r *observedRun) Pulled(pull natsproxy.Pull) {
	if pull.Stream != r.stream {
		return
	}

	r.hold.pulled(pull)
}

// Withhold decides the fate of one acknowledgement and closes the window of the message it settles.
func (r *observedRun) Withhold(ack natsproxy.Ack) bool {
	// Another consumer's acknowledgement of the same sequence is not the fault's target: swallowing it
	// would fault a handler nobody chose.
	if ack.Stream != r.stream || !r.targets(ack.Consumer) {
		return false
	}

	r.touch()

	seq := r.sequence(ack.StreamSeq)
	ledger := r.ledger.Load()

	// Asking for more time settles nothing, so it neither closes a window nor may be swallowed; it
	// restarts the clock on the delivery's deadline.
	if ack.InProgress() {
		if ledger != nil {
			ledger.acknowledged(seq, ack, false, r.offset())
		}

		return false
	}

	if ack.Negative() {
		r.failed.Add(1)
	}

	// The mutation was built against recorded sequences, so the wire's own numbering is translated
	// before the fault is decided rather than after. A message Stutter never staged translates to
	// fedBack, which no fault targets, whatever sequence it landed at.
	withheld := r.policy.Withhold(natsproxy.Ack{
		Stream:     ack.Stream,
		Consumer:   ack.Consumer,
		Payload:    ack.Payload,
		StreamSeq:  seq,
		Deliveries: ack.Deliveries,
	})

	if ledger != nil {
		ledger.acknowledged(seq, ack, withheld, r.offset())
	}

	r.windows.settle(seq)

	return withheld
}

// options are the hooks the run is watched, faulted and held through on the bus.
func (r *observedRun) options() natsproxy.Options {
	return natsproxy.Options{Acks: r, Deliveries: r, Pulls: r, Hold: &r.hold.gate}
}

// stage installs the translation from the sequences this run's corpus lands at to the ones its
// messages were recorded under. It runs before the first publish, because the proxy may report a
// delivery before Fill has heard back where it landed.
func (r *observedRun) stage(messages []corpus.Message, first uint64) {
	recorded := make(map[uint64]uint64, len(messages))
	for _, message := range corpus.Numbering(messages, first) {
		recorded[message.Sequence] = message.Recorded
	}

	subjects := make(map[string]struct{}, len(messages))
	for _, message := range messages {
		subjects[message.Subject] = struct{}{}
	}

	r.recorded.Store(&recorded)
	r.staged.Store(&subjects)
}

// settledOrOwed is the wait's condition for the run's end: every admitted message done and the
// consumer under test's deliveries and acknowledgements quiet for the settle period; or, with messages
// still owed, quiet for the owed-silence limit. Reaching the limit means nothing is moving, so the cut
// falls in the same place in every run.
func (r *observedRun) settledOrOwed(settle, static, quiesce time.Duration) func(context.Context) (bool, error) {
	return func(context.Context) (bool, error) {
		ledger, silence := r.ledger.Load(), r.silence()

		if silence >= settle && ledger.settled(r.offset()) {
			return true, nil
		}

		return silence >= ledger.limit(static, quiesce), nil
	}
}

// end notes when the run's wait ended: what is owed is counted as of then, and the span ends there.
func (r *observedRun) end() {
	r.endedAt = r.offset()
}

// publishing notes when the corpus began to be published: the span starts there.
func (r *observedRun) publishing() {
	r.published = r.offset()
}

// ended reads what the service stopping by itself means for the run, completing its exit with the
// message it followed. interrupted says the run's own wait ended on the exit.
//
// Before its first delivery the service left nothing to compare, whatever run it was, so the run cannot
// be used. A restart is refused anywhere: the service came back with none of its state, and what it did
// next cannot be attributed to the message it was handling.
func (r *observedRun) ended(exit *replay.Exit, interrupted bool) error {
	exit.After = r.lastDelivered.Load()
	exit.Exited = exit.Exited || interrupted

	var err error

	if exit.Exited && exit.After == 0 {
		err = &replay.ExitError{Exit: *exit}
	}

	if exit.Restarts > 0 {
		err = errors.Join(err, fmt.Errorf("%w (RestartCount %d): attribution is unsound", errRestarted, exit.Restarts))
	}

	return err
}

// finish ends whatever window is still open, once the run is over.
func (r *observedRun) finish() {
	r.windows.finish()
}

// scope names the consumer under test, just before the corpus is published. It restarts the drain
// clock too: the run begins at publication, and a service that took longer to start than the drain
// period would otherwise be called finished before its first delivery.
//
// An empty name means the service had created no consumer by the startup limit. Every consumer is then
// a stranger: one created later was never held to one message in flight, so none of its deliveries is
// attributed or faulted, and any of them stops the run.
func (r *observedRun) scope(consumer string, startup time.Duration) {
	r.target.Store(&consumer)
	r.startup = startup
	r.touch()
}

// serialised is the consumer under test once Serialise has held it: empty before, and when the service
// had created none.
func (r *observedRun) serialised() string {
	if target := r.target.Load(); target != nil {
		return *target
	}

	return ""
}

// targets reports whether a consumer is the one under test.
func (r *observedRun) targets(consumer string) bool {
	target := r.target.Load()

	return target == nil || *target == consumer
}

// unscoped fails a run in which a consumer that was not under test took deliveries. Its effects sit in
// the run's sequence with no way to separate them, so a verdict would be on two handlers at once.
func (r *observedRun) unscoped() error {
	count := r.foreign.Load()
	if count == 0 {
		return nil
	}

	if target := r.target.Load(); target != nil && *target == "" {
		return fmt.Errorf("%w: %d deliveries to %s, which did not exist at the startup limit of %s",
			errLateConsumer, count, r.strangerNames(), r.startup)
	}

	return fmt.Errorf("%w: %d deliveries to a consumer that was not paused, "+
		"most likely one created after the service had finished starting", errUnscoped, count)
}

// coreSubscribed fails a run in which a core subscription was handed staged messages, naming each
// subject and how many.
func (r *observedRun) coreSubscribed() error {
	r.coreMu.Lock()
	defer r.coreMu.Unlock()

	if len(r.core) == 0 {
		return nil
	}

	counts := make([]string, 0, len(r.core))
	for _, subject := range slices.Sorted(maps.Keys(r.core)) {
		counts = append(counts, fmt.Sprintf("%s (%d)", subject, r.core[subject]))
	}

	return fmt.Errorf("%w: a core subscription was handed staged messages on %s, "+
		"and its work cannot be told apart from the consumer's", errUnscoped, strings.Join(counts, ", "))
}

// strangerNames lists the consumers that took deliveries without being under test, in name order.
func (r *observedRun) strangerNames() string {
	var names []string

	r.strangers.Range(func(name, _ any) bool {
		if text, isText := name.(string); isText {
			names = append(names, text)
		}

		return true
	})

	slices.Sort(names)

	return strings.Join(names, ", ")
}

// result is what the run observed, in the shape a driven run reports, or why it cannot be used.
//
// Failed counts negative acknowledgements rather than handler errors: a service Stutter does not
// call has no return value to read, so asking the bus for redelivery is the only way it can say a
// delivery failed.
func (r *observedRun) result(clause string) (replay.Result, error) {
	if err := r.unscoped(); err != nil {
		return replay.Result{}, err
	}

	if err := r.coreSubscribed(); err != nil {
		return replay.Result{}, err
	}

	effects := r.recorder.Effects()
	unstaged := r.unstagedSequences()

	if worked := len(forMessage(effect.Compared(effects), fedBack)); worked > 0 {
		return replay.Result{}, fmt.Errorf("%w (stream sequence %s; effects recorded: %d)",
			errFedBack, strings.Join(unstaged, ", "), worked)
	}

	var owed, exhausted, capped int
	if ledger := r.ledger.Load(); ledger != nil {
		owed, exhausted, capped = ledger.owed(r.endedAt), ledger.exhausted(r.endedAt), ledger.deliveryCap()
	}

	var span time.Duration
	if r.published > 0 && r.endedAt > r.published {
		span = r.endedAt - r.published
	}

	return replay.Result{
		Clause:          clause,
		Effects:         effects,
		Delivered:       int(r.delivered.Load()),
		Failed:          int(r.failed.Load()),
		Owed:            owed,
		Exhausted:       exhausted,
		DeliveryCap:     capped,
		Late:            r.recorder.LateCount(),
		Setup:           r.recorder.SetupCount(),
		FedBack:         len(unstaged),
		Elsewhere:       int(r.elsewhere.Load()),
		Refusals:        r.recorder.Refusals(),
		NoResponders:    r.recorder.NoResponders(),
		ClosedAfterInfo: r.recorder.ClosedAfterInfoCount(),
		Span:            span,
	}, nil
}

// unstagedSequences lists the stream sequences of fed-back deliveries, in order.
func (r *observedRun) unstagedSequences() []string {
	var sequences []uint64

	r.unstaged.Range(func(seq, _ any) bool {
		if number, isNumber := seq.(uint64); isNumber {
			sequences = append(sequences, number)
		}

		return true
	})

	slices.Sort(sequences)

	listed := make([]string, 0, len(sequences))
	for _, seq := range sequences {
		listed = append(listed, strconv.FormatUint(seq, 10))
	}

	return listed
}

// forMessage narrows effects to those attributed to one message.
func forMessage(effects []effect.Effect, message uint64) []effect.Effect {
	var narrowed []effect.Effect

	for _, item := range effects {
		if item.MessageSeq == message {
			narrowed = append(narrowed, item)
		}
	}

	return narrowed
}

// sequence translates a stream sequence the bus is using into the one the message was recorded
// under, or fedBack for a message Stutter did not stage.
//
// Staging lands the corpus wherever the stream is up to, so without the translation a fault aimed at
// a recorded sequence would land on a different message, and every effect would be attributed to
// one. A sequence staging never used is the service's own output fed back, or a message already in
// the stream, and passing it off as recorded would attribute it to a corpus message — or aim that
// message's fault at it.
func (r *observedRun) sequence(staged uint64) uint64 {
	translation := r.recorded.Load()
	if translation == nil {
		return fedBack
	}

	if recorded, known := (*translation)[staged]; known {
		return recorded
	}

	return fedBack
}

func (r *observedRun) touch() {
	r.activity.Store(int64(r.offset()))
}

// offset is how long the run has been going, on the monotonic clock.
func (r *observedRun) offset() time.Duration {
	return time.Since(r.began)
}

// silence is how long the bus has been quiet, measured on the monotonic clock.
func (r *observedRun) silence() time.Duration {
	return time.Since(r.began) - time.Duration(r.activity.Load())
}

// windows attributes effects to the message in flight for a service that pulls for itself.
//
// A driven run closes a window a quiesce after the handler returns, with the driver holding the next
// message back meanwhile. Nothing holds an observed service back, so a window also ends the moment
// the next delivery arrives: an effect that outran its own message is better attributed to the one
// that followed than recorded against a message the service had already finished with.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type windows struct {
	recorder *effect.Recorder
	timer    *time.Timer
	consumer string
	quiesce  time.Duration
	// current is the message the open window belongs to, so an acknowledgement settles its own
	// window and never someone else's.
	current uint64
	// epoch counts windows, so a close armed for one cannot shut the one that replaced it.
	epoch  uint64
	active bool
	mu     sync.Mutex
}

// open starts a window for a delivery.
func (w *windows) open(seq uint64, payload []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.stop()
	w.epoch++
	w.current = seq
	w.active = true
	w.recorder.Open(w.consumer, seq, payload)
}

// settle closes the window a quiesce after its message was acknowledged, which catches writes the
// handler started but did not wait for.
//
// The wait runs on a timer because the proxy calls this on the connection it is forwarding: blocking
// here would stall the service's own traffic and change the run being measured.
func (w *windows) settle(seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.active || w.current != seq {
		return
	}

	w.stop()

	epoch := w.epoch
	w.timer = time.AfterFunc(w.quiesce, func() { w.closeEpoch(epoch) })
}

// closeEpoch ends the window the timer was armed for, and does nothing if a later delivery has
// already replaced it.
func (w *windows) closeEpoch(epoch uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.epoch != epoch {
		return
	}

	w.active = false
	w.recorder.Close()
}

// finish ends whatever window is still open.
func (w *windows) finish() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.stop()

	if w.active {
		w.active = false
		w.recorder.Close()
	}
}

// opened reports whether a message's window is open.
func (w *windows) opened() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.active
}

// stop disarms a pending close. The caller holds the lock.
func (w *windows) stop() {
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
}
