package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
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
)

const (
	// settleMargin multiplies the quiesce to get how long a starting service must stay quiet before
	// the corpus is published. Startup work is the same scale of thing as a handler's trailing writes,
	// and a few of them in a row is what separates a finished startup from a pause inside one.
	settleMargin = 5
	// defaultStartup is how long a service may take to create a consumer. Generous, because it is only
	// ever spent in full on a service that never does, and that run is already a failed one.
	defaultStartup = 10 * time.Second
	// drainMargin multiplies the longest redelivery deadline to get a default drain wait. A
	// redelivery lands at the deadline, not before it, so the margin is what stops a run that is
	// merely waiting from being called finished.
	drainMargin = 2
	// drainPoll is how often an observed run checks whether the bus has gone quiet. Well below any
	// redelivery deadline a consumer would be configured with, so it costs the run no accuracy.
	drainPoll = 25 * time.Millisecond
)

// Consumer is a service that pulls from the bus for itself.
//
// It has no Handle: Stutter never delivers to it. A provisioned container creates its own JetStream
// consumer and acknowledges on its own connection, which is why the run is watched and faulted on
// the wire rather than driven.
type Consumer interface {
	// Close releases the service's connections. It runs BEFORE the proxies are closed.
	Close(ctx context.Context)
}

// Start builds a service that consumes for itself, against the proxied addresses.
type Start func(ctx context.Context, at Addresses) (Consumer, error)

// runObserved replays the corpus into a service Stutter does not dispatch to.
//
// The shape is the driven run's, with three substitutions: the stream is rebuilt to scope the run
// because the service cannot be told to skip a message, the fault is injected by swallowing
// acknowledgements on the wire, and the run ends when the bus goes quiet rather than when the driver
// stops delivering.
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
	run := newObservedRun(name, s.cfg.Corpus.Topic().Stream, wire, recorder, corpus.Numbering(messages), s.quiesce())

	observed, err := s.observe(ctx, recorder, natsproxy.Options{Acks: run, Deliveries: run})
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

	runErr := s.begin(ctx, run, recorder, messages)
	if runErr == nil {
		runErr = run.awaitQuiet(ctx, s.drainWait())
	}

	run.finish()

	// Ordered deliberately: the forwarding proxies wait for in-flight connections, so a service
	// holding an idle connection open would make teardown hang rather than fail.
	service.Close(ctx)

	if err := observed.settle(ctx, runErr); err != nil {
		return replay.Result{}, err
	}

	return run.result(verdict.Clause)
}

// prepare returns the part of the corpus this run replays, and clears the stream to receive it.
//
// Cleared rather than staged outright: the service starts against a stream holding nothing, and the
// corpus arrives only once it has finished starting. Clearing is also the bus-side reset, since it
// takes the previous run's consumers with the stream.
//
// The corpus is snapshotted once, before the first run reduces it. Sandbox runs are serial — a check
// compares one run against the next — so the snapshot needs no guard of its own.
func (s *Sandbox) prepare(ctx context.Context, retain []uint64) ([]corpus.Message, error) {
	if s.recorded == nil {
		snapshot, err := s.cfg.Corpus.Snapshot(ctx)
		if err != nil {
			return nil, fmt.Errorf("read the corpus: %w", err)
		}

		s.recorded = snapshot
	}

	if err := s.cfg.Corpus.Clear(ctx); err != nil {
		return nil, fmt.Errorf("clear the corpus: %w", err)
	}

	return scope(s.recorded, retain), nil
}

// begin publishes the corpus once the service has finished starting, with every consumer but the one
// under test paused.
//
// Both halves exist because a real service does not start the way a test fixture does. It runs its
// own startup work — a cleanup timer's first pass fired inside the first message's window on a real
// target — and it may run several consumers in one process, whose effects interleave on shared
// connections with no way to tell them apart. Holding the corpus back makes the first a setup effect;
// pausing makes the second a service with one consumer.
func (s *Sandbox) begin(
	ctx context.Context,
	run *observedRun,
	recorder *effect.Recorder,
	messages []corpus.Message,
) error {
	consumers, err := s.awaitStartup(ctx, recorder)
	if err != nil {
		return err
	}

	target, err := s.target(consumers)
	if err != nil {
		return err
	}

	for _, other := range consumers {
		if other == target {
			continue
		}

		if err := s.cfg.Corpus.Pause(ctx, other); err != nil {
			return fmt.Errorf("scope the run to consumer %q: %w", target, err)
		}
	}

	run.scope(target)

	if err := s.cfg.Corpus.Fill(ctx, messages); err != nil {
		return fmt.Errorf("stage the corpus: %w", err)
	}

	return nil
}

// awaitStartup waits for the service to create a consumer on the corpus stream and then fall quiet,
// and returns the consumers it created.
//
// Quiet means no effect for the settle period. Nothing is attributed to a message yet, so whatever
// the service does meanwhile is counted as setup, and a pull request is bookkeeping rather than an
// effect, so a service polling an empty stream is already quiet.
//
// A service that never creates a consumer is given up on at the startup limit and the corpus is
// published regardless: the run then observes nothing, and the observation gate says so and says
// where to look. Returning an error instead would bury that diagnosis under a timeout.
func (s *Sandbox) awaitStartup(ctx context.Context, recorder *effect.Recorder) ([]string, error) {
	ticker := time.NewTicker(drainPoll)
	defer ticker.Stop()

	started := time.Now()
	quietSince, setup := started, recorder.SetupCount()

	for {
		consumers, err := s.cfg.Corpus.Consumers(ctx)
		if err != nil {
			return nil, fmt.Errorf("watch the service under test start: %w", err)
		}

		if count := recorder.SetupCount(); count != setup {
			quietSince, setup = time.Now(), count
		}

		settled := len(consumers) > 0 && time.Since(quietSince) >= s.settle()
		if settled || time.Since(started) >= s.startupLimit() {
			return consumers, nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("cancelled while the service under test was starting: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// target picks the consumer under test from those the service created.
//
// A service with one consumer needs no declaration, which keeps local setup at zero for the common
// case. One with several has to be told which: guessing would put a verdict on the wrong handler.
func (s *Sandbox) target(consumers []string) (string, error) {
	switch {
	case s.cfg.Consumer != "":
		if len(consumers) > 0 && !slices.Contains(consumers, s.cfg.Consumer) {
			return "", fmt.Errorf("%w: %q is not among them (%s)",
				errNoSuchConsumer, s.cfg.Consumer, strings.Join(consumers, ", "))
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

	return defaultStartup
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

// drainWait is how long the bus must stay silent before an observed run is called finished.
//
// It has to outlast the longest redelivery the recorded configuration permits. A withheld
// acknowledgement produces nothing at all until the deadline passes, so a run that gave up first
// would report the fault as a service that simply stopped — a clean sequence where the fault's whole
// effect was still to come.
func (s *Sandbox) drainWait() time.Duration {
	if s.cfg.Drain > 0 {
		return s.cfg.Drain
	}

	longest := s.cfg.Policy.Deadline(1)

	// The last backoff entry governs every attempt after it, so the curve's own length bounds the
	// search however high MaxDeliver is set.
	for attempt := 2; attempt <= len(s.cfg.Policy.BackOff); attempt++ {
		if deadline := s.cfg.Policy.Deadline(attempt); deadline > longest {
			longest = deadline
		}
	}

	return longest*drainMargin + s.quiesce()
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
	// recorded translates a sequence the rebuilt stream is using back to the one the message was
	// recorded under.
	recorded map[uint64]uint64
	// began anchors activity to the monotonic clock. A wall-clock timestamp will not do: measured on
	// WSL2, the wall clock steps forward by one to two seconds every thirty, and a step inside the drain
	// period ends the run before a withheld acknowledgement's redelivery arrives — a clean sequence
	// reported for a fault that never got to land.
	began time.Time
	// target is the consumer under test. Nil until the service has started, and for good if it never
	// created one; while nil, every consumer on the stream counts.
	target atomic.Pointer[string]
	// stream is the corpus stream. A delivery or acknowledgement on any other stream is the service's
	// own bus work and is none of this run's business.
	stream string
	// activity is when the bus was last active, as an offset from began.
	activity  atomic.Int64
	delivered atomic.Int64
	failed    atomic.Int64
	// foreign counts deliveries to a consumer that is not under test.
	foreign atomic.Int64
}

func newObservedRun(
	consumer string,
	stream string,
	wire *replay.WirePolicy,
	recorder *effect.Recorder,
	staged []corpus.Staged,
	quiesce time.Duration,
) *observedRun {
	recorded := make(map[uint64]uint64, len(staged))
	for _, message := range staged {
		recorded[message.Sequence] = message.Recorded
	}

	run := &observedRun{
		policy:   wire,
		windows:  &windows{recorder: recorder, consumer: consumer, quiesce: quiesce},
		recorder: recorder,
		recorded: recorded,
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
		return
	}

	// Opening a window for it would hand the consumer under test's window to a message it never saw.
	if !r.targets(delivery.Ack.Consumer) {
		r.foreign.Add(1)

		return
	}

	r.touch()
	r.delivered.Add(1)
	r.windows.open(r.sequence(delivery.Ack.StreamSeq), delivery.Payload)
}

// Withhold decides the fate of one acknowledgement and closes the window of the message it settles.
func (r *observedRun) Withhold(ack natsproxy.Ack) bool {
	// Another consumer's acknowledgement of the same sequence is not the fault's target: swallowing it
	// would fault a handler nobody chose.
	if ack.Stream != r.stream || !r.targets(ack.Consumer) {
		return false
	}

	r.touch()

	// Asking for more time settles nothing, so it neither closes a window nor may be swallowed.
	if ack.InProgress() {
		return false
	}

	if ack.Negative() {
		r.failed.Add(1)
	}

	seq := r.sequence(ack.StreamSeq)

	// The mutation was built against recorded sequences, so the wire's own numbering is translated
	// before the fault is decided rather than after.
	withheld := r.policy.Withhold(natsproxy.Ack{
		Stream:     ack.Stream,
		Consumer:   ack.Consumer,
		Payload:    ack.Payload,
		StreamSeq:  seq,
		Deliveries: ack.Deliveries,
	})

	r.windows.settle(seq)

	return withheld
}

// awaitQuiet blocks until the bus has handed nothing over and settled nothing for the drain period.
//
// Silence is the only honest signal an observed run has that it is over: Stutter no longer decides
// when to stop delivering.
func (r *observedRun) awaitQuiet(ctx context.Context, drain time.Duration) error {
	ticker := time.NewTicker(drainPoll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("observed run cancelled before the bus went quiet: %w", ctx.Err())
		case <-ticker.C:
			if r.silence() >= drain {
				return nil
			}
		}
	}
}

// finish ends whatever window is still open, once the run is over.
func (r *observedRun) finish() {
	r.windows.finish()
}

// scope names the consumer under test, just before the corpus is published. It restarts the drain
// clock too: the run begins at publication, and a service that took longer to start than the drain
// period would otherwise be called finished before its first delivery.
func (r *observedRun) scope(consumer string) {
	if consumer != "" {
		r.target.Store(&consumer)
	}

	r.touch()
}

// targets reports whether a consumer is the one under test.
func (r *observedRun) targets(consumer string) bool {
	target := r.target.Load()

	return target == nil || *target == consumer
}

// unscoped fails a run in which a consumer that was not under test took deliveries. Its effects sit in
// the run's sequence with no way to separate them, so a verdict would be on two handlers at once.
func (r *observedRun) unscoped() error {
	if count := r.foreign.Load(); count > 0 {
		return fmt.Errorf("%w: %d deliveries to a consumer that was not paused, "+
			"most likely one created after the service had finished starting", errUnscoped, count)
	}

	return nil
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

	return replay.Result{
		Clause:    clause,
		Effects:   r.recorder.Effects(),
		Delivered: int(r.delivered.Load()),
		Failed:    int(r.failed.Load()),
		Late:      r.recorder.LateCount(),
		Setup:     r.recorder.SetupCount(),
	}, nil
}

// sequence translates a stream sequence the bus is using into the one the message was recorded
// under.
//
// Staging renumbers the stream from one, so without the translation a fault aimed at a recorded
// sequence would land on a different message, and every effect would be attributed to one.
func (r *observedRun) sequence(staged uint64) uint64 {
	if recorded, known := r.recorded[staged]; known {
		return recorded
	}

	return staged
}

func (r *observedRun) touch() {
	r.activity.Store(int64(time.Since(r.began)))
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

// stop disarms a pending close. The caller holds the lock.
func (w *windows) stop() {
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
}
