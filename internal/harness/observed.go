package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
)

const (
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

	staged, err := s.stage(ctx, retain)
	if err != nil {
		return replay.Result{}, err
	}

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), s.cfg.HashKey)
	run := newObservedRun(name, s.cfg.Corpus.Topic().Stream, wire, recorder, staged, s.quiesce())

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

	runErr := run.awaitQuiet(ctx, s.drainWait())
	run.finish()

	// Ordered deliberately: the forwarding proxies wait for in-flight connections, so a service
	// holding an idle connection open would make teardown hang rather than fail.
	service.Close(ctx)

	if err := observed.settle(ctx, runErr); err != nil {
		return replay.Result{}, err
	}

	return run.result(verdict.Clause), nil
}

// stage rebuilds the corpus stream so it holds only the messages this run is scoped to.
//
// It is both the run's scoping and its bus-side reset: rebuilding takes the previous run's consumer
// with it, which is what stops a second run resuming where the first one stopped.
//
// The corpus is snapshotted once, before the first run reduces it. Sandbox runs are serial — a check
// compares one run against the next — so the snapshot needs no guard of its own.
func (s *Sandbox) stage(ctx context.Context, retain []uint64) ([]corpus.Staged, error) {
	if s.recorded == nil {
		snapshot, err := s.cfg.Corpus.Snapshot(ctx)
		if err != nil {
			return nil, fmt.Errorf("read the corpus: %w", err)
		}

		s.recorded = snapshot
	}

	staged, err := s.cfg.Corpus.Stage(ctx, scope(s.recorded, retain))
	if err != nil {
		return nil, fmt.Errorf("stage the corpus: %w", err)
	}

	return staged, nil
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
	// stream is the corpus stream. A delivery or acknowledgement on any other stream is the service's
	// own bus work and is none of this run's business.
	stream    string
	activity  atomic.Int64
	delivered atomic.Int64
	failed    atomic.Int64
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

	r.touch()
	r.delivered.Add(1)
	r.windows.open(r.sequence(delivery.Ack.StreamSeq), delivery.Payload)
}

// Withhold decides the fate of one acknowledgement and closes the window of the message it settles.
func (r *observedRun) Withhold(ack natsproxy.Ack) bool {
	if ack.Stream != r.stream {
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
			if time.Since(r.lastActivity()) >= drain {
				return nil
			}
		}
	}
}

// finish ends whatever window is still open, once the run is over.
func (r *observedRun) finish() {
	r.windows.finish()
}

// result is what the run observed, in the shape a driven run reports.
//
// Failed counts negative acknowledgements rather than handler errors: a service Stutter does not
// call has no return value to read, so asking the bus for redelivery is the only way it can say a
// delivery failed.
func (r *observedRun) result(clause string) replay.Result {
	return replay.Result{
		Clause:    clause,
		Effects:   r.recorder.Effects(),
		Delivered: int(r.delivered.Load()),
		Failed:    int(r.failed.Load()),
		Late:      r.recorder.LateCount(),
		Setup:     r.recorder.SetupCount(),
	}
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
	r.activity.Store(time.Now().UnixNano())
}

func (r *observedRun) lastActivity() time.Time {
	return time.Unix(0, r.activity.Load())
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
