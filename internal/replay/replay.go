// Package replay drives a corpus into a service under test and collects the effect sequence it
// produced.
//
// Delivery is serial by default: exactly one message is outstanding at a time, so every effect
// observed while its attribution window is open provably belongs to it. Concurrency is a deliberate
// mutation, never the default, and that choice is what makes attribution exact without correlating
// connections.
//
// The replay consumer is configured from the recorded consumer's own settings, backoff curve
// included, so a fault is only ever injected under the delivery contract the corpus was recorded
// under.
package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/policy"
)

const (
	// defaultQuiesce is how long a message's attribution window stays open after its handler
	// returns, to catch writes the handler started but did not wait for.
	defaultQuiesce = 100 * time.Millisecond
	// defaultFetchWait bounds how long Next blocks before concluding the corpus is drained.
	defaultFetchWait = 2 * time.Second
)

// ErrForbidden means the recorded configuration does not permit the requested fault.
//
// Refusing is the point: a finding produced by a fault the bus cannot commit is unfixable, and one
// unfixable finding costs more than every real one it sits beside.
var ErrForbidden = errors.New("the recorded configuration does not permit this fault")

// ErrUnsupported means Stutter can describe the fault but cannot yet inject it honestly.
var ErrUnsupported = errors.New("fault not yet supported")

// Message is one delivery handed to the service under test.
type Message struct {
	Subject string
	Payload []byte
	Seq     uint64
	// Deliveries is 1 on first delivery and increments on each redelivery, so a handler and a
	// mutation can both tell a duplicate from an original.
	Deliveries uint64
}

// Handler is the service under test.
type Handler func(ctx context.Context, msg Message) error

// Mutation decides how a delivery is disturbed.
//
// The three levers are all the delivery contract itself offers: hold a message before working on
// it, dispatch it after a later one, or withhold its acknowledgement. Everything the bus is allowed
// to do to a consumer reduces to some combination of those.
type Mutation interface {
	// Fault identifies the delivery fault, both for the legality check and for the report.
	Fault() policy.Fault
	// HoldFor is how long to hold a delivery before dispatching it to the handler. A hold that
	// outlasts the governing deadline makes the server redeliver while the first delivery is still
	// being worked on.
	HoldFor(msg Message) time.Duration
	// Defer reports whether to hold this delivery back and dispatch it after the following one.
	Defer(msg Message) bool
	// WithholdAck reports whether to withhold the acknowledgement once the handler returns.
	WithholdAck(msg Message) bool
}

// inert supplies the neutral behaviour every mutation inherits, so each one declares only the lever
// it actually pulls.
type inert struct{}

// HoldFor holds nothing.
func (inert) HoldFor(Message) time.Duration { return 0 }

// Defer defers nothing.
func (inert) Defer(Message) bool { return false }

// WithholdAck withholds nothing.
func (inert) WithholdAck(Message) bool { return false }

// Clean applies no fault. It produces the reference run.
type Clean struct{ inert }

// Fault identifies the mutation.
func (Clean) Fault() policy.Fault { return policy.FaultNone }

// Duplicate delivers one message twice by withholding the ack on its first delivery only.
//
// The handler runs, the bus redelivers, the handler runs again, and the second delivery is
// acknowledged. Net effect on the service: it saw the message twice — precisely what at-least-once
// delivery permits.
type Duplicate struct {
	inert

	// Seq is the stream sequence of the message to duplicate.
	Seq uint64
}

// Fault identifies the mutation.
func (Duplicate) Fault() policy.Fault { return policy.FaultDuplicate }

// WithholdAck withholds on the first delivery of the target message only.
func (d Duplicate) WithholdAck(msg Message) bool {
	return msg.Seq == d.Seq && msg.Deliveries == 1
}

// CrashBeforeAck models a consumer that dies between performing its side effect and reporting it.
//
// Mechanically this is a withheld acknowledgement, exactly like Duplicate — the bus cannot tell a
// consumer that died after its side effect from one that simply never acknowledged, so there is no
// separate lever to pull and the code does not pretend otherwise. Where the two genuinely differ is
// repetition: a crash loop withholds again on each redelivery, up to Times.
type CrashBeforeAck struct {
	inert

	// Seq is the stream sequence of the message whose consumer crashes.
	Seq uint64
	// Times is how many deliveries crash before one succeeds. Zero means one.
	Times uint64
}

// Fault identifies the mutation.
func (CrashBeforeAck) Fault() policy.Fault { return policy.FaultCrashBeforeAck }

// WithholdAck withholds for the first Times deliveries of the target message.
func (c CrashBeforeAck) WithholdAck(msg Message) bool {
	times := c.Times
	if times == 0 {
		times = 1
	}

	return msg.Seq == c.Seq && msg.Deliveries <= times
}

// Delay models a consumer too slow to acknowledge within the deadline governing its attempt.
//
// For must be sized against policy.Config.Deadline for the attempt in question, not against the
// declared ack wait: where a consumer sets a backoff curve, the curve is the deadline.
//
// It holds the message and then withholds the acknowledgement, because an acknowledgement that
// arrives after the deadline has effectively not arrived — the bus has already decided the message
// is unhandled. Holding alone is not enough: a pull consumer with nothing fetching has nowhere for
// the server to redeliver into, so a late ack would still settle the message and the fault would
// model nothing.
//
// Limitation, stated rather than hidden: under serial delivery the two attempts cannot overlap, so
// this reproduces the redelivery but not a second worker running while the first is still going.
// That needs concurrency, which is gated.
type Delay struct {
	inert

	// Seq is the stream sequence of the message to hold.
	Seq uint64
	// For is how long to hold it before the handler sees it.
	For time.Duration
}

// Fault identifies the mutation.
func (Delay) Fault() policy.Fault { return policy.FaultDelay }

// HoldFor holds the target message on its first delivery.
func (d Delay) HoldFor(msg Message) time.Duration {
	if d.targets(msg) {
		return d.For
	}

	return 0
}

// WithholdAck withholds on the delivery that was held, so the missed deadline actually costs a
// redelivery.
func (d Delay) WithholdAck(msg Message) bool {
	return d.targets(msg)
}

func (d Delay) targets(msg Message) bool {
	return msg.Seq == d.Seq && msg.Deliveries == 1
}

// Reorder dispatches one message after the message that followed it.
//
// It requires headroom for two outstanding messages, which is exactly the condition under which the
// bus stops guaranteeing order.
type Reorder struct {
	inert

	// First is the stream sequence of the message to hold back.
	First uint64
}

// Fault identifies the mutation.
func (Reorder) Fault() policy.Fault { return policy.FaultReorder }

// Defer holds the target message back on its first delivery.
func (r Reorder) Defer(msg Message) bool {
	return msg.Seq == r.First && msg.Deliveries == 1
}

// Concurrent delivers two messages at once.
//
// NOT YET INJECTABLE. Dispatching two handlers simultaneously interleaves their effects on the
// wire, and window-based attribution then assigns effects to the wrong message — which manufactures
// false positives, the one failure mode the report cannot survive. It needs per-connection
// correlation between the driver and the proxy. The type and its legality rule exist so the gap is
// visible rather than merely absent.
type Concurrent struct {
	inert

	// First and Second are the stream sequences to deliver together.
	First  uint64
	Second uint64
}

// Fault identifies the mutation.
func (Concurrent) Fault() policy.Fault { return policy.FaultConcurrent }

// Result is one run's observable outcome.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Result struct {
	// Clause is the configuration that licensed this run's fault, quoted into any finding.
	Clause string
	// Effects is the sequence the run produced, in observation order.
	Effects []effect.Effect
	// Delivered counts handler invocations, including redeliveries.
	Delivered int
	// Failed counts handler invocations that returned an error. A high count on a clean run means
	// the corpus is being rejected, which must be seen before any divergence is believed.
	Failed int
	// Late counts effects that arrived after their attribution window closed.
	Late int
}

// Options tunes a run. The zero value is usable and applies the package defaults.
//
// Delivery semantics are not here: they come from the recorded consumer configuration.
type Options struct {
	// Retain, when non-empty, limits the run to these stream sequences. Every other message is
	// acknowledged without ever reaching the handler.
	//
	// This is scoping, not a fault. It is how a shrinker asks "does it still reproduce with only
	// these messages?", and it deliberately does not go through the legality table: restricting an
	// experiment is not something the bus does to a consumer, so it needs no licence. Routing it
	// through FaultDrop instead would let a configuration that forbids dropping block shrinking,
	// which would leave every finding without a minimal repro.
	Retain []uint64
	// Quiesce is how long to wait after a handler returns before closing its window.
	Quiesce time.Duration
	// FetchWait bounds the wait for a redelivery before the corpus is called drained.
	FetchWait time.Duration
}

func (o Options) withDefaults() Options {
	if o.Quiesce <= 0 {
		o.Quiesce = defaultQuiesce
	}

	if o.FetchWait <= 0 {
		o.FetchWait = defaultFetchWait
	}

	return o
}

// Runner replays a corpus under the delivery contract it was recorded with.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Runner struct {
	store    *corpus.Corpus
	canon    *effect.Canonicaliser
	key      []byte
	retained map[uint64]struct{}
	opts     Options
	config   policy.Config
}

// NewRunner builds a Runner.
//
// config is the recorded consumer's own configuration: it shapes the replay consumer and decides
// which faults may be injected. hashKey keys the raw-bytes hash used to diagnose the normaliser, and
// every run being compared must share one.
func NewRunner(store *corpus.Corpus, hashKey []byte, config policy.Config, opts Options) *Runner {
	retained := make(map[uint64]struct{}, len(opts.Retain))
	for _, seq := range opts.Retain {
		retained[seq] = struct{}{}
	}

	return &Runner{
		store:    store,
		canon:    effect.NewCanonicaliser(),
		key:      hashKey,
		retained: retained,
		config:   config,
		opts:     opts.withDefaults(),
	}
}

// NewRecorder builds a recorder sharing this runner's canonicaliser and hash key, so that every run
// in a comparison normalises identically.
//
// The caller owns the recorder because the proxy must be wired to the same one before Run starts.
func (r *Runner) NewRecorder() *effect.Recorder {
	return effect.NewRecorder(r.canon, r.key)
}

// Run replays the whole corpus under one mutation and returns what the service did.
//
// name must be unique per run: it names the replay consumer, and reusing one would resume from the
// previous run's position instead of replaying from the start.
func (r *Runner) Run(
	ctx context.Context,
	name string,
	mutation Mutation,
	handler Handler,
	recorder *effect.Recorder,
) (Result, error) {
	verdict, err := r.admit(mutation)
	if err != nil {
		return Result{}, err
	}

	stream, err := r.store.Replay(ctx, name, corpus.ConsumerOptions{
		BackOff:       r.config.BackOff,
		AckWait:       r.config.Deadline(1),
		MaxDeliver:    r.config.MaxDeliver,
		MaxAckPending: r.config.MaxAckPending,
	})
	if err != nil {
		return Result{}, fmt.Errorf("start replay %q: %w", name, err)
	}

	result := Result{Clause: verdict.Clause}

	current := &pass{
		runner:   r,
		stream:   stream,
		mutation: mutation,
		handler:  handler,
		recorder: recorder,
		result:   &result,
		name:     name,
	}

	if err := current.drive(ctx); err != nil {
		return Result{}, err
	}

	result.Effects = recorder.Effects()
	result.Late = recorder.LateCount()

	return result, nil
}

// admit refuses a fault the recorded configuration does not license, or one Stutter cannot yet
// inject honestly.
func (r *Runner) admit(mutation Mutation) (policy.Verdict, error) {
	if _, isConcurrent := mutation.(Concurrent); isConcurrent {
		return policy.Verdict{}, fmt.Errorf(
			"%w: concurrent delivery needs per-connection effect attribution, without which effects "+
				"are assigned to the wrong message", ErrUnsupported)
	}

	fault := mutation.Fault()

	verdict := r.config.Permits(fault)
	if !verdict.Permitted {
		return verdict, fmt.Errorf("%w: %s — %s", ErrForbidden, fault, verdict.Clause)
	}

	return verdict, nil
}

// retains reports whether a message is in scope for this run. An empty retain set means the whole
// corpus is.
func (r *Runner) retains(seq uint64) bool {
	if len(r.retained) == 0 {
		return true
	}

	_, kept := r.retained[seq]

	return kept
}

// pass is the state of one replay. It exists so the loop's steps can be separate methods without
// each carrying seven parameters.
type pass struct {
	runner   *Runner
	stream   *corpus.Replay
	mutation Mutation
	handler  Handler
	recorder *effect.Recorder
	result   *Result
	held     *corpus.Delivery
	name     string
}

// drive pulls the corpus, applying the mutation's dispatch-order and acknowledgement decisions.
func (p *pass) drive(ctx context.Context) error {
	for {
		delivery, err := p.stream.Next(ctx, p.runner.opts.FetchWait)
		if err != nil {
			done, drainErr := p.drained(ctx, err)
			if drainErr != nil {
				return drainErr
			}

			if done {
				return nil
			}

			continue
		}

		skipped, skipErr := p.skip(delivery)
		if skipErr != nil {
			return skipErr
		}

		if skipped || p.holdBack(delivery) {
			continue
		}

		if err := p.dispatch(ctx, delivery); err != nil {
			return err
		}
	}
}

// drained handles a fetch that returned nothing, reporting whether the run is over.
func (p *pass) drained(ctx context.Context, err error) (bool, error) {
	if !errors.Is(err, corpus.ErrDrained) {
		return false, fmt.Errorf("replay %q: %w", p.name, err)
	}

	if p.held == nil {
		return true, nil
	}

	// A deferred message with nothing left to follow it: dispatch it and drain again, since its own
	// redelivery may still be outstanding.
	held := p.held
	p.held = nil

	return false, p.settle(ctx, held)
}

// skip acknowledges a message outside the retained set without dispatching it, so it is never
// counted as delivered.
func (p *pass) skip(delivery *corpus.Delivery) (bool, error) {
	if p.runner.retains(delivery.Seq) {
		return false, nil
	}

	if err := delivery.Ack(); err != nil {
		return false, fmt.Errorf("replay %q, skipping message %d: %w", p.name, delivery.Seq, err)
	}

	return true, nil
}

// holdBack holds a message back so a later one overtakes it, reporting whether it did.
func (p *pass) holdBack(delivery *corpus.Delivery) bool {
	if p.held != nil || !p.mutation.Defer(message(delivery)) {
		return false
	}

	p.held = delivery

	return true
}

// dispatch settles a delivery, then any message held back behind it.
func (p *pass) dispatch(ctx context.Context, delivery *corpus.Delivery) error {
	if err := p.settle(ctx, delivery); err != nil {
		return err
	}

	if p.held == nil {
		return nil
	}

	held := p.held
	p.held = nil

	return p.settle(ctx, held)
}

// settle holds, dispatches and acknowledges one delivery.
func (p *pass) settle(ctx context.Context, delivery *corpus.Delivery) error {
	msg := message(delivery)

	// The hold happens before the window opens: time spent waiting is not the handler's work, and
	// the server may redeliver during it, which is the whole point of the fault.
	if hold := p.mutation.HoldFor(msg); hold > 0 {
		if err := holdMessage(ctx, hold); err != nil {
			return err
		}
	}

	if handlerErr := p.runner.deliver(ctx, p.name, p.recorder, msg, p.handler); handlerErr != nil {
		p.result.Failed++
	}

	p.result.Delivered++

	// Withholding is not an error path: it is the fault being injected.
	settle := delivery.Ack
	if p.mutation.WithholdAck(msg) {
		settle = delivery.Nak
	}

	if err := settle(); err != nil {
		return fmt.Errorf("replay %q, settling delivery %d: %w", p.name, msg.Seq, err)
	}

	return nil
}

// deliver opens the attribution window, runs the handler, and holds the window open through the
// quiesce period so that writes the handler did not wait for are still attributed to it.
func (r *Runner) deliver(
	ctx context.Context,
	consumer string,
	recorder *effect.Recorder,
	msg Message,
	handler Handler,
) error {
	recorder.Open(consumer, msg.Seq, msg.Payload)

	defer func() {
		pause(ctx, r.opts.Quiesce)
		recorder.Close()
	}()

	if err := handler(ctx, msg); err != nil {
		return fmt.Errorf("handler on message %d: %w", msg.Seq, err)
	}

	return nil
}

func message(delivery *corpus.Delivery) Message {
	return Message{
		Subject:    delivery.Subject,
		Payload:    delivery.Payload,
		Seq:        delivery.Seq,
		Deliveries: delivery.Deliveries,
	}
}

// hold_ waits for a fault's hold, reporting cancellation so a hung run does not look like a pass.
func holdMessage(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("replay cancelled while holding a message: %w", ctx.Err())
	}
}

// pause waits out the quiesce period. Cancellation simply ends the wait: the window closes either
// way, and there is nothing left to report it to.
func pause(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
