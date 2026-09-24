package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// The Fill hold's bound, B = min(Deadline(1), E, holdCeiling) / holdDivisor.
//
// Every harm measured from holding deliveries appears at one times its timer or more: a hold past the
// first-attempt deadline had the bus redeliver a clean-run message, one past a pull's life lost a
// delivery. A tenth of the shortest timer is "well below" all of them.
const (
	// holdDivisor is how far below the shortest timer the hold must stay.
	holdDivisor = 10
	// holdCeiling is nats.go's default JetStream API timeout: the shortest default client timer that
	// crosses a hold and never appears on the wire.
	holdCeiling = 5 * time.Second
	// heartbeatsMissed is how many idle heartbeats a pull's client counts before it gives up on the
	// pull: twice the heartbeat is the timer a hold must stay under.
	heartbeatsMissed = 2
)

// The term that set a hold's bound.
const (
	termDeadline  = "deadline"
	termExpires   = "expires"
	termHeartbeat = "heartbeat"
	termCeiling   = "ceiling"
)

// ErrHoldExceeded means the corpus took longer to publish than deliveries may be held: the run was
// stopped rather than risk the consumer's own timers redelivering or losing a message.
var ErrHoldExceeded = errors.New("the Fill hold outlasted its bound")

// ErrPending means the consumer under test will not be handed exactly the run's messages its filter
// admits: some were discarded or skipped before it could be, or others already in the stream would
// reach it too.
var ErrPending = errors.New("the consumer under test will not receive exactly the corpus")

// FillHold is one run's hold while its corpus was published: how long it lasted, the bound it had to
// stay under and the timer that set that bound, and what was published meanwhile.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type FillHold struct {
	// Term names the timer that set Bound: deadline, expires, heartbeat or ceiling.
	Term string
	// Length is how long the hold lasted, on the monotonic clock.
	Length time.Duration
	// Bound is the longest it was allowed.
	Bound time.Duration
	// Messages and Bytes are what was published under it, headers included.
	Messages int
	Bytes    int
}

// Holds is every Fill hold this sandbox's observed runs made, in run order.
func (s *Sandbox) Holds() []FillHold {
	return append([]FillHold(nil), s.holds...)
}

// fill publishes the run's messages into the stream while every delivery to the service waits.
//
// A handler that publishes into its own stream would otherwise land inside the corpus's numbering. The
// hold is timed from the moment it opens; past its bound the run stops at once, without waiting for
// the rest of the corpus to be published. Every hold is recorded, stopped or not.
func (s *Sandbox) fill(ctx context.Context, run *observedRun, target string, messages []corpus.Message) error {
	held, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	run.hold.begin(target, s.cfg.Policy.Deadline(1), cancel)

	err := s.publish(held, run, target, messages)

	record := run.hold.end()
	record.Messages = len(messages)

	for _, message := range messages {
		record.Bytes += message.Size()
	}

	s.holds = append(s.holds, record)

	if errors.Is(context.Cause(held), ErrHoldExceeded) {
		return fmt.Errorf("%w: deliveries were held %s while %d messages (%d bytes) were published, "+
			"past the %s bound set by the %s", ErrHoldExceeded, record.Length, record.Messages, record.Bytes,
			record.Bound, record.Term)
	}

	return err
}

// publish numbers the run's messages from wherever the stream is up to, publishes them, and checks the
// consumer under test will be handed them.
//
// The translation is installed before the first publish: the proxy notes a delivery as it reads it,
// and a service consuming for itself is handed a message before Fill hears back where it landed.
func (s *Sandbox) publish(ctx context.Context, run *observedRun, target string, messages []corpus.Message) error {
	first, err := s.cfg.Corpus.Next(ctx)
	if err != nil {
		return fmt.Errorf("number the corpus: %w", err)
	}

	run.stage(messages, first)

	if err := s.cfg.Corpus.Fill(ctx, messages, first); err != nil {
		return fmt.Errorf("stage the corpus: %w", err)
	}

	// A consumer created after publication was never held to one message in flight, and scope already
	// refuses whatever it takes; there is nothing to count against yet.
	if target == "" {
		return nil
	}

	return s.pending(ctx, target, messages)
}

// pending checks the consumer under test has exactly the run's messages its filter admits still to be
// handed or settled, read from the server on Stutter's own connection.
//
// A stream limit or age can discard a staged message, and a delivery policy can skip one, before the
// service is ever handed it; a run over part of the corpus reads as a handler that did less. Messages
// already in the stream would reach it beside the corpus. Either way the run stops, saying which.
func (s *Sandbox) pending(ctx context.Context, target string, messages []corpus.Message) error {
	count, filters, err := s.cfg.Corpus.Pending(ctx, target)
	if err != nil {
		return fmt.Errorf("read what consumer %q has pending: %w", target, err)
	}

	want := len(corpus.Admitted(messages, filters))

	switch {
	case count < want:
		return fmt.Errorf("%w: consumer %q has %d of the %d staged messages its filter admits still to "+
			"receive, a shortfall of %d — the stream's limits or age discarded some, or its delivery "+
			"policy skips them", ErrPending, target, count, want, want-count)
	case count > want:
		return fmt.Errorf("%w: consumer %q has %d messages still to receive, an excess of %d over the %d "+
			"staged ones its filter admits — the stream already held messages it will also receive",
			ErrPending, target, count, count-want, want)
	default:
		return nil
	}
}

// pullLimit is the shortest a consumer's pull requests let a hold last, and the timer that set it:
// the least of each request's expiry and twice its idle heartbeat. Zero is no limit yet.
type pullLimit struct {
	term  string
	limit time.Duration
}

// lower takes one pull request into account, reporting whether it shortened the limit. A request that
// asks only for what is there now sets no timer to outlast.
func (p *pullLimit) lower(pull natsproxy.Pull) bool {
	if pull.NoWait {
		return false
	}

	lowered := false

	if pull.Expires > 0 && (p.limit == 0 || pull.Expires < p.limit) {
		p.limit, p.term, lowered = pull.Expires, termExpires, true
	}

	if beat := heartbeatsMissed * pull.Heartbeat; pull.Heartbeat > 0 && (p.limit == 0 || beat < p.limit) {
		p.limit, p.term, lowered = beat, termHeartbeat, true
	}

	return lowered
}

// holdBound is B and the term that set it: the least of the first-attempt deadline, the pulls' limit
// and the ceiling, each counted only when set, divided by holdDivisor.
func holdBound(deadline time.Duration, pulls pullLimit) (time.Duration, string) {
	limit, term := holdCeiling, termCeiling

	if deadline > 0 && deadline < limit {
		limit, term = deadline, termDeadline
	}

	if pulls.limit > 0 && pulls.limit < limit {
		limit, term = pulls.limit, pulls.term
	}

	return limit / holdDivisor, term
}

// fillHold is one run's hold: the gate the proxy waits at, and the clock that stops the run if the
// gate stays shut past its bound.
//
// The bound is set when the hold opens and lowered, never raised, by a pull request the consumer under
// test makes while it is open: a client that shortens its own timers mid-hold shortens the hold too.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type fillHold struct {
	opened time.Time
	// pulls is the pull limit of every consumer on the run's stream, by name: the target is not known
	// until the hold opens, and its pulls before then count.
	pulls  map[string]*pullLimit
	timer  *time.Timer
	cancel context.CancelCauseFunc
	target string
	term   string
	gate   natsproxy.Hold
	// deadline is the consumer's first-attempt deadline.
	deadline time.Duration
	bound    time.Duration
	open     bool
	mu       sync.Mutex
}

// pulled takes a pull request into account, and re-arms the clock when it lowers an open hold's bound.
func (h *fillHold) pulled(pull natsproxy.Pull) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.pulls == nil {
		h.pulls = make(map[string]*pullLimit)
	}

	limit, known := h.pulls[pull.Consumer]
	if !known {
		limit = &pullLimit{}
		h.pulls[pull.Consumer] = limit
	}

	if !limit.lower(pull) || !h.open || pull.Consumer != h.target {
		return
	}

	if bound, term := holdBound(h.deadline, *limit); bound < h.bound {
		h.bound, h.term = bound, term
		h.arm()
	}
}

// begin closes the gate and starts the clock for the consumer under test.
func (h *fillHold) begin(target string, deadline time.Duration, cancel context.CancelCauseFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.gate.Begin()

	h.target, h.deadline, h.cancel = target, deadline, cancel
	h.opened, h.open = time.Now(), true

	var limit pullLimit
	if known, seen := h.pulls[target]; seen {
		limit = *known
	}

	h.bound, h.term = holdBound(deadline, limit)
	h.arm()
}

// end stops the clock, opens the gate and reports the hold.
func (h *fillHold) end() FillHold {
	h.mu.Lock()
	defer h.mu.Unlock()

	length := time.Since(h.opened)

	if h.timer != nil {
		h.timer.Stop()
	}

	h.open = false
	h.gate.Release()

	return FillHold{Term: h.term, Length: length, Bound: h.bound}
}

// arm sets the clock to stop the run when the hold reaches its bound, at once if it already has. The
// caller holds the lock.
func (h *fillHold) arm() {
	if h.timer != nil {
		h.timer.Stop()
	}

	remaining := h.bound - time.Since(h.opened)
	if remaining <= 0 {
		h.cancel(ErrHoldExceeded)

		return
	}

	cancel := h.cancel
	h.timer = time.AfterFunc(remaining, func() { cancel(ErrHoldExceeded) })
}
