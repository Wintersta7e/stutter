package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/policy"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// serviceRetries is how many deliveries of a message a run allows beyond the crash loop's: one. A
// handler that loses an optimistic-concurrency race on the redelivery, NAKs, re-reads and writes again
// is the read-modify-write bug class, and a cap one lower hides it.
const serviceRetries = 1

// DeliveryCap is the most deliveries of one message any run allows: the first, the ones the crash loop
// withholds, and one retry of the service's own. It is the same for every run of a check, because a
// bound that differed between the runs it compares would itself manufacture a divergence.
const DeliveryCap = 1 + policy.CrashLoopWithheld + serviceRetries

// interruption is what ended a wait: its own condition, or something that made the condition moot.
type interruption uint8

const (
	// finished means the wait's own condition held.
	finished interruption = iota
	// targetExited means the service under test stopped by itself, so there was nothing left to wait
	// for.
	targetExited
	// proxyStopped means a proxy's Serve returned: the service's traffic through it is no longer
	// observed, so nothing the wait could see would be complete.
	proxyStopped
)

// errProxyStopped wraps a proxy's error that ended a wait before its teardown.
var errProxyStopped = errors.New("a proxy stopped during the run")

// ErrDrainBelowFloor means Config.Drain would have runs stop waiting for owed messages before the
// longest redelivery their configuration allows could land.
var ErrDrainBelowFloor = errors.New("the drain is shorter than the consumer's redeliveries need")

// DrainFloor is the shortest owed-silence limit a run on config may be given: twice the longest
// deadline any delivery the cap allows can wait for, plus the quiesce — zero or less being the default
// one. A run that stopped waiting sooner would call a message owed whose redelivery was still coming,
// so a drain below it is refused, by New and by a caller validating a drain for every consumer it will
// check.
func DrainFloor(config policy.Config, quiesce time.Duration) time.Duration {
	if quiesce <= 0 {
		quiesce = replay.DefaultQuiesce
	}

	return floor(config, quiesce)
}

// await is the one wait every start of the service under test goes through — a run's startup and its
// end alike — so each ends by the same rules.
//
// It checks until every drainPoll, first at once, and returns finished when until reports true. An
// error from until ends the wait with that error. It returns targetExited the moment exited closes,
// because a service that has stopped will never satisfy anything a start is waiting for, and waiting
// the condition out would only report the exit late as something else. It returns proxyStopped, with
// the proxy's error, the moment any proxy's Serve returns, for the same reason: from then on the
// service's traffic through it goes unobserved.
func (e *egress) await(
	ctx context.Context,
	exited <-chan struct{},
	until func(context.Context) (bool, error),
) (interruption, error) {
	ticker := time.NewTicker(drainPoll)
	defer ticker.Stop()

	for {
		select {
		case <-exited:
			return targetExited, nil
		case err := <-e.served:
			return proxyStopped, e.stopped(err)
		default:
		}

		done, err := until(ctx)
		if err != nil {
			return finished, err
		}

		if done {
			return finished, nil
		}

		select {
		case <-ctx.Done():
			return finished, fmt.Errorf("cancelled while waiting: %w", ctx.Err())
		case <-exited:
			return targetExited, nil
		case err := <-e.served:
			return proxyStopped, e.stopped(err)
		case <-ticker.C:
		}
	}
}

// stopped takes one proxy's Serve result before teardown, as the error that ended the wait. It is
// counted, so teardown drains only the results still to come.
func (e *egress) stopped(err error) error {
	e.taken++

	if err == nil {
		return errProxyStopped
	}

	return fmt.Errorf("%w: %w", errProxyStopped, err)
}

// answer is the latest word the server has had on one message's latest delivery.
type answer uint8

const (
	// awaiting means the delivery has had no answer the server heard: none yet, or one the fault
	// withheld. The message is still owed.
	awaiting answer = iota
	// settledAnswer means the server settled the message.
	settledAnswer
	// refused means the service asked for the message again.
	refused
)

// attempt is what one admitted message has been through in the run.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type attempt struct {
	// since is when its latest delivery, or the latest request for more time on it, happened: an
	// offset from the run's start.
	since    time.Duration
	attempts uint64
	answer   answer
}

// settlement is a run's ledger: for every message the consumer under test admits, how many times it
// was delivered and what the server last heard about it.
//
// A message is done when the server settled it, or when it reached the delivery cap and its last
// delivery was refused or has outlived the deadline governing it — the server will not deliver it
// again. A run whose messages are all done has nothing more coming.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type settlement struct {
	entries map[uint64]*attempt
	// final is the deadline governing the last delivery the cap allows.
	final time.Duration
	// longestNak is the longest redelivery delay any NAK asked for.
	longestNak time.Duration
	// capped is the most deliveries of a message the run allows.
	capped uint64
	// beyond says the consumer itself would deliver a message more times than the run allows.
	beyond bool
	mu     sync.Mutex
}

// newSettlement opens the ledger for the messages the consumer under test admits, under the
// configuration its deliveries follow.
func newSettlement(config policy.Config, admitted []uint64) *settlement {
	capped := config.EffectiveCap(DeliveryCap)

	ledger := &settlement{
		entries: make(map[uint64]*attempt, len(admitted)),
		final:   config.Deadline(capped),
		capped:  uint64(max(capped, 0)),
		beyond:  config.MaxDeliver <= 0 || config.MaxDeliver > capped,
	}

	ledger.admit(admitted)

	return ledger
}

// admit adds messages to the ledger, each owed until it is done.
func (l *settlement) admit(seqs []uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seq := range seqs {
		l.entries[seq] = &attempt{}
	}
}

// delivered records a delivery of a message, its attempt count as the server gave it. A message the
// ledger does not hold — fed back, or not admitted — is none of its business.
func (l *settlement) delivered(seq, attempts uint64, at time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, held := l.entries[seq]
	if !held {
		return
	}

	entry.attempts = max(entry.attempts, attempts)
	entry.answer = awaiting
	entry.since = at
}

// acknowledged records the service's answer to a delivery, classified as the server classifies it. One
// the fault withheld never reaches the server, so whatever it said the message stays owed.
func (l *settlement) acknowledged(seq uint64, ack natsproxy.Ack, withheld bool, at time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, held := l.entries[seq]
	if !held {
		return
	}

	entry.attempts = max(entry.attempts, ack.Deliveries)

	switch {
	case withheld:
		entry.answer = awaiting
	case ack.InProgress():
		entry.since = at
	case ack.Settles():
		entry.answer = settledAnswer
	case ack.Negative():
		entry.answer = refused
		l.longestNak = max(l.longestNak, ack.NakDelay())
	default:
		// Anything else the server ignores, and so does the ledger.
	}
}

// settled reports whether every admitted message is done.
func (l *settlement) settled(now time.Duration) bool {
	return l.owed(now) == 0
}

// owed counts the admitted messages that are not done.
func (l *settlement) owed(now time.Duration) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	owed := 0

	for _, entry := range l.entries {
		if !l.done(entry, now) {
			owed++
		}
	}

	return owed
}

// exhausted counts the messages done at the cap without the server settling them, where the consumer
// itself would have delivered them again.
func (l *settlement) exhausted(now time.Duration) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.beyond {
		return 0
	}

	exhausted := 0

	for _, entry := range l.entries {
		if entry.answer != settledAnswer && l.done(entry, now) {
			exhausted++
		}
	}

	return exhausted
}

// limit is how long the run waits in silence for messages still owed: the static part, or longer when
// a NAK asked for a later redelivery than the static part allows for.
func (l *settlement) limit(static, quiesce time.Duration) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	return max(static, drainMargin*l.longestNak+quiesce)
}

// done reports whether the server has finished with a message. The caller holds the lock.
func (l *settlement) done(entry *attempt, now time.Duration) bool {
	if entry.answer == settledAnswer {
		return true
	}

	if entry.attempts < l.capped {
		return false
	}

	return entry.answer == refused || now-entry.since >= l.final
}

// floor is the owed-silence limit's static part when nothing lengthens it: long enough for the
// longest redelivery any delivery the cap allows can wait for, with the margin a deadline needs.
func floor(config policy.Config, quiesce time.Duration) time.Duration {
	longest := time.Duration(0)

	for delivery := 1; delivery <= config.EffectiveCap(DeliveryCap); delivery++ {
		longest = max(longest, config.Deadline(delivery))
	}

	return drainMargin*longest + quiesce
}
