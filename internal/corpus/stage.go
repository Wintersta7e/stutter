package corpus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/policy"
)

// pauseHorizon is how long Pause holds a consumer. Far longer than any run, because nothing lifts the
// pause but the next restore, which returns the consumer to how the checkpoint holds it.
const pauseHorizon = 24 * time.Hour

// ErrFill means the corpus could not be staged exactly as numbered: a publish was refused, the bus
// took a message for a duplicate, or a message landed somewhere other than where it was numbered.
var ErrFill = errors.New("the corpus could not be staged as numbered")

// errTooManyPending means a consumer's pending count is past anything a corpus could stage.
var errTooManyPending = errors.New("pending count out of range")

// Message is one corpus message held outside the stream.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Message struct {
	// Header carries the message's headers, each name with its values in order; nil is none. A
	// header-keyed dedupe guard reads them, and a corpus published without them fails a correct
	// handler under duplicate delivery.
	Header map[string][]string
	// Subject and Payload are the message as it was recorded.
	Subject string
	Payload []byte
	// Seq is the sequence the message was recorded under — its rank, for a corpus read from a
	// directory. Staging lands it wherever the stream is up to, so this is the only name for a message
	// that survives a run.
	Seq uint64
}

// FillError is why Fill stopped, naming the corpus message it was staging.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type FillError struct {
	// Err is the cause of a refused publish; nil for a duplicate or a misplaced landing.
	Err error
	// Seq is the recorded sequence of the message being staged.
	Seq uint64
	// Other is the recorded sequence of the corpus message this one duplicated, or zero when the bus
	// matched it to a message that was already in the stream.
	Other uint64
	// Landed is the stream sequence the message landed at instead of the one it was numbered.
	Landed uint64
	// want is the stream sequence the message was numbered.
	want uint64
	// duplicate says the bus took the message for one it had already stored.
	duplicate bool
}

func (e *FillError) Error() string {
	prefix := fmt.Sprintf("stage message %d: ", e.Seq)

	switch {
	case e.Err != nil:
		return prefix + e.Err.Error()
	case e.duplicate && e.Other != 0:
		return fmt.Sprintf("%sthe bus took it for a duplicate of message %d", prefix, e.Other)
	case e.duplicate:
		return prefix + "the bus took it for a duplicate of a message already in the stream"
	default:
		return fmt.Sprintf("%slanded at %d, want %d: something else published into the stream",
			prefix, e.Landed, e.want)
	}
}

// Is makes every Fill failure match ErrFill, whatever its cause.
func (*FillError) Is(target error) bool {
	return target == ErrFill
}

// Unwrap exposes the cause of a refused publish.
func (e *FillError) Unwrap() error {
	return e.Err
}

// Staged is one message as it was staged, pairing the sequence it was recorded under with the one it
// now has.
//
// Rebuilding the stream renumbers it, so a caller holding recorded sequences needs the translation:
// without it a fault would be aimed at a message that is no longer there.
type Staged struct {
	// Recorded is the sequence the message was recorded under.
	Recorded uint64
	// Sequence is the sequence it has now.
	Sequence uint64
}

// Snapshot lifts the whole corpus out of the stream so it can be staged again afterwards.
//
// Holding the messages outside the stream is what makes scoping repeatable. Stage destroys
// everything it does not republish, and a check interleaves whole-corpus runs with the subsets a
// shrink asks for — so a caller that re-read the stream between runs would find the corpus already
// reduced to the last subset it staged. It also means a staging that fails part way cannot cost the
// corpus anything: the messages are already in the caller's hands.
func (c *Corpus) Snapshot(ctx context.Context) ([]Message, error) {
	stream, err := c.stream.Stream(ctx, c.topic.Stream)
	if err != nil {
		return nil, fmt.Errorf("open the corpus stream: %w", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the corpus stream state: %w", err)
	}

	if info.State.Msgs == 0 {
		return nil, nil
	}

	messages := make([]Message, 0, info.State.Msgs)

	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq; sequence++ {
		raw, err := stream.GetMsg(ctx, sequence)
		if err != nil {
			return nil, fmt.Errorf("read corpus message %d: %w", sequence, err)
		}

		messages = append(messages, Message{
			Subject: raw.Subject,
			Payload: raw.Data,
			Header:  headers(raw.Header),
			Seq:     sequence,
		})
	}

	return messages, nil
}

// Clear rebuilds the corpus stream empty, ready for Fill.
//
// It is not a reset between runs: it leaves every bucket, stream and consumer the service made, and a
// seeded key at the service's revision. The bus reset is a checkpoint's restore. Clear survives for a
// Go caller that published its corpus into the stream: the corpus is taken out once, the stream
// cleared, and a checkpoint taken of the empty stream — so it runs at most once, before the first
// checkpoint, and is refused after one.
//
// The stream is deleted and recreated rather than purged, for the same reason a key/value guard is:
// a purged stream carries state a virgin one does not, and a run that starts from an equivalent
// rather than an identical position is not comparable with the one before it.
func (c *Corpus) Clear(ctx context.Context) error {
	if c.checkpointed {
		return errClearAfterCheckpoint
	}

	if err := c.stream.DeleteStream(ctx, c.topic.Stream); err != nil {
		return fmt.Errorf("delete the corpus stream: %w", err)
	}

	if _, err := c.stream.CreateStream(ctx, streamConfig(c.topic.Stream, c.topic.Subjects)); err != nil {
		return fmt.Errorf("recreate the corpus stream: %w", err)
	}

	return nil
}

// Next is the stream sequence the next published message will land at: one past the bound stream's
// last. A stream a job or the service created may already hold messages, so staging starts wherever
// the stream is up to rather than at one.
func (c *Corpus) Next(ctx context.Context) (uint64, error) {
	stream, err := c.stream.Stream(ctx, c.topic.Stream)
	if err != nil {
		return 0, fmt.Errorf("open the corpus stream: %w", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("read the corpus stream state: %w", err)
	}

	return info.State.LastSeq + 1, nil
}

// Numbering is the sequence each message will have once Fill has published it from first on.
//
// A service consuming for itself is handed a message the moment it lands, before Fill has heard back
// where it landed, so the translation has to be known before publishing rather than read back after.
// Numbering starts at the stream's next sequence (Next), and Fill checks that it did.
func Numbering(messages []Message, first uint64) []Staged {
	staged := make([]Staged, len(messages))
	for at, message := range messages {
		staged[at] = Staged{Recorded: message.Seq, Sequence: first + uint64(at)}
	}

	return staged
}

// Fill publishes messages into the bound stream in order, headers included, the first at first.
//
// Each publish waits for the bus's acknowledgement. A message the bus took for a duplicate, or one
// landing anywhere but where Numbering said, is an error rather than a detail: the corpus would be
// missing a message, or something else published into the stream, and every fault aimed by sequence
// would hit the wrong message.
func (c *Corpus) Fill(ctx context.Context, messages []Message, first uint64) error {
	for at, message := range messages {
		msg := nats.NewMsg(message.Subject)
		msg.Data = message.Payload

		for name, values := range message.Header {
			msg.Header[name] = slices.Clone(values)
		}

		ack, err := c.stream.PublishMsg(ctx, msg)
		if err != nil {
			return &FillError{Err: fmt.Errorf("publish: %w", err), Seq: message.Seq}
		}

		want := first + uint64(at)

		switch {
		case ack.Duplicate:
			return &FillError{Seq: message.Seq, Other: filled(messages[:at], first, ack.Sequence), duplicate: true}
		case ack.Sequence != want:
			return &FillError{Seq: message.Seq, Landed: ack.Sequence, want: want}
		}
	}

	return nil
}

// filled names the corpus message already published at a stream sequence, or zero when none of
// them was.
func filled(published []Message, first, sequence uint64) uint64 {
	if sequence < first || sequence-first >= uint64(len(published)) {
		return 0
	}

	return published[sequence-first].Seq
}

// Pending is how many messages the consumer has still to be handed or to settle, and the filter
// subjects the server holds for it, sorted.
//
// Both are read from the server: a caller's idea of the consumer's filter need not match the one the
// service actually created, and counting against the wrong one would call a correct run short.
func (c *Corpus) Pending(ctx context.Context, consumer string) (int, []string, error) {
	stream, err := c.stream.Stream(ctx, c.topic.Stream)
	if err != nil {
		return 0, nil, fmt.Errorf("open the corpus stream: %w", err)
	}

	info, err := lookup(ctx, stream, consumer)
	if err != nil {
		return 0, nil, fmt.Errorf("find consumer %q: %w", consumer, err)
	}

	if info.NumPending > math.MaxInt32 {
		return 0, nil, fmt.Errorf("%w: consumer %q has %d messages pending",
			errTooManyPending, consumer, info.NumPending)
	}

	return int(info.NumPending) + info.NumAckPending, filterSubjects(info.Config), nil
}

// Pause stops the bus delivering to one consumer on the corpus stream for the rest of the run.
//
// It is how a run is scoped to one consumer of a service that runs several in one process, whose
// effects would otherwise interleave with no way to tell them apart. It is not a fault: a paused pull
// consumer looks idle to its client. Measured on the pinned server — no delivery and no client-side
// error, whether the client fetches or consumes with heartbeats — and the service re-creating the
// consumer with its own configuration does not lift the pause. The next Clear deletes it outright.
func (c *Corpus) Pause(ctx context.Context, consumer string) error {
	if _, err := c.stream.PauseConsumer(ctx, c.topic.Stream, consumer, time.Now().Add(pauseHorizon)); err != nil {
		return fmt.Errorf("pause consumer %q: %w", consumer, err)
	}

	return nil
}

// Serialise limits a consumer on the corpus stream to one unacknowledged message at a time and to at
// most deliveryCap deliveries of each message, and returns the consumer as it was read BEFORE the
// rewrite.
//
// Attribution needs one message in flight, and a service that pulls for itself picks its own batch.
// With several delivered at once, the order the proxy sees them settle in is not the order the service
// worked in: measured on a real target, the acknowledgement of one message — a buffered publish on the
// bus connection — reached the proxy after the next message's write on the database connection, so
// every effect was attributed one message early. With one in flight the server cannot deliver the next
// message until the previous acknowledgement has reached it, through the proxy.
//
// The cap is what ends a run whose service refuses a message forever: measured on the pinned server, a
// message refused under an unlimited MaxDeliver was delivered tens of thousands of times a second. The
// backoff curve keeps its first entries, as many as the cap allows — the server refuses a curve longer
// than the delivery limit — so every attempt the cap keeps lands exactly when it would have. A rewrite
// only ever removes or delays a delivery, never adds or hastens one.
//
// It changes the run, not the verdict's licence: legality reads the returned, pre-rewrite
// configuration.
func (c *Corpus) Serialise(ctx context.Context, consumer string, deliveryCap int) (policy.Config, error) {
	info, err := c.consumerInfo(ctx, consumer)
	if err != nil {
		return policy.Config{}, err
	}

	read, err := mapConfig(info.Config)
	if err != nil {
		return policy.Config{}, fmt.Errorf("consumer %q: %w", consumer, err)
	}

	capped := read.EffectiveCap(deliveryCap)

	// Every other field is written back as read, a push consumer's deliver subject included.
	config := info.Config
	config.MaxAckPending = 1
	config.MaxDeliver = capped
	config.BackOff = config.BackOff[:min(len(config.BackOff), capped)]

	if _, err := c.stream.UpdateConsumer(ctx, c.topic.Stream, config); err != nil {
		return policy.Config{}, fmt.Errorf("serialise consumer %q: %w", consumer, err)
	}

	return read, nil
}

// lookup reads a consumer's configuration whatever its kind. The client hands out pull and push
// consumers through separate calls, each refusing the other kind, and a service under test may use
// either.
func lookup(ctx context.Context, stream jetstream.Stream, consumer string) (*jetstream.ConsumerInfo, error) {
	pull, err := stream.Consumer(ctx, consumer)
	if err == nil {
		return pull.CachedInfo(), nil
	}

	if !errors.Is(err, jetstream.ErrNotPullConsumer) {
		return nil, fmt.Errorf("look up a pull consumer: %w", err)
	}

	push, err := stream.PushConsumer(ctx, consumer)
	if err != nil {
		return nil, fmt.Errorf("look up a push consumer: %w", err)
	}

	return push.CachedInfo(), nil
}

// headers copies a stored message's headers out of the NATS type, nil when there are none.
func headers(stored nats.Header) map[string][]string {
	if len(stored) == 0 {
		return nil
	}

	copied := make(map[string][]string, len(stored))
	for name, values := range stored {
		copied[name] = slices.Clone(values)
	}

	return copied
}

// streamConfig is the shape of a stream Stutter owns, in one place so a staged stream is identical to
// the one Start created.
func streamConfig(name string, subjects []string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}
}
