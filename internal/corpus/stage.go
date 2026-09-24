package corpus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// pauseHorizon is how long Pause holds a consumer. Far longer than any run, because nothing lifts the
// pause: Clear deletes the consumer with the stream before the next run starts.
const pauseHorizon = 24 * time.Hour

// errForeignMessages means the corpus stream held a message Fill did not publish.
var errForeignMessages = errors.New("something other than Stutter published into the corpus stream")

// Message is one corpus message held outside the stream.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Message struct {
	// Subject and Payload are the message as it was recorded. Headers are not carried: nothing
	// downstream reads them, and a header the bus adds on ingestion is not the recording's.
	Subject string
	Payload []byte
	// Seq is the sequence the message was recorded under. Staging renumbers the stream from one, so
	// this is the only name for a message that survives a rebuild.
	Seq uint64
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

		messages = append(messages, Message{Subject: raw.Subject, Payload: raw.Data, Seq: sequence})
	}

	return messages, nil
}

// Clear rebuilds the corpus stream empty, ready for Fill.
//
// Clearing and filling are how a run is scoped when the service under test chooses its own messages:
// it cannot be told to skip one, and holding a delivery back at the proxy breaks the consumer's own
// batch accounting — a pull consumer counts a swallowed message against the batch it asked for, so
// its next fetch comes back short. Rebuilding also takes the previous run's consumers with it, which
// is what stops a second run resuming where the first one stopped.
//
// They are two steps rather than one so the service can be started in between, against a stream that
// holds nothing yet: whatever it does on startup then happens before there is a message to attribute
// it to.
//
// The stream is deleted and recreated rather than purged, for the same reason a key/value guard is:
// a purged stream carries state a virgin one does not, and a run that starts from an equivalent
// rather than an identical position is not comparable with the one before it.
func (c *Corpus) Clear(ctx context.Context) error {
	if err := c.stream.DeleteStream(ctx, c.topic.Stream); err != nil {
		return fmt.Errorf("delete the corpus stream: %w", err)
	}

	if _, err := c.stream.CreateStream(ctx, streamConfig(c.topic)); err != nil {
		return fmt.Errorf("recreate the corpus stream: %w", err)
	}

	return nil
}

// Numbering is the sequence each message will have once Fill has published it into a cleared stream.
//
// A service consuming for itself is handed a message the moment it lands, before Fill has heard back
// where it landed, so the translation has to be known before publishing rather than read back after.
// A virgin stream numbers from one, and Fill checks that it did.
func Numbering(messages []Message) []Staged {
	staged := make([]Staged, len(messages))
	for at, message := range messages {
		staged[at] = Staged{Recorded: message.Seq, Sequence: uint64(at) + 1}
	}

	return staged
}

// Fill publishes messages into a cleared corpus stream, in order.
//
// A message landing anywhere but where Numbering said is an error rather than a detail: something
// else published into the stream, and every fault aimed by sequence would hit the wrong message.
func (c *Corpus) Fill(ctx context.Context, messages []Message) error {
	for at, message := range messages {
		sequence, err := c.Publish(ctx, message.Subject, message.Payload)
		if err != nil {
			return fmt.Errorf("stage message %d: %w", message.Seq, err)
		}

		if want := uint64(at) + 1; sequence != want {
			return fmt.Errorf("%w: message %d landed at %d, want %d", errForeignMessages, message.Seq, sequence, want)
		}
	}

	return nil
}

// Consumers names every consumer on the corpus stream, in name order.
func (c *Corpus) Consumers(ctx context.Context) ([]string, error) {
	stream, err := c.stream.Stream(ctx, c.topic.Stream)
	if err != nil {
		return nil, fmt.Errorf("open the corpus stream: %w", err)
	}

	lister := stream.ConsumerNames(ctx)

	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}

	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("list the corpus stream's consumers: %w", err)
	}

	slices.Sort(names)

	return names, nil
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

// Serialise limits a consumer on the corpus stream to one unacknowledged message at a time.
//
// Attribution needs one message in flight, and a service that pulls for itself picks its own batch.
// With several delivered at once, the order the proxy sees them settle in is not the order the service
// worked in: measured on a real target, the acknowledgement of one message — a buffered publish on the
// bus connection — reached the proxy after the next message's write on the database connection, so
// every effect was attributed one message early. With one in flight the server cannot deliver the next
// message until the previous acknowledgement has reached it, through the proxy.
//
// It changes the run, not the verdict's licence: which faults are legal is still read off the
// recorded configuration.
func (c *Corpus) Serialise(ctx context.Context, consumer string) error {
	stream, err := c.stream.Stream(ctx, c.topic.Stream)
	if err != nil {
		return fmt.Errorf("open the corpus stream: %w", err)
	}

	info, err := lookup(ctx, stream, consumer)
	if err != nil {
		return fmt.Errorf("find consumer %q: %w", consumer, err)
	}

	// Every other field is written back as read, a push consumer's deliver subject included.
	config := info.Config
	config.MaxAckPending = 1

	if _, err := c.stream.UpdateConsumer(ctx, c.topic.Stream, config); err != nil {
		return fmt.Errorf("serialise consumer %q: %w", consumer, err)
	}

	return nil
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

// streamConfig is the corpus stream's shape, in one place so a staged stream is identical to the one
// Start created.
func streamConfig(topic Topic) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      topic.Stream,
		Subjects:  []string{topic.Filter},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}
}
