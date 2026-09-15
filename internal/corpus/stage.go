package corpus

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

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
	stream, err := c.stream.Stream(ctx, StreamName)
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

// Stage rebuilds the corpus stream so it holds exactly these messages, in this order.
//
// It is how a run is scoped when the service under test chooses its own messages: it cannot be told
// to skip one, and holding a delivery back at the proxy breaks the consumer's own batch accounting —
// a pull consumer counts a swallowed message against the batch it asked for, so its next fetch comes
// back short. Rebuilding also takes the previous run's consumers with it, which is what stops a
// second run resuming where the first one stopped.
//
// The stream is deleted and recreated rather than purged, for the same reason a key/value guard is:
// a purged stream carries state a virgin one does not, and a run that starts from an equivalent
// rather than an identical position is not comparable with the one before it.
func (c *Corpus) Stage(ctx context.Context, messages []Message) ([]Staged, error) {
	if err := c.stream.DeleteStream(ctx, StreamName); err != nil {
		return nil, fmt.Errorf("delete the corpus stream: %w", err)
	}

	if _, err := c.stream.CreateStream(ctx, streamConfig()); err != nil {
		return nil, fmt.Errorf("recreate the corpus stream: %w", err)
	}

	staged := make([]Staged, 0, len(messages))

	for _, message := range messages {
		sequence, err := c.Publish(ctx, message.Subject, message.Payload)
		if err != nil {
			return nil, fmt.Errorf("stage message %d: %w", message.Seq, err)
		}

		staged = append(staged, Staged{Recorded: message.Seq, Sequence: sequence})
	}

	return staged, nil
}

// streamConfig is the corpus stream's shape, in one place so a staged stream is identical to the one
// Start created.
func streamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      StreamName,
		Subjects:  []string{subjectFilter},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}
}
