package corpus_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

// firstOrder is the message every test here keeps, named once so the assertions and the fixture
// cannot drift apart.
const firstOrder = "ORD-1"

// TestStageKeepsOnlyTheChosenMessages is how a run is scoped to part of the corpus when the service
// under test picks its own messages. Shrinking a failure to a minimal repro is exactly that.
func TestStageKeepsOnlyTheChosenMessages(t *testing.T) {
	t.Parallel()

	store := stocked(t, firstOrder, "ORD-2", "ORD-3")

	snapshot, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	keep := []corpus.Message{snapshot[0], snapshot[2]}

	staged := stage(t, store, keep)

	if len(staged) != len(keep) {
		t.Fatalf("staged %d messages, want %d", len(staged), len(keep))
	}

	for at, message := range staged {
		if message.Recorded != keep[at].Seq {
			t.Errorf("staged[%d].Recorded = %d, want %d", at, message.Recorded, keep[at].Seq)
		}
	}

	// Renumbered from one, which is why the translation is returned at all: a fault aimed at the
	// recorded sequence would otherwise land on a different message or on none.
	if staged[0].Sequence != 1 || staged[1].Sequence != 2 {
		t.Errorf("sequences = %d, %d, want 1, 2", staged[0].Sequence, staged[1].Sequence)
	}

	delivered := drainSubjects(t, store)
	if len(delivered) != len(keep) {
		t.Fatalf("the staged corpus delivered %q, want the two kept messages", delivered)
	}

	if delivered[0] != firstOrder || delivered[1] != "ORD-3" {
		t.Errorf("delivered %q, want ORD-1 then ORD-3 — the dropped message is still there", delivered)
	}
}

// TestSnapshotStagesTheWholeCorpusAgainAfterASubset is the property a check depends on: it
// interleaves whole-corpus runs with the subsets a shrink asks for, and staging destroys everything
// it does not republish. A caller that re-read the stream between runs would find the corpus already
// reduced to the last subset it staged.
func TestSnapshotStagesTheWholeCorpusAgainAfterASubset(t *testing.T) {
	t.Parallel()

	store := stocked(t, firstOrder, "ORD-2", "ORD-3")

	snapshot, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	stage(t, store, []corpus.Message{snapshot[1]})

	if delivered := drainSubjects(t, store); len(delivered) != 1 {
		t.Fatalf("the subset corpus delivered %q, want ORD-2 alone", delivered)
	}

	stage(t, store, snapshot)

	delivered := drainSubjects(t, store)
	if len(delivered) != len(snapshot) {
		t.Fatalf("the restored corpus delivered %q, want all three messages back", delivered)
	}

	if delivered[0] != firstOrder || delivered[2] != "ORD-3" {
		t.Errorf("delivered %q, want the recorded order back", delivered)
	}
}

// TestFillRefusesAStreamSomethingElsePublishedInto: the translation is computed before publishing,
// so a message that lands elsewhere would have every fault aimed by sequence hit the wrong message.
func TestFillRefusesAStreamSomethingElsePublishedInto(t *testing.T) {
	t.Parallel()

	store := stocked(t, firstOrder)

	snapshot, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if err := store.Clear(t.Context()); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}

	// The service under test publishing into the stream it consumes, between Clear and Fill.
	if _, err := store.Publish(t.Context(), corpus.SubjectPrefix+"orders", []byte("FOREIGN")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if err := store.Fill(t.Context(), snapshot); err == nil {
		t.Fatal("Fill() error = nil, want a refusal: the message landed at 2, not 1")
	}
}

// TestAPausedConsumerIsHandedNothing is how a run is scoped to one consumer of a service running
// several. The pause has to hold while the corpus is published AND survive the service re-creating
// the consumer with its own configuration, which is what a real service does on startup.
func TestAPausedConsumerIsHandedNothing(t *testing.T) {
	t.Parallel()

	store := stocked(t)

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	t.Cleanup(connection.Close)

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	consumers := make(map[string]jetstream.Consumer, 2)

	create := func(name string) {
		created, createErr := stream.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
			Durable:   name,
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		if createErr != nil {
			t.Fatalf("CreateOrUpdateConsumer(%q) error = %v", name, createErr)
		}

		consumers[name] = created
	}

	create("paused")
	create("scoped")

	names, err := store.Consumers(t.Context())
	if err != nil {
		t.Fatalf("Consumers() error = %v", err)
	}

	if !slices.Equal(names, []string{"paused", "scoped"}) {
		t.Fatalf("Consumers() = %q, want both, in name order", names)
	}

	if err := store.Pause(t.Context(), "paused"); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	create("paused")

	if _, err := store.Publish(t.Context(), corpus.SubjectPrefix+"orders", []byte(firstOrder)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if got := fetched(t, consumers["scoped"]); got != 1 {
		t.Errorf("the scoped consumer was handed %d messages, want 1", got)
	}

	if got := fetched(t, consumers["paused"]); got != 0 {
		t.Errorf("the paused consumer was handed %d messages, want none", got)
	}
}

// fetched pulls once and counts what arrived. A paused consumer must look idle, not broken, so an
// error here fails the test rather than counting as nothing.
func fetched(t *testing.T, consumer jetstream.Consumer) int {
	t.Helper()

	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}

	count := 0
	for range batch.Messages() {
		count++
	}

	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("batch error = %v, want an idle consumer to look idle", err)
	}

	return count
}

// stage scopes the corpus as a run does: clear, fill, and hand back the translation.
func stage(t *testing.T, store *corpus.Corpus, messages []corpus.Message) []corpus.Staged {
	t.Helper()

	if err := store.Clear(t.Context()); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}

	if err := store.Fill(t.Context(), messages); err != nil {
		t.Fatalf("Fill() error = %v", err)
	}

	return corpus.Numbering(messages)
}

// stocked starts a corpus holding one message per payload, in the order given.
func stocked(t *testing.T, payloads ...string) *corpus.Corpus {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	for _, payload := range payloads {
		if _, err := store.Publish(t.Context(), corpus.SubjectPrefix+"orders", []byte(payload)); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	return store
}

// drainSubjects replays the corpus and returns each payload, which is how a test sees what a service
// under test would actually be handed.
func drainSubjects(t *testing.T, store *corpus.Corpus) []string {
	t.Helper()

	replay, err := store.Replay(t.Context(), "drain-"+t.Name(), corpus.ConsumerOptions{
		AckWait:       time.Second,
		MaxDeliver:    1,
		MaxAckPending: 1,
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	var payloads []string

	for {
		delivery, err := replay.Next(t.Context(), 500*time.Millisecond)
		if err != nil {
			return payloads
		}

		payloads = append(payloads, string(delivery.Payload))

		if ackErr := delivery.Ack(); ackErr != nil {
			t.Fatalf("Ack() error = %v", ackErr)
		}
	}
}
