package corpus_test

import (
	"testing"
	"time"

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

	staged, err := store.Stage(t.Context(), keep)
	if err != nil {
		t.Fatalf("Stage() error = %v", err)
	}

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

	if _, err := store.Stage(t.Context(), []corpus.Message{snapshot[1]}); err != nil {
		t.Fatalf("Stage() a subset error = %v", err)
	}

	if delivered := drainSubjects(t, store); len(delivered) != 1 {
		t.Fatalf("the subset corpus delivered %q, want ORD-2 alone", delivered)
	}

	if _, err := store.Stage(t.Context(), snapshot); err != nil {
		t.Fatalf("Stage() the whole snapshot again error = %v", err)
	}

	delivered := drainSubjects(t, store)
	if len(delivered) != len(snapshot) {
		t.Fatalf("the restored corpus delivered %q, want all three messages back", delivered)
	}

	if delivered[0] != firstOrder || delivered[2] != "ORD-3" {
		t.Errorf("delivered %q, want the recorded order back", delivered)
	}
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
