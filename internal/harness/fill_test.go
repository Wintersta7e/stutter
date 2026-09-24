package harness_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// orders is a corpus of count order messages, each payload padded to at least size bytes.
func orders(prefix string, count, size int) []corpus.Message {
	messages := make([]corpus.Message, 0, count)

	for at := range count {
		payload := orderPayload(fmt.Sprintf("%s-%d", prefix, at+1), "WIDGET-FILL")
		if pad := size - len(payload) - len(`,"pad":""`); pad > 0 {
			payload = append(payload[:len(payload)-1], fmt.Sprintf(`,"pad":%q}`, strings.Repeat("x", pad))...)
		}

		messages = append(messages, corpus.Message{
			Subject: toy.SubjectOrderCreated,
			Payload: payload,
			Seq:     uint64(at) + 1,
		})
	}

	return messages
}

// TestAFillPastItsHoldBoundStopsTheRun: a corpus too large to publish inside the hold's bound would
// have the bus redeliver a message the service is still waiting to be handed, so the run stops as soon
// as the bound passes, naming the hold and the bound.
func TestAFillPastItsHoldBoundStopsTheRun(t *testing.T) {
	t.Parallel()

	// A 20 ms deadline caps the bound at half of it, 10 ms, well under what 50 MB takes to publish.
	config := observedConfig()
	config.AckWait = 20 * time.Millisecond

	built, _ := quirkySandbox(t, config, quirks{pullExpires: 5 * time.Second}, func(settings *harness.Config) {
		settings.Recorded = orders("ORD-BIG", 50, 1_000_000)
	})

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if !errors.Is(err, harness.ErrHoldExceeded) {
		t.Fatalf("Run() error = %v, want ErrHoldExceeded", err)
	}

	for _, want := range []string{"hold", "10ms"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Run() error = %v, want it to name %q", err, want)
		}
	}

	holds := built.Holds()
	if len(holds) == 0 {
		t.Fatal("Holds() is empty: the stopped run left no record")
	}

	if last := holds[len(holds)-1]; last.Length < last.Bound {
		t.Errorf("the last hold lasted %s, within its bound %s, yet the run stopped", last.Length, last.Bound)
	}
}

// TestFillIsHeldAgainstASelfPublishingHandler: a handler that publishes into the stream it consumes
// would land its output between the corpus's own messages while they are still being published, and
// the corpus would no longer be numbered as staged. Held, it cannot act until the corpus is in.
func TestFillIsHeldAgainstASelfPublishingHandler(t *testing.T) {
	t.Parallel()

	const (
		repeats    = 10
		corpusSize = 500
	)

	t.Logf("%d repeats of %d messages", repeats, corpusSize)

	if repeats == 0 {
		t.Fatal("no repeats")
	}

	for repeat := range repeats {
		t.Run(fmt.Sprintf("repeat %d", repeat+1), func(t *testing.T) {
			t.Parallel()

			config := observedConfig()
			config.AckWait = 5 * time.Second
			config.FilterSubjects = []string{toy.SubjectOrderCreated}

			behaviour := quirks{audit: true, pullExpires: 5 * time.Second}

			built, _ := quirkySandbox(t, config, behaviour, func(settings *harness.Config) {
				settings.Recorded = orders("ORD-AUDIT", corpusSize, 0)
			})

			_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
			if errors.Is(err, corpus.ErrFill) {
				t.Fatalf("Run() error = %v: the handler's output landed inside the corpus", err)
			}

			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			for _, hold := range built.Holds() {
				t.Logf("hold %s of bound %s set by the %s", hold.Length, hold.Bound, hold.Term)

				if hold.Length > hold.Bound/2 {
					t.Logf("the hold took more than half its bound: %s of %s", hold.Length, hold.Bound)
				}
			}
		})
	}
}

// TestEveryFilledRunLeavesOneHoldRecord: every run that published its corpus is measured, so how close
// a check came to its bound is on record for each run.
func TestEveryFilledRunLeavesOneHoldRecord(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := observedSandbox(t, config, "ORD-HOLD-1", "ORD-HOLD-2")

	session := &counting{inner: built}

	if _, err := check.Run(t.Context(), session, check.Options{
		Messages: recorded,
		Consumer: observedConsumer,
		Config:   config,
		MaxRuns:  1,
	}); err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	holds := built.Holds()

	t.Logf("%d runs, %d holds", session.runs, len(holds))

	if session.runs == 0 || len(holds) != session.runs {
		t.Fatalf("%d holds recorded for %d runs, want one per run", len(holds), session.runs)
	}

	for at, hold := range holds {
		if hold.Length >= hold.Bound {
			t.Errorf("hold %d lasted %s, not within its bound %s", at+1, hold.Length, hold.Bound)
		}
	}
}

// ownedBus opens a bus bound to the corpus stream, lets job make the bus's starting state on a direct
// connection, and checkpoints it as B1 — the shape a compose check hands every run.
func ownedBus(t *testing.T, job func(stream jetstream.JetStream)) (*corpus.Corpus, *corpus.Checkpoint) {
	t.Helper()

	root := t.TempDir()
	bus := filepath.Join(root, "bus")

	if err := os.Mkdir(bus, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	store, err := corpus.Open(t.Context(), filepath.Join(bus, "store"), corpus.StreamName)
	if err != nil {
		t.Fatalf("corpus.Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	job(stream)
	connection.Close()

	b1, err := store.Checkpoint(t.Context(), filepath.Join(bus, "B1"))
	if err != nil {
		t.Fatalf("Checkpoint(B1) error = %v", err)
	}

	return store, &b1
}

// createCorpusStream is a job creating the corpus stream the way a user's migration would.
func createCorpusStream(t *testing.T, stream jetstream.JetStream, maxMsgs int64) {
	t.Helper()

	if _, err := stream.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     corpus.StreamName,
		Subjects: []string{filterAll},
		MaxMsgs:  maxMsgs,
		Discard:  jetstream.DiscardOld,
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}
}

// TestAPendingShortfallStopsTheRun: a stream limit discards part of the corpus before the service is
// handed it, and a run over part of the corpus reads as a handler that did less. It stops, naming how
// many are missing.
func TestAPendingShortfallStopsTheRun(t *testing.T) {
	t.Parallel()

	store, b1 := ownedBus(t, func(stream jetstream.JetStream) { createCorpusStream(t, stream, 2) })

	built, _ := quirkySandbox(t, observedConfig(), quirks{}, func(settings *harness.Config) {
		settings.Corpus = store
		settings.Baseline = b1
		settings.Recorded = orders("ORD-SHORT", 3, 0)
	})

	result, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if !errors.Is(err, harness.ErrPending) {
		t.Fatalf("Run() error = %v (delivered %d), want ErrPending", err, result.Delivered)
	}

	if !strings.Contains(err.Error(), "shortfall of 1") {
		t.Errorf("Run() error = %v, want it to name a shortfall of 1", err)
	}
}

// TestAStreamAlreadyHoldingAdmittedMessagesNeverReachesAVerdict: messages a job left in the stream
// would reach the consumer beside the corpus; the run stops before or when it is handed them.
func TestAStreamAlreadyHoldingAdmittedMessagesNeverReachesAVerdict(t *testing.T) {
	t.Parallel()

	store, b1 := ownedBus(t, func(stream jetstream.JetStream) {
		createCorpusStream(t, stream, 0)

		for _, order := range []string{"ORD-LEFT-1", "ORD-LEFT-2"} {
			left := orderPayload(order, "WIDGET-FILL")
			if _, err := stream.Publish(t.Context(), toy.SubjectOrderCreated, left); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
		}
	})

	built, _ := quirkySandbox(t, observedConfig(), quirks{}, func(settings *harness.Config) {
		settings.Corpus = store
		settings.Baseline = b1
		settings.Recorded = orders("ORD-NEW", 3, 0)
	})

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err == nil {
		t.Fatal("Run() error = nil: a run over messages Stutter did not publish reached a verdict")
	}

	t.Logf("the run stopped: %v", err)

	if !errors.Is(err, harness.ErrPending) && !strings.Contains(err.Error(), "Stutter did not publish") {
		t.Errorf("Run() error = %v, want ErrPending or the fed-back stop", err)
	}
}
