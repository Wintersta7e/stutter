package harness_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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

	config := observedConfig()
	config.AckWait = 300 * time.Millisecond

	built, _ := quirkySandbox(t, config, quirks{pullExpires: 5 * time.Second}, func(settings *harness.Config) {
		settings.Recorded = orders("ORD-BIG", 50, 1_000_000)
	})

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if !errors.Is(err, harness.ErrHoldExceeded) {
		t.Fatalf("Run() error = %v, want ErrHoldExceeded", err)
	}

	for _, want := range []string{"hold", "30ms"} {
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

			behaviour := quirks{filter: toy.SubjectOrderCreated, audit: true, pullExpires: 5 * time.Second}

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
