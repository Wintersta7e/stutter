package shrink_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/shrink"
)

// errReplay stands in for a replay that did not complete: a container that died, a proxy that hung
// up. It must never be read as "this candidate did not reproduce".
var errReplay = errors.New("replay did not complete")

// decision is the fake's verdict on one candidate, standing in for a real replay-and-compare.
type decision func(messages []uint64, mutations []replay.Mutation) (bool, error)

// spy is an Attempt with a call count, so a test can assert what the search actually spent rather
// than trusting the Stats the search reports about itself.
type spy struct {
	decide decision
	calls  int
}

func (s *spy) attempt(_ context.Context, messages []uint64, mutations []replay.Mutation) (bool, error) {
	s.calls++

	return s.decide(messages, mutations)
}

// needs builds a decision that reproduces exactly when every listed message and every listed
// mutation is retained — a failure with one definite cause, which the search has to find exactly.
func needs(messages []uint64, mutations []replay.Mutation) decision {
	return func(gotMessages []uint64, gotMutations []replay.Mutation) (bool, error) {
		for _, want := range messages {
			if !slices.Contains(gotMessages, want) {
				return false, nil
			}
		}

		for _, want := range mutations {
			if !slices.Contains(gotMutations, want) {
				return false, nil
			}
		}

		return true, nil
	}
}

// failOn wraps a decision so that the nth attempt reports a broken replay instead of a verdict.
func failOn(nth int, inner decision) decision {
	calls := 0

	return func(gotMessages []uint64, gotMutations []replay.Mutation) (bool, error) {
		calls++
		if calls == nth {
			return false, errReplay
		}

		return inner(gotMessages, gotMutations)
	}
}

func run(t *testing.T, start shrink.Candidate, decide decision, opts shrink.Options) (
	shrink.Candidate, shrink.Stats, *spy,
) {
	t.Helper()

	fake := &spy{decide: decide}

	got, stats, err := shrink.Shrink(t.Context(), start, fake.attempt, opts)
	if err != nil {
		t.Fatalf("Shrink() = %v, want no error", err)
	}

	return got, stats, fake
}

// messages numbers a corpus from one, matching the stream sequences a real candidate carries.
func messages(count uint64) []uint64 {
	out := make([]uint64, 0, count)
	for seq := uint64(1); seq <= count; seq++ {
		out = append(out, seq)
	}

	return out
}

// faults is the mutation set a real finding starts with: several legal faults, at most one of which
// the failure actually needs.
func faults() []replay.Mutation {
	return []replay.Mutation{
		replay.Duplicate{Seq: 3},
		replay.Reorder{First: 5},
		replay.Delay{Seq: 2, For: time.Second},
		replay.CrashBeforeAck{Seq: 4, Times: 2},
	}
}

func TestShrinkFindsTheMinimalMessages(t *testing.T) {
	t.Parallel()

	const corpusSize = 12

	start := shrink.Candidate{
		Messages:  messages(corpusSize),
		Mutations: []replay.Mutation{replay.Duplicate{Seq: 3}},
	}

	got, stats, fake := run(t, start, needs([]uint64{3, 7}, nil), shrink.Options{})

	if want := []uint64{3, 7}; !slices.Equal(got.Messages, want) {
		t.Errorf("Messages = %v, want %v", got.Messages, want)
	}

	if len(got.Mutations) != 1 {
		t.Errorf("Mutations = %v, want the one mutation kept", got.Mutations)
	}

	if stats.StartMessages != corpusSize || stats.FinalMessages != 2 {
		t.Errorf("Stats = %+v, want a reduction from %d messages to 2", stats, corpusSize)
	}

	if !stats.Minimal() {
		t.Error("Minimal() = false on a search that ran to completion")
	}

	if stats.Attempts != fake.calls {
		t.Errorf("Stats.Attempts = %d, but Attempt was called %d times", stats.Attempts, fake.calls)
	}
}

func TestShrinkFindsTheOnlyMutationThatMatters(t *testing.T) {
	t.Parallel()

	want := replay.Reorder{First: 5}
	start := shrink.Candidate{Messages: messages(8), Mutations: faults()}

	got, stats, _ := run(t, start, needs(nil, []replay.Mutation{want}), shrink.Options{})

	if len(got.Mutations) != 1 || got.Mutations[0] != want {
		t.Errorf("Mutations = %v, want exactly [%v]", got.Mutations, want)
	}

	// The message axis floors at one, never zero: a candidate delivering nothing produces no effects
	// and so cannot reproduce, which makes the empty set not worth an attempt.
	if len(got.Messages) != 1 {
		t.Errorf("Messages = %v, want a single message once the messages stop mattering", got.Messages)
	}

	if stats.StartMutations != len(faults()) || stats.FinalMutations != 1 {
		t.Errorf("Stats = %+v, want a reduction from %d mutations to 1", stats, len(faults()))
	}
}

func TestShrinkReducesBothAxes(t *testing.T) {
	t.Parallel()

	wantMessages := []uint64{3, 7}
	wantMutation := replay.Duplicate{Seq: 3}

	start := shrink.Candidate{Messages: messages(12), Mutations: faults()}
	decide := needs(wantMessages, []replay.Mutation{wantMutation})

	got, stats, _ := run(t, start, decide, shrink.Options{})

	if !slices.Equal(got.Messages, wantMessages) {
		t.Errorf("Messages = %v, want %v", got.Messages, wantMessages)
	}

	if len(got.Mutations) != 1 || got.Mutations[0] != wantMutation {
		t.Errorf("Mutations = %v, want exactly [%v]", got.Mutations, wantMutation)
	}

	if !stats.Minimal() {
		t.Errorf("Stats = %+v, want a completed search to claim minimality", stats)
	}
}

// TestShrinkIsDeterministic pins the whole point of a repro: two runs over the same failing input
// must hand the user the same two messages, not two different plausible answers.
func TestShrinkIsDeterministic(t *testing.T) {
	t.Parallel()

	const repeats = 8

	cases := []struct {
		name   string
		decide decision
		start  shrink.Candidate
	}{
		{
			name:   "one definite cause on both axes",
			start:  shrink.Candidate{Messages: messages(12), Mutations: faults()},
			decide: needs([]uint64{3, 7}, []replay.Mutation{replay.Duplicate{Seq: 3}}),
		},
		{
			// Ambiguous on purpose: any two messages will do, so there are 28 equally minimal answers
			// and only a deterministic tie-break makes the reported one reproducible.
			name:  "many equally minimal answers",
			start: shrink.Candidate{Messages: messages(8), Mutations: faults()},
			decide: func(got []uint64, _ []replay.Mutation) (bool, error) {
				return len(got) >= 2, nil
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			first, firstStats, _ := run(t, testCase.start, testCase.decide, shrink.Options{})

			for range repeats {
				got, stats, _ := run(t, testCase.start, testCase.decide, shrink.Options{})

				if !slices.Equal(got.Messages, first.Messages) {
					t.Fatalf("Messages = %v, want %v on every run", got.Messages, first.Messages)
				}

				if !slices.Equal(got.Mutations, first.Mutations) {
					t.Fatalf("Mutations = %v, want %v on every run", got.Mutations, first.Mutations)
				}

				if stats.Attempts != firstStats.Attempts {
					t.Fatalf("Attempts = %d, want %d on every run", stats.Attempts, firstStats.Attempts)
				}
			}
		})
	}
}

// TestShrinkResultIsOneMinimal checks the property rather than a hard-coded answer: nothing can be
// removed from the result and still reproduce. A search that stops one step early satisfies every
// equality assertion above and fails this.
func TestShrinkResultIsOneMinimal(t *testing.T) {
	t.Parallel()

	decide := needs([]uint64{3, 7}, []replay.Mutation{replay.Duplicate{Seq: 3}})
	start := shrink.Candidate{Messages: messages(12), Mutations: faults()}

	got, _, _ := run(t, start, decide, shrink.Options{})

	for index := range got.Messages {
		fewer := slices.Concat(got.Messages[:index], got.Messages[index+1:])

		reproduces, err := decide(fewer, got.Mutations)
		if err != nil {
			t.Fatalf("decide() = %v, want no error", err)
		}

		if reproduces {
			t.Errorf("dropping message %d still reproduces: %v is not minimal",
				got.Messages[index], got.Messages)
		}
	}

	for index := range got.Mutations {
		fewer := slices.Concat(got.Mutations[:index], got.Mutations[index+1:])

		reproduces, err := decide(got.Messages, fewer)
		if err != nil {
			t.Fatalf("decide() = %v, want no error", err)
		}

		if reproduces {
			t.Errorf("dropping mutation %v still reproduces: %v is not minimal",
				got.Mutations[index], got.Mutations)
		}
	}
}

// TestShrinkRespectsTheAttemptCap covers the input that cannot be reduced at all, which is where an
// unbounded search would spend hours: the answer must be the candidate it started from, and it must
// not be called minimal.
func TestShrinkRespectsTheAttemptCap(t *testing.T) {
	t.Parallel()

	const (
		corpusSize = 64
		budget     = 20
	)

	all := messages(corpusSize)
	start := shrink.Candidate{Messages: all, Mutations: []replay.Mutation{replay.Duplicate{Seq: 3}}}

	got, stats, fake := run(t, start, needs(all, nil), shrink.Options{MaxAttempts: budget})

	if !stats.CapHit {
		t.Errorf("Stats = %+v, want CapHit on a search that could never reduce", stats)
	}

	if stats.Minimal() {
		t.Error("Minimal() = true after the cap truncated the search")
	}

	if stats.Attempts != budget || fake.calls != budget {
		t.Errorf("Attempts = %d and %d calls, want exactly the cap of %d",
			stats.Attempts, fake.calls, budget)
	}

	if !slices.Equal(got.Messages, all) {
		t.Errorf("Messages = %v, want the %d it started with — the best candidate proved so far",
			got.Messages, corpusSize)
	}
}

// TestShrinkPropagatesAttemptErrors guards against the tempting shortcut of reading a failed replay
// as "did not reproduce", which would let the search delete the real cause and then report whatever
// happened to survive.
func TestShrinkPropagatesAttemptErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		nth  int
	}{
		{name: "the run that verifies the starting candidate", nth: 1},
		{name: "a run partway through the search", nth: 3},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			fake := &spy{decide: failOn(testCase.nth, needs([]uint64{3, 7}, nil))}
			start := shrink.Candidate{Messages: messages(12), Mutations: faults()}

			_, stats, err := shrink.Shrink(t.Context(), start, fake.attempt, shrink.Options{})
			if !errors.Is(err, errReplay) {
				t.Fatalf("Shrink() error = %v, want it to carry %v", err, errReplay)
			}

			if stats.Attempts != testCase.nth {
				t.Errorf("Attempts = %d, want the search to stop at %d", stats.Attempts, testCase.nth)
			}
		})
	}
}

func TestShrinkRejectsAStartThatDoesNotReproduce(t *testing.T) {
	t.Parallel()

	fake := &spy{decide: func(_ []uint64, _ []replay.Mutation) (bool, error) { return false, nil }}
	start := shrink.Candidate{Messages: messages(12), Mutations: faults()}

	got, stats, err := shrink.Shrink(t.Context(), start, fake.attempt, shrink.Options{})
	if !errors.Is(err, shrink.ErrNotReproducible) {
		t.Fatalf("Shrink() error = %v, want %v", err, shrink.ErrNotReproducible)
	}

	if got.Messages != nil || got.Mutations != nil {
		t.Errorf("Candidate = %+v, want nothing on a start that never reproduced", got)
	}

	if stats.Attempts != 1 {
		t.Errorf("Attempts = %d, want the single verification attempt", stats.Attempts)
	}
}

// TestShrinkLeavesTheStartingCandidateAlone matters because the caller still holds the failing run
// it is reporting: the search must not reduce that out from under it.
func TestShrinkLeavesTheStartingCandidateAlone(t *testing.T) {
	t.Parallel()

	start := shrink.Candidate{Messages: messages(12), Mutations: faults()}
	wantMessages := slices.Clone(start.Messages)
	wantMutations := slices.Clone(start.Mutations)

	got, _, _ := run(t, start, needs([]uint64{3, 7}, nil), shrink.Options{})

	got.Messages[0] = 0

	if !slices.Equal(start.Messages, wantMessages) {
		t.Errorf("start.Messages = %v, want it untouched at %v", start.Messages, wantMessages)
	}

	if !slices.Equal(start.Mutations, wantMutations) {
		t.Errorf("start.Mutations = %v, want it untouched at %v", start.Mutations, wantMutations)
	}
}
