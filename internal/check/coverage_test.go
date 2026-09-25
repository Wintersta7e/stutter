package check_test

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
)

// permissiveConfig licenses all four faults the check tries: explicit acks, a first deadline to cross,
// unlimited deliveries, and room for more than one message in flight.
func permissiveConfig() policy.Config {
	return policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{"orders.created", "orders.shipped"},
		AckWait:        time.Second,
		MaxDeliver:     -1,
		MaxAckPending:  10,
	}
}

// spent is one coverage line's pairs, as {pairs, attempted, unexpressed, cut by budget}.
type spent [4]int

func spentOf(line report.Coverage) spent {
	return spent{line.Pairs, line.Attempted, line.Unexpressed, line.CutByBudget}
}

// TestCoverageAccountsForEveryPair: every message-and-fault pair of a legal fault was run, refused by
// the session, or never reached because the budget ran out — exactly one of the three.
func TestCoverageAccountsForEveryPair(t *testing.T) {
	t.Parallel()

	cases := []struct {
		want    map[policy.Fault]spent
		maxRuns int
	}{
		{maxRuns: 4, want: map[policy.Fault]spent{
			policy.FaultDuplicate:      {3, 3, 0, 0},
			policy.FaultCrashBeforeAck: {3, 1, 0, 2},
			policy.FaultDelay:          {3, 0, 0, 3},
			policy.FaultReorder:        {3, 0, 0, 3},
		}},
		{maxRuns: 0, want: map[policy.Fault]spent{
			policy.FaultDuplicate:      {3, 3, 0, 0},
			policy.FaultCrashBeforeAck: {3, 3, 0, 0},
			policy.FaultDelay:          {3, 0, 3, 0},
			policy.FaultReorder:        {3, 0, 3, 0},
		}},
	}

	pairs := 0

	for _, testCase := range cases {
		// Never diverging, and refusing the two faults a service that pulls for itself cannot be given.
		scripted := newSession()
		scripted.nonIdempotent = nil
		scripted.refuses = map[policy.Fault]bool{policy.FaultDelay: true, policy.FaultReorder: true}

		opts := options(scripted)
		opts.Config = permissiveConfig()
		opts.MaxRuns = testCase.maxRuns

		result, err := check.Run(t.Context(), scripted, opts)
		if err != nil {
			t.Fatalf("MaxRuns %d: Run() error = %v", testCase.maxRuns, err)
		}

		if len(result.Coverage) != len(testCase.want) {
			t.Fatalf("MaxRuns %d: %d coverage lines, want one per fault tried (%d): %+v",
				testCase.maxRuns, len(result.Coverage), len(testCase.want), result.Coverage)
		}

		for _, line := range result.Coverage {
			if !line.Legal {
				t.Errorf("MaxRuns %d: %s is not legal under a configuration licensing it: %s",
					testCase.maxRuns, line.Fault, line.Clause)
			}

			if got := spentOf(line); got != testCase.want[line.Fault] {
				t.Errorf("MaxRuns %d: %s = %v, want %v", testCase.maxRuns, line.Fault, got, testCase.want[line.Fault])
			}

			if sum := line.Attempted + line.Unexpressed + line.CutByBudget; sum != line.Pairs {
				t.Errorf("MaxRuns %d: %s: attempted %d + unexpressed %d + cut %d = %d, want %d pairs",
					testCase.maxRuns, line.Fault, line.Attempted, line.Unexpressed, line.CutByBudget, sum, line.Pairs)
			}

			pairs += line.Pairs
		}
	}

	t.Logf("faults=%d pairs=%d", len(cases[0].want), pairs)

	if pairs == 0 {
		t.Fatal("no pair was accounted for")
	}
}

// TestAnIllegalFaultNamesTheClauseRefusingIt: a fault the configuration refuses has no pairs, and the
// line says which part of the configuration refused it.
func TestAnIllegalFaultNamesTheClauseRefusingIt(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.nonIdempotent = nil

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	index := slices.IndexFunc(result.Coverage, func(c report.Coverage) bool { return c.Fault == policy.FaultReorder })
	if index < 0 {
		t.Fatalf("no coverage line for reorder: %+v", result.Coverage)
	}

	reorder := result.Coverage[index]
	if reorder.Legal || reorder.Pairs != 0 || !strings.Contains(reorder.Clause, "MaxAckPending") {
		t.Errorf("reorder under MaxAckPending 1 = %+v, want illegal, no pairs, and the clause naming MaxAckPending",
			reorder)
	}
}

func TestSilentMessagesAreNamedBySequence(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.silent = map[uint64]bool{2: true}

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Health == nil {
		t.Fatal("no health was measured")
	}

	if got := result.Health.SilentSeqs; !slices.Equal(got, []uint64{2}) || result.Health.Silent != 1 {
		t.Errorf("SilentSeqs = %v, Silent = %d, want [2] and 1", got, result.Health.Silent)
	}
}

// failingAt is a session whose nth run fails with an environment error.
type failingAt struct {
	check.Session

	runs int
	at   int
}

func (f *failingAt) Run(
	ctx context.Context, name string, mutation replay.Mutation, retain []uint64,
) (replay.Result, error) {
	f.runs++
	if f.runs == f.at {
		return replay.Result{}, errDialFailed
	}

	return f.Session.Run(ctx, name, mutation, retain)
}

// TestASetupErrorCountsTheRunsBeforeIt: a setup error after runs completed is not a check where
// nothing was replayed.
func TestASetupErrorCountsTheRunsBeforeIt(t *testing.T) {
	t.Parallel()

	for at, want := range map[int]int{3: 2, 1: 0} {
		scripted := newSession()

		result, err := check.Run(t.Context(), &failingAt{Session: scripted, at: at}, options(scripted))
		if err != nil {
			t.Fatalf("run %d fails: Run() error = %v", at, err)
		}

		if result.Setup == nil {
			t.Fatalf("run %d fails: no setup error was reported", at)
		}

		if result.Completed != want {
			t.Errorf("run %d fails: Completed = %d, want %d", at, result.Completed, want)
		}
	}
}

// TestAReproNamesItsCorpusFiles: a sequence alone sends the reader counting files; the file name is
// what they open.
func TestAReproNamesItsCorpusFiles(t *testing.T) {
	t.Parallel()

	for files, want := range map[string]string{
		"named": "messages #1 (1.orders.created.json)",
		"nil":   "messages #1",
	} {
		scripted := newSession()
		opts := options(scripted)

		if files == "named" {
			opts.Files = map[uint64]string{1: "1.orders.created.json"}
		}

		result, err := check.Run(t.Context(), scripted, opts)
		if err != nil {
			t.Fatalf("%s: Run() error = %v", files, err)
		}

		if len(result.Findings) == 0 {
			t.Fatalf("%s: no findings", files)
		}

		if got := result.Findings[0].Repro; got != want {
			t.Errorf("%s: Repro = %q, want %q", files, got, want)
		}

		if !maps.Equal(result.Files, opts.Files) {
			t.Errorf("%s: Report.Files = %v, want the options' %v", files, result.Files, opts.Files)
		}
	}
}
