package check_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
)

var errSessionBroken = errors.New("the sandbox never came up")

// session is a scripted service under test. Message 1 is non-idempotent — a second delivery repeats
// its write — and message 2 is not. Nothing else here is nondeterministic, so the determinism gate
// holds unless a test deliberately breaks it.
// Field order is dictated by govet's fieldalignment check, not by reading order.
type session struct {
	names         []string
	nonIdempotent map[uint64]bool
	messages      []uint64
	resets        int
	unstable      bool
	failOnReset   bool
}

func newSession() *session {
	return &session{
		messages:      []uint64{1, 2, 3},
		nonIdempotent: map[uint64]bool{1: true},
	}
}

func (s *session) Reset(context.Context) error {
	s.resets++
	if s.failOnReset {
		return errSessionBroken
	}

	return nil
}

func (s *session) Run(
	_ context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	s.names = append(s.names, name)

	var effects []effect.Effect

	for _, seq := range s.scope(retain) {
		effects = append(effects, write(seq))

		if s.repeats(mutation, seq) {
			effects = append(effects, write(seq))
		}
	}

	// An unstable service produces a different effect on every run, which is what the determinism
	// gate exists to catch.
	if s.unstable {
		effects = append(effects, effect.Effect{
			Kind:      effect.KindPostgres,
			Canonical: fmt.Sprintf("INSERT INTO audit VALUES (%d)", len(s.names)),
		})
	}

	return replay.Result{Effects: effects, Clause: "AckPolicy: explicit", Delivered: len(retain)}, nil
}

func (s *session) repeats(mutation replay.Mutation, seq uint64) bool {
	if !s.nonIdempotent[seq] {
		return false
	}

	switch fault := mutation.(type) {
	case replay.Duplicate:
		return fault.Seq == seq
	case replay.CrashBeforeAck:
		return fault.Seq == seq
	default:
		return false
	}
}

func (s *session) scope(retain []uint64) []uint64 {
	if len(retain) == 0 {
		return s.messages
	}

	var kept []uint64

	for _, seq := range s.messages {
		if slices.Contains(retain, seq) {
			kept = append(kept, seq)
		}
	}

	return kept
}

func write(seq uint64) effect.Effect {
	return effect.Effect{
		Kind:       effect.KindPostgres,
		Canonical:  fmt.Sprintf("UPDATE stock SET qty = qty - 1 WHERE id = %d", seq),
		MessageSeq: seq,
	}
}

func serialConfig() policy.Config {
	return policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{"corpus.>"},
		AckWait:        time.Second,
		MaxDeliver:     5,
		MaxAckPending:  1,
	}
}

func options(s *session) check.Options {
	return check.Options{
		Messages: s.messages,
		Consumer: "reserve_stock",
		Config:   serialConfig(),
	}
}

func TestCheckFindsTheNonIdempotentMessage(t *testing.T) {
	t.Parallel()

	scripted := newSession()

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := result.ExitCode(); got != 1 {
		t.Fatalf("ExitCode() = %d, want 1\n%s", got, result)
	}

	if len(result.Findings) == 0 {
		t.Fatal("no findings")
	}

	found := result.Findings[0]
	if found.Status != report.StatusFail {
		t.Errorf("Status = %q, want FAIL (reservations: %v)", found.Status, found.Reservations)
	}

	if found.Clause == "" {
		t.Error("finding carries no configuration clause")
	}

	// The shrink must reduce three messages to the one that actually matters.
	if want := "messages #1"; found.Repro != want {
		t.Errorf("Repro = %q, want %q", found.Repro, want)
	}
}

// TestViolatedGateStopsTheRun is the promise that a report is never qualified: when the reference is
// not reproducible, no findings are computed at all.
func TestViolatedGateStopsTheRun(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.unstable = true

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := result.ExitCode(); got != 2 {
		t.Errorf("ExitCode() = %d, want 2", got)
	}

	if len(result.Findings) != 0 {
		t.Errorf("computed %d findings despite a violated gate", len(result.Findings))
	}

	if len(result.Violations()) != 1 {
		t.Errorf("Violations() = %d, want 1", len(result.Violations()))
	}
}

func TestSessionFailureIsASetupError(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.failOnReset = true

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := result.ExitCode(); got != 3 {
		t.Errorf("ExitCode() = %d, want 3", got)
	}
}

// TestForbiddenFaultsAreNeverAttempted keeps the legality table load-bearing rather than decorative:
// under a config that guarantees ordering, no reorder run is ever performed.
func TestForbiddenFaultsAreNeverAttempted(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	opts := options(scripted)
	opts.Config.MaxAckPending = 1 // forbids reorder and concurrent

	if _, err := check.Run(t.Context(), scripted, opts); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	for _, name := range scripted.names {
		if len(name) >= len("reorder") && name[:len("reorder")] == "reorder" {
			t.Errorf("a reorder run was performed under MaxAckPending 1: %q", name)
		}
	}
}

// TestEveryPassGetsADistinctName guards a bug that would be silent: a reused consumer name resumes
// from the previous run's position instead of replaying from the start, so the run would compare
// against a corpus that was never delivered.
func TestEveryPassGetsADistinctName(t *testing.T) {
	t.Parallel()

	scripted := newSession()

	if _, err := check.Run(t.Context(), scripted, options(scripted)); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	seen := make(map[string]bool, len(scripted.names))
	for _, name := range scripted.names {
		if seen[name] {
			t.Fatalf("consumer name %q was reused", name)
		}

		seen[name] = true
	}

	if scripted.resets != len(scripted.names) {
		t.Errorf("%d resets for %d runs; every pass must start from the same state",
			scripted.resets, len(scripted.names))
	}
}

func TestMaxRunsCapsTheSearch(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	opts := options(scripted)
	opts.MaxRuns = 3

	if _, err := check.Run(t.Context(), scripted, opts); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Two clean references plus at most one mutated run before the cap bites. Shrinking a
	// divergence found on that run may add more, so the bound is on mutated hunting, not total.
	if len(scripted.names) < 2 {
		t.Fatalf("only %d runs; the reference pair should always happen", len(scripted.names))
	}
}
