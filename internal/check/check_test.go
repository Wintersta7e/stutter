package check_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
)

var errSessionBroken = errors.New("the sandbox never came up")

// stockWrite is the canonical form the scripted session's non-idempotent write renders as, named once
// so a declaration in a test and the effect it is meant to match cannot drift apart.
const stockWrite = "UPDATE stock"

// session is a scripted service under test. Message 1 is non-idempotent — a second delivery repeats
// its write — and message 2 is not. Nothing else here is nondeterministic, so the determinism gate
// holds unless a test deliberately breaks it.
// Field order is dictated by govet's fieldalignment check, not by reading order.
type session struct {
	names         []string
	nonIdempotent map[uint64]bool
	stubbedGuard  map[uint64]bool
	// refuses are the faults this session cannot express against the service it drives, as a run
	// watched on the wire cannot express a delay.
	refuses map[policy.Fault]bool
	// silent are messages the service takes delivery of and does nothing observable with.
	silent map[uint64]bool
	// readGuard are messages whose handler looks its claim up before writing, and looks again when the
	// message is redelivered. Whether it writes again too is nonIdempotent's business.
	readGuard   map[uint64]bool
	messages    []uint64
	resets      int
	unstable    bool
	failOnReset bool
	// refusesAll marks every effect as refused by its dependency, so nothing it does changes anything.
	refusesAll bool
	// flaky repeats a non-idempotent write only on a hunt's faulted run, never when a shrink replays
	// the same fault: a divergence that does not reproduce.
	flaky bool
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
	if s.refuses[mutation.Fault()] {
		return replay.Result{}, fmt.Errorf("%w: this session cannot express %s",
			replay.ErrUnsupported, mutation.Fault())
	}

	s.names = append(s.names, name)

	var effects []effect.Effect

	for _, seq := range s.scope(retain) {
		if s.silent[seq] {
			continue
		}

		if s.stubbedGuard[seq] {
			effects = append(effects, effect.Effect{
				Kind:       effect.KindHTTP,
				Canonical:  "GET guard.example.test/claimed",
				MessageSeq: seq,
				Stubbed:    mutation.Fault() != policy.FaultNone,
			})
		}

		if s.readGuard[seq] {
			effects = append(effects, claimLookup(seq))
		}

		effects = append(effects, write(seq))

		if s.readGuard[seq] && redelivers(mutation, seq) {
			effects = append(effects, claimLookup(seq))
		}

		if s.repeats(mutation, seq) && (!s.flaky || !strings.HasPrefix(name, "shrink")) {
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

	if s.refusesAll {
		for index := range effects {
			effects[index].Rejected = true
		}
	}

	return replay.Result{Effects: effects, Clause: "AckPolicy: explicit", Delivered: len(s.scope(retain))}, nil
}

func (s *session) repeats(mutation replay.Mutation, seq uint64) bool {
	return s.nonIdempotent[seq] && redelivers(mutation, seq)
}

// redelivers reports whether a mutation hands this message to the handler a second time.
func redelivers(mutation replay.Mutation, seq uint64) bool {
	switch fault := mutation.(type) {
	case replay.Duplicate:
		return fault.Seq == seq
	case replay.CrashBeforeAck:
		return fault.Seq == seq
	default:
		return false
	}
}

// claimLookup is a read-based dedupe guard looking its claim up.
func claimLookup(seq uint64) effect.Effect {
	return effect.Effect{
		Kind:       effect.KindPostgres,
		Canonical:  fmt.Sprintf("SELECT 1 FROM processed WHERE id = %d", seq),
		MessageSeq: seq,
		Read:       true,
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

// TestCheckPropagatesAStubbedGuard proves the report's guard-dependent rule is reached from live
// replay effects. Report unit tests that hand-build StubReads do not catch a missing producer here.
func TestCheckPropagatesAStubbedGuard(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.stubbedGuard = map[uint64]bool{1: true}

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(result.Findings) == 0 {
		t.Fatal("no findings")
	}

	found := result.Findings[0]
	if found.Confidence != report.ConfidenceGuardDependent {
		t.Errorf("Confidence = %q, want %q", found.Confidence, report.ConfidenceGuardDependent)
	}

	if found.Status != report.StatusWarn {
		t.Errorf("Status = %q, want WARN (reservations: %v)", found.Status, found.Reservations)
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

// TestANonReproducingDivergenceViolatesDeterminism: a fault that diverged once and not again when the
// shrink replayed it says the service is not deterministic under that fault, which is the determinism
// gate's business — not a broken sandbox. Reported as a setup error, it sent the reader to the wrong
// place and hid which fault and message did it.
func TestANonReproducingDivergenceViolatesDeterminism(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.flaky = true

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	t.Logf("report:\n%s", result)

	if got := result.ExitCode(); got != report.ExitGateViolated {
		t.Fatalf("ExitCode() = %d, want %d (gate violated)\n%s", got, report.ExitGateViolated, result)
	}

	violations := result.Violations()
	if len(violations) != 1 || violations[0].Name != report.GateDeterminism {
		t.Fatalf("Violations() = %+v, want the determinism gate alone", violations)
	}

	violated := violations[0]
	if violated.Result.Class != gate.ClassNotReproducible {
		t.Errorf("Class = %q, want %q", violated.Result.Class, gate.ClassNotReproducible)
	}

	if violated.Fault != policy.FaultDuplicate || violated.Result.Message != 1 {
		t.Errorf("violation names %q on message %d, want %q on message 1",
			violated.Fault, violated.Result.Message, policy.FaultDuplicate)
	}

	if len(result.Findings) != 0 {
		t.Errorf("Findings = %d, want none beside a violated gate", len(result.Findings))
	}

	for _, want := range []string{"duplicate delivery", "message #1"} {
		if !strings.Contains(result.String(), want) {
			t.Errorf("the report does not name %q:\n%s", want, result)
		}
	}
}

// TestACleanRunThatSawNothingIsWithheld is the measured false negative: a service that never reached
// its proxies produced two empty clean runs, determinism held over nothing, and the report read PASS.
func TestACleanRunThatSawNothingIsWithheld(t *testing.T) {
	t.Parallel()

	cases := []struct {
		configure func(*session)
		name      string
	}{
		{
			name:      "no effects at all",
			configure: func(s *session) { s.silent = map[uint64]bool{1: true, 2: true, 3: true} },
		},
		{
			// A refused operation changed nothing, so a run made only of refusals compared nothing.
			name:      "every effect refused",
			configure: func(s *session) { s.refusesAll = true },
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			scripted := newSession()
			testCase.configure(scripted)

			result, err := check.Run(t.Context(), scripted, options(scripted))
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			if got := result.ExitCode(); got != report.ExitGateViolated {
				t.Errorf("ExitCode() = %d, want %d (gate violated)", got, report.ExitGateViolated)
			}

			violations := result.Violations()
			if len(violations) != 1 || violations[0].Name != report.GateObservation {
				t.Errorf("Violations() = %v, want only %q", violations, report.GateObservation)
			}

			// The gate stops the check before any fault is spent on a service nobody saw.
			if len(scripted.names) != 2 {
				t.Errorf("ran %v, want only the reference pair", scripted.names)
			}
		})
	}
}

// TestCleanRunHealthIsReported keeps the counts a verdict rests on beside the verdict.
func TestCleanRunHealthIsReported(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.silent = map[uint64]bool{3: true}

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Health == nil {
		t.Fatal("Health = nil, want the clean run's counts")
	}

	want := report.Health{Messages: 3, Delivered: 3, Effects: 2, Silent: 1}
	if *result.Health != want {
		t.Errorf("Health = %+v, want %+v", *result.Health, want)
	}

	// A message that did nothing is not a reason to withhold the report: a handler that filters is
	// entitled to ignore some of what it is sent.
	if violations := result.Violations(); len(violations) != 0 {
		t.Errorf("Violations() = %v, want none while some messages produced effects", violations)
	}
}

// TestAGuardThatOnlyLooksAgainIsNotAFailure: a read-based dedupe guard finds its claim on redelivery
// and does nothing else. The extra read is a real difference, so it is still reported — but a read
// changes no data, so it warns rather than fails.
func TestAGuardThatOnlyLooksAgainIsNotAFailure(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.nonIdempotent = nil
	scripted.readGuard = map[uint64]bool{1: true}

	opts := options(scripted)
	opts.MaxRuns = 1

	result, err := check.Run(t.Context(), scripted, opts)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := result.ExitCode(); got != report.ExitPass {
		t.Errorf("ExitCode() = %d, want %d: a guard that only looked again failed\n%s", got, report.ExitPass, result)
	}

	if len(result.Findings) != 1 || result.Findings[0].Status != report.StatusWarn {
		t.Fatalf("Findings = %+v, want one WARN for the extra read", result.Findings)
	}

	explained := slices.ContainsFunc(result.Findings[0].Reservations, func(reservation string) bool {
		return strings.Contains(reservation, "only difference is a read")
	})
	if !explained {
		t.Errorf("Reservations = %q, want the reason the read was not ruled a failure", result.Findings[0].Reservations)
	}
}

// TestAGuardThatMissesStillFails: its FIRST difference is the extra read, and the write behind it is
// what matters. Judging the first difference alone would call this one harmless.
func TestAGuardThatMissesStillFails(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.readGuard = map[uint64]bool{1: true}

	opts := options(scripted)
	opts.MaxRuns = 1

	result, err := check.Run(t.Context(), scripted, opts)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := result.ExitCode(); got != report.ExitFail {
		t.Errorf("ExitCode() = %d, want %d: a guard that read and then wrote again was let through\n%s",
			got, report.ExitFail, result)
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

// TestAnInexpressibleFaultIsSkippedNotFatal covers a session that cannot commit every fault the
// recorded configuration permits. A service that consumes for itself is faulted by swallowing its
// own acknowledgements, and a delay has to act before the service has been handed the message, so
// the session refuses it. Treating that refusal as a setup failure would throw away every real
// finding beside it.
func TestAnInexpressibleFaultIsSkippedNotFatal(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	scripted.refuses = map[policy.Fault]bool{policy.FaultDelay: true}

	result, err := check.Run(t.Context(), scripted, options(scripted))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := result.ExitCode(); got == 3 {
		t.Fatalf("a refused fault was reported as a setup error:\n%s", result)
	}

	if len(result.Findings) == 0 {
		t.Errorf("the refusal cost the run every other finding:\n%s", result)
	}

	for _, name := range scripted.names {
		if strings.HasPrefix(name, string(policy.FaultDelay)) {
			t.Errorf("a delay run was performed by a session that refuses delay: %q", name)
		}
	}
}

// TestAnInvariantSilencesADivergence is the answer to work that repeats harmlessly.
//
// A doubled audit row is a real duplicated write and Stutter is right to see it, but whether it
// matters is knowledge only the handler's owner has. Silencing is counted rather than dropped, so a
// run that silenced everything cannot read as a clean one.
func TestAnInvariantSilencesADivergence(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	opts := options(scripted)
	opts.Invariants = []check.Invariant{{
		Matches: stockWrite,
		Impact:  report.ImpactAcceptable,
		Because: "stock updates are absolute, not relative",
	}}

	result, err := check.Run(t.Context(), scripted, opts)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(result.Findings) != 0 {
		t.Fatalf("Findings = %d, want 0 — the invariant did not silence the divergence:\n%s",
			len(result.Findings), result)
	}

	if result.Silenced == 0 {
		t.Error("the divergence was dropped rather than counted; a silenced run must not read as clean")
	}

	if got := result.ExitCode(); got != 0 {
		t.Errorf("ExitCode() = %d, want 0", got)
	}
}

// TestAnInvariantPromotesADivergenceToFail is the other direction, and the one that changes an exit
// code. A repeated outbound call defaults to WARN because Stutter cannot tell a charge from a ping;
// an owner who knows it is a charge says so once.
func TestAnInvariantPromotesADivergenceToFail(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	opts := options(scripted)
	opts.Invariants = []check.Invariant{{
		Consumer: "reserve_stock",
		Matches:  stockWrite,
		Impact:   report.ImpactCorrupting,
		Because:  "every reservation is money",
	}}

	result, err := check.Run(t.Context(), scripted, opts)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(result.Findings) == 0 {
		t.Fatalf("no findings at all:\n%s", result)
	}

	found := result.Findings[0]
	if found.Status != report.StatusFail {
		t.Errorf("Status = %q, want FAIL — the invariant did not promote it (reservations: %v)",
			found.Status, found.Reservations)
	}

	if !strings.Contains(result.String(), "every reservation is money") {
		t.Errorf("the report does not say which declaration decided this:\n%s", result)
	}

	if got := result.ExitCode(); got != 1 {
		t.Errorf("ExitCode() = %d, want 1", got)
	}
}

// TestAnInvariantForAnotherConsumerIsNotApplied keeps a rule from leaking across handlers. Two
// consumers writing the same table is ordinary, and a declaration about one of them is not a
// statement about the other.
func TestAnInvariantForAnotherConsumerIsNotApplied(t *testing.T) {
	t.Parallel()

	scripted := newSession()
	opts := options(scripted)
	opts.Invariants = []check.Invariant{{
		Consumer: "some_other_consumer",
		Matches:  stockWrite,
		Impact:   report.ImpactAcceptable,
		Because:  "not this handler",
	}}

	result, err := check.Run(t.Context(), scripted, opts)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Silenced != 0 {
		t.Errorf("Silenced = %d, want 0 — another consumer's declaration was applied here", result.Silenced)
	}

	if len(result.Findings) == 0 {
		t.Errorf("the finding was silenced by a rule naming a different consumer:\n%s", result)
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
