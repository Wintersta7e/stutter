package report_test

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/report"
)

const (
	clauseRedelivery = "AckPolicy: explicit with MaxDeliver 5 — an unacknowledged message is redelivered"
	consumerStock    = "reserve_stock"
	consumerReceipt  = "send_receipt"
	consumerCharge   = "charge_customer"
	reproStock       = "order.created#8891 delivered at T+0 and T+0.4s"
	reproReceipt     = "order.paid#41 delivered at T+0 and T+1.2s"
	// updateEnv rewrites the golden files instead of comparing against them.
	updateEnv = "STUTTER_UPDATE_GOLDEN"
)

func heldGates() []report.GateCheck {
	return []report.GateCheck{
		{Name: report.GateDeterminism, Result: gate.Result{Class: gate.ClassMatch, Index: -1}},
		{Name: report.GateRekeying, Result: gate.Result{Class: gate.ClassMatch, Index: -1}},
	}
}

func violatedGates() []report.GateCheck {
	return []report.GateCheck{
		{Name: report.GateDeterminism, Result: gate.Result{
			Class: gate.ClassDivergentSet,
			Index: 3,
			Want:  "pg.query UPDATE stock SET qty = <n>",
			Got:   "pg.query DELETE FROM stock",
		}},
		{Name: report.GateRekeying, Result: gate.Result{Class: gate.ClassMatch, Index: -1}},
	}
}

// corruptingStock is the planted-bug finding the concept document's example report opens with.
//
// It carries NO explicit Impact on purpose: a doubled write has to fail on Stutter's own authority,
// because nothing sets an impact until user-declared invariants land and a report that cannot fail
// without a declaration cannot gate CI on a fresh corpus.
func corruptingStock() report.Divergence {
	return report.Divergence{
		Consumer:     consumerStock,
		Fault:        policy.FaultDuplicate,
		Clause:       clauseRedelivery,
		Summary:      "stock decremented twice",
		Repro:        reproStock,
		Clean:        "stock 14 -> 13",
		Mutated:      "stock 14 -> 12",
		DivergedKind: effect.KindPostgres,
		DivergedAt:   2,
	}
}

// unclassifiedReceipt is the concept document's canonical acceptable duplicate: a second email.
func unclassifiedReceipt() report.Divergence {
	return report.Divergence{
		Consumer:     consumerReceipt,
		Fault:        policy.FaultDuplicate,
		Clause:       clauseRedelivery,
		Summary:      "2 outbound emails",
		Repro:        reproReceipt,
		DivergedKind: effect.KindSMTP,
		DivergedAt:   1,
	}
}

// guardedCharge is a corrupting divergence whose handler consulted a stub before it diverged, which
// is the case §5.3 names as the top false-positive source in the design.
func guardedCharge() report.Divergence {
	return report.Divergence{
		Consumer:     consumerCharge,
		Fault:        policy.FaultCrashBeforeAck,
		Clause:       "AckPolicy: explicit with MaxDeliver unlimited — an unacknowledged message is redelivered",
		Summary:      "customer charged twice",
		Repro:        "payment.requested#3120, ack withheld after the side effect",
		Clean:        "1 charge",
		Mutated:      "2 charges",
		DivergedKind: effect.KindPostgres,
		StubReads:    []int{4, 1},
		DivergedAt:   3,
	}
}

func wideScan() report.Scan {
	return report.Scan{Consumers: 40, Messages: 12400}
}

func TestRenderMatchesGolden(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		golden string
		built  report.Report
	}{
		{
			name:   "all consumers pass",
			golden: "all_pass",
			built:  report.New(wideScan(), heldGates(), nil),
		},
		{
			name:   "one failure alongside an unclassified warning",
			golden: "one_fail",
			built: report.New(wideScan(), heldGates(),
				[]report.Divergence{unclassifiedReceipt(), corruptingStock()}),
		},
		{
			name:   "warning only",
			golden: "warn_only",
			built:  report.New(wideScan(), heldGates(), []report.Divergence{unclassifiedReceipt()}),
		},
		{
			name:   "guard-dependent finding",
			golden: "guard_dependent",
			built: report.New(report.Scan{Consumers: 3, Messages: 900}, heldGates(),
				[]report.Divergence{guardedCharge()}),
		},
		{
			name:   "a violated gate withholds every finding",
			golden: "gate_violated",
			built:  report.New(wideScan(), violatedGates(), []report.Divergence{corruptingStock()}),
		},
		{
			name:   "setup never got as far as a run",
			golden: "setup_failed",
			built:  report.SetupFailed(errors.New("start service under test: connection refused")),
		},
		{
			name:   "a declared invariant silences a divergence",
			golden: "silenced",
			built: report.New(report.Scan{Consumers: 5, Messages: 1}, heldGates(), []report.Divergence{
				{Consumer: consumerReceipt, Fault: policy.FaultDuplicate, Impact: report.ImpactAcceptable},
			}),
		},
		{
			name:   "no gates were run, and the report says so",
			golden: "no_gates",
			built:  report.New(report.Scan{Consumers: 2, Messages: 7}, nil, nil),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var out strings.Builder
			if err := testCase.built.Render(&out); err != nil {
				t.Fatalf("Render() error = %v", err)
			}

			assertGolden(t, testCase.golden, out.String())
		})
	}
}

// TestStringMatchesRender guards the convenience accessor against drifting from the writer.
func TestStringMatchesRender(t *testing.T) {
	t.Parallel()

	built := report.New(wideScan(), heldGates(), []report.Divergence{corruptingStock()})

	var out strings.Builder
	if err := built.Render(&out); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if built.String() != out.String() {
		t.Errorf("String() and Render() disagree:\n%q\n%q", built.String(), out.String())
	}
}

// TestExitCodes pins every code in the table. They are load-bearing: a gate violation reported as a
// test failure is one users learn to ignore.
func TestExitCodes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		built report.Report
		want  int
	}{
		{
			name:  "all consumers pass and both gates held",
			built: report.New(wideScan(), heldGates(), nil),
			want:  report.ExitPass,
		},
		{
			name:  "one failure",
			built: report.New(wideScan(), heldGates(), []report.Divergence{corruptingStock()}),
			want:  report.ExitFail,
		},
		{
			name:  "a warning never fails the build",
			built: report.New(wideScan(), heldGates(), []report.Divergence{unclassifiedReceipt()}),
			want:  report.ExitPass,
		},
		{
			name:  "many warnings still never fail the build",
			built: report.New(wideScan(), heldGates(), []report.Divergence{unclassifiedReceipt(), guardedCharge()}),
			want:  report.ExitPass,
		},
		{
			name:  "a violated gate is not a test failure",
			built: report.New(wideScan(), violatedGates(), []report.Divergence{corruptingStock()}),
			want:  report.ExitGateViolated,
		},
		{
			name:  "setup failure outranks everything",
			built: report.SetupFailed(errors.New("no such compose service")),
			want:  report.ExitSetupError,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.built.ExitCode(); got != testCase.want {
				t.Errorf("ExitCode() = %d, want %d", got, testCase.want)
			}
		})
	}
}

// TestExitCodesAreDistinct guards against the four codes collapsing into each other.
func TestExitCodesAreDistinct(t *testing.T) {
	t.Parallel()

	codes := map[string]int{
		"pass":  report.ExitPass,
		"fail":  report.ExitFail,
		"gate":  report.ExitGateViolated,
		"setup": report.ExitSetupError,
	}

	seen := make(map[int]string, len(codes))

	for name, code := range codes {
		if other, clash := seen[code]; clash {
			t.Errorf("%s and %s share exit code %d", name, other, code)
		}

		seen[code] = name
	}
}

// TestGuardDependentMarking covers the §5.3 rule: only a stub read BEFORE the first divergent
// effect can have decided the divergence, so only that lowers confidence.
func TestGuardDependentMarking(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		wantConfidence report.Confidence
		wantStatus     report.Status
		stubReads      []int
		divergedAt     int
	}{
		{
			name:           "no stubbed reads at all",
			stubReads:      nil,
			divergedAt:     3,
			wantConfidence: report.ConfidenceFirm,
			wantStatus:     report.StatusFail,
		},
		{
			name:           "a stubbed read before the divergence",
			stubReads:      []int{1},
			divergedAt:     3,
			wantConfidence: report.ConfidenceGuardDependent,
			wantStatus:     report.StatusWarn,
		},
		{
			name:           "a stubbed read only after the divergence cannot have caused it",
			stubReads:      []int{4, 9},
			divergedAt:     3,
			wantConfidence: report.ConfidenceFirm,
			wantStatus:     report.StatusFail,
		},
		{
			name:           "a stubbed read at the divergence itself is not before it",
			stubReads:      []int{3},
			divergedAt:     3,
			wantConfidence: report.ConfidenceFirm,
			wantStatus:     report.StatusFail,
		},
		{
			name:           "divergence at the first effect leaves nothing before it",
			stubReads:      []int{0, 1},
			divergedAt:     0,
			wantConfidence: report.ConfidenceFirm,
			wantStatus:     report.StatusFail,
		},
		{
			name:           "an unknown divergence position counts every stubbed read",
			stubReads:      []int{7},
			divergedAt:     -1,
			wantConfidence: report.ConfidenceGuardDependent,
			wantStatus:     report.StatusWarn,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			divergence := corruptingStock()
			divergence.StubReads = testCase.stubReads
			divergence.DivergedAt = testCase.divergedAt

			finding := onlyFinding(t, report.New(wideScan(), heldGates(), []report.Divergence{divergence}))

			if finding.Confidence != testCase.wantConfidence {
				t.Errorf("Confidence = %q, want %q", finding.Confidence, testCase.wantConfidence)
			}

			if finding.Status != testCase.wantStatus {
				t.Errorf("Status = %q, want %q (reservations: %v)",
					finding.Status, testCase.wantStatus, finding.Reservations)
			}
		})
	}
}

// TestGuardDependentNamesTheEarliestRead checks the note points at the read that could have decided
// the divergence, not at whichever one happened to be listed first.
func TestGuardDependentNamesTheEarliestRead(t *testing.T) {
	t.Parallel()

	divergence := corruptingStock()
	divergence.StubReads = []int{2, 0, 5}
	divergence.DivergedAt = 4

	finding := onlyFinding(t, report.New(wideScan(), heldGates(), []report.Divergence{divergence}))

	const want = "stubbed dependency at effect 0, before its first divergent effect at 4"

	joined := strings.Join(finding.Reservations, "\n")
	if !strings.Contains(joined, want) {
		t.Errorf("reservations = %q, want them to contain %q", joined, want)
	}
}

// TestReservationsHoldFindingsBack covers everything that biases a divergence towards WARN. One
// false positive voids trust in every other line, so each of these is a deliberate under-report.
func TestReservationsHoldFindingsBack(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		mutate   func(*report.Divergence)
		wantNote string
	}{
		{
			name:     "no minimal repro",
			mutate:   func(d *report.Divergence) { d.Repro = "" },
			wantNote: "no minimal repro was found",
		},
		{
			name:     "no configuration clause",
			mutate:   func(d *report.Divergence) { d.Clause = "" },
			wantNote: "cannot be traced back to your stream config",
		},
		{
			name:     "the diverging effect was a mail submission",
			mutate:   func(d *report.Divergence) { d.DivergedKind = effect.KindSMTP },
			wantNote: "annoying rather than corrupting",
		},
		{
			name:     "the diverging effect was an outbound call Stutter cannot weigh",
			mutate:   func(d *report.Divergence) { d.DivergedKind = effect.KindHTTP },
			wantNote: "may be a charge or may be harmless",
		},
		{
			name:     "the diverging effect was a published message",
			mutate:   func(d *report.Divergence) { d.DivergedKind = effect.KindNATS },
			wantNote: "may be a charge or may be harmless",
		},
		{
			name:     "the diverging effect was on an unparsed protocol",
			mutate:   func(d *report.Divergence) { d.DivergedKind = effect.KindOpaque },
			wantNote: "not actionable as it stands",
		},
		{
			name:     "the diverging protocol is not recorded at all",
			mutate:   func(d *report.Divergence) { d.DivergedKind = "" },
			wantNote: "declare an invariant to silence",
		},
		{
			name:     "the handler read a stub before diverging",
			mutate:   func(d *report.Divergence) { d.StubReads = []int{0} },
			wantNote: "guard-dependent",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			divergence := corruptingStock()
			testCase.mutate(&divergence)

			built := report.New(wideScan(), heldGates(), []report.Divergence{divergence})
			finding := onlyFinding(t, built)

			if finding.Status != report.StatusWarn {
				t.Errorf("Status = %q, want %q", finding.Status, report.StatusWarn)
			}

			if built.ExitCode() != report.ExitPass {
				t.Errorf("ExitCode() = %d, want %d — a warning must never fail the build",
					built.ExitCode(), report.ExitPass)
			}

			joined := strings.Join(finding.Reservations, "\n")
			if !strings.Contains(joined, testCase.wantNote) {
				t.Errorf("reservations = %q, want them to contain %q", joined, testCase.wantNote)
			}

			if !strings.Contains(built.String(), testCase.wantNote) {
				t.Errorf("rendered report does not state why it warned:\n%s", built.String())
			}
		})
	}
}

// TestDivergentWriteFailsWithoutAnyDeclaration is the regression this package was shipped without.
//
// Reporting every unclassified divergence as a warning made exit code 1 unreachable: nothing sets
// an impact until user-declared invariants land, so the tool could never fail CI on the very bug
// class it exists to find. A doubled write now fails on Stutter's own authority.
func TestDivergentWriteFailsWithoutAnyDeclaration(t *testing.T) {
	t.Parallel()

	divergence := corruptingStock()
	if divergence.Impact != report.ImpactUnclassified {
		t.Fatalf("fixture declares an impact (%q); the point of this test is that none is needed",
			divergence.Impact)
	}

	built := report.New(wideScan(), heldGates(), []report.Divergence{divergence})

	finding := onlyFinding(t, built)
	if finding.Status != report.StatusFail {
		t.Errorf("Status = %q, want %q (reservations: %v)",
			finding.Status, report.StatusFail, finding.Reservations)
	}

	if len(finding.Reservations) != 0 {
		t.Errorf("Reservations = %v, want none", finding.Reservations)
	}

	if got := built.ExitCode(); got != report.ExitFail {
		t.Errorf("ExitCode() = %d, want %d", got, report.ExitFail)
	}
}

// TestDefaultImpactFollowsTheDivergingProtocol pins the whole table, including the deliberate
// asymmetry: only a datastore write is called corrupting without a declaration.
func TestDefaultImpactFollowsTheDivergingProtocol(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		kind effect.Kind
		want report.Status
	}{
		{name: "a divergent write sequence is wrong data", kind: effect.KindPostgres, want: report.StatusFail},
		{name: "a second mail submission is annoying", kind: effect.KindSMTP, want: report.StatusWarn},
		{name: "an outbound call could be either", kind: effect.KindHTTP, want: report.StatusWarn},
		{name: "a published message could be either", kind: effect.KindNATS, want: report.StatusWarn},
		{name: "an unparsed protocol cannot be described", kind: effect.KindOpaque, want: report.StatusWarn},
		{name: "an unrecorded protocol decides nothing", kind: "", want: report.StatusWarn},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			divergence := corruptingStock()
			divergence.DivergedKind = testCase.kind

			finding := onlyFinding(t, report.New(wideScan(), heldGates(), []report.Divergence{divergence}))
			if finding.Status != testCase.want {
				t.Errorf("Status = %q, want %q (reservations: %v)",
					finding.Status, testCase.want, finding.Reservations)
			}
		})
	}
}

// TestExplicitImpactOverridesTheProtocolDefault is how a declared invariant will promote or silence
// a divergence once invariants land.
func TestExplicitImpactOverridesTheProtocolDefault(t *testing.T) {
	t.Parallel()

	promoted := unclassifiedReceipt()
	promoted.Impact = report.ImpactCorrupting

	built := report.New(wideScan(), heldGates(), []report.Divergence{promoted})
	if finding := onlyFinding(t, built); finding.Status != report.StatusFail {
		t.Errorf("promoted mail divergence: Status = %q, want %q", finding.Status, report.StatusFail)
	}

	silenced := corruptingStock()
	silenced.Impact = report.ImpactAcceptable

	quiet := report.New(wideScan(), heldGates(), []report.Divergence{silenced})
	if len(quiet.Findings) != 0 {
		t.Errorf("silenced write divergence: Findings = %v, want none", quiet.Findings)
	}

	if got := quiet.ExitCode(); got != report.ExitPass {
		t.Errorf("silenced write divergence: ExitCode() = %d, want %d", got, report.ExitPass)
	}
}

// TestGuardDependenceOutranksTheProtocolDefault keeps the top false-positive source from failing CI
// on the strength of a frozen stub reply.
func TestGuardDependenceOutranksTheProtocolDefault(t *testing.T) {
	t.Parallel()

	built := report.New(wideScan(), heldGates(), []report.Divergence{guardedCharge()})

	finding := onlyFinding(t, built)
	if finding.Status != report.StatusWarn {
		t.Errorf("Status = %q, want %q", finding.Status, report.StatusWarn)
	}

	if finding.Confidence != report.ConfidenceGuardDependent {
		t.Errorf("Confidence = %q, want %q", finding.Confidence, report.ConfidenceGuardDependent)
	}

	if got := built.ExitCode(); got != report.ExitPass {
		t.Errorf("ExitCode() = %d, want %d", got, report.ExitPass)
	}
}

// TestGuardDependentWarningsReadDifferently guards the distinction the report has to make: "could
// not tell" must not skim the same as "probably fine", or it gets ignored alongside it.
func TestGuardDependentWarningsReadDifferently(t *testing.T) {
	t.Parallel()

	mixed := report.New(wideScan(), heldGates(),
		[]report.Divergence{guardedCharge(), unclassifiedReceipt()}).String()

	for _, want := range []string{
		"[guard-dependent]",
		"1 of 2 warnings is guard-dependent",
		"Stutter could not decide",
	} {
		if !strings.Contains(mixed, want) {
			t.Errorf("rendered report is missing %q:\n%s", want, mixed)
		}
	}

	plain := report.New(wideScan(), heldGates(), []report.Divergence{unclassifiedReceipt()}).String()
	for _, absent := range []string{"[guard-dependent]", "guard-dependent:", "NOTE"} {
		if strings.Contains(plain, absent) {
			t.Errorf("a plain warning was marked with %q:\n%s", absent, plain)
		}
	}
}

// TestFailingFindingCarriesItsClause pins §5.2's rule: a finding a user cannot trace back to their
// own stream config is a finding they will not act on.
func TestFailingFindingCarriesItsClause(t *testing.T) {
	t.Parallel()

	built := report.New(wideScan(), heldGates(), []report.Divergence{corruptingStock()})

	finding := onlyFinding(t, built)
	if finding.Clause != clauseRedelivery {
		t.Errorf("Clause = %q, want %q", finding.Clause, clauseRedelivery)
	}

	if len(finding.Reservations) != 0 {
		t.Errorf("Reservations = %v, want none on a failure", finding.Reservations)
	}

	if !strings.Contains(built.String(), "legal because: "+clauseRedelivery) {
		t.Errorf("rendered report does not name the clause:\n%s", built.String())
	}
}

// TestViolatedGateComputesNoFindings covers §7: the report is withheld, not qualified.
func TestViolatedGateComputesNoFindings(t *testing.T) {
	t.Parallel()

	built := report.New(wideScan(), violatedGates(), []report.Divergence{corruptingStock(), guardedCharge()})

	if len(built.Findings) != 0 {
		t.Errorf("Findings = %v, want none while a gate is violated", built.Findings)
	}

	rendered := built.String()

	for _, absent := range []string{consumerStock, consumerCharge, "FAIL", "WARN"} {
		if strings.Contains(rendered, absent) {
			t.Errorf("rendered report contains %q despite a violated gate:\n%s", absent, rendered)
		}
	}

	for _, present := range []string{
		string(report.GateDeterminism),
		"violated",
		"No findings were computed",
		"not a test failure",
		"the service under test is not deterministic",
	} {
		if !strings.Contains(rendered, present) {
			t.Errorf("rendered report is missing %q:\n%s", present, rendered)
		}
	}
}

// TestViolatedGateNamesOnlyTheGateThatFailed keeps a held gate from being reported as violated
// alongside the one that was.
func TestViolatedGateNamesOnlyTheGateThatFailed(t *testing.T) {
	t.Parallel()

	built := report.New(wideScan(), violatedGates(), nil)

	violations := built.Violations()
	if len(violations) != 1 {
		t.Fatalf("Violations() = %v, want exactly one", violations)
	}

	if violations[0].Name != report.GateDeterminism {
		t.Errorf("Violations()[0].Name = %q, want %q", violations[0].Name, report.GateDeterminism)
	}
}

// TestFindingsAreOrderedDeterministically keeps report output diffable between CI runs.
func TestFindingsAreOrderedDeterministically(t *testing.T) {
	t.Parallel()

	receipt := unclassifiedReceipt()
	stock := corruptingStock()

	second := stock
	second.Consumer = "apply_discount"
	second.Fault = policy.FaultReorder

	built := report.New(wideScan(), heldGates(), []report.Divergence{receipt, stock, second})

	got := make([]string, len(built.Findings))
	for index, finding := range built.Findings {
		got[index] = string(finding.Status) + " " + finding.Consumer
	}

	want := []string{"FAIL apply_discount", "FAIL " + consumerStock, "WARN " + consumerReceipt}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("finding order = %v, want %v", got, want)
	}
}

// TestSilencedDivergencesAreCountedNotHidden stops a run that silenced everything from reading as a
// clean one.
func TestSilencedDivergencesAreCountedNotHidden(t *testing.T) {
	t.Parallel()

	silenced := corruptingStock()
	silenced.Impact = report.ImpactAcceptable

	built := report.New(wideScan(), heldGates(), []report.Divergence{silenced})

	if built.Silenced != 1 {
		t.Errorf("Silenced = %d, want 1", built.Silenced)
	}

	if len(built.Findings) != 0 {
		t.Errorf("Findings = %v, want none", built.Findings)
	}

	if !strings.Contains(built.String(), "silenced by a declared invariant") {
		t.Errorf("rendered report hides the silenced divergence:\n%s", built.String())
	}
}

// TestRenderPropagatesWriteErrors keeps a broken pipe from reading as a rendered report.
func TestRenderPropagatesWriteErrors(t *testing.T) {
	t.Parallel()

	built := report.New(wideScan(), heldGates(), nil)

	if err := built.Render(failingWriter{}); err == nil {
		t.Error("Render() to a failing writer returned nil")
	}
}

// TestRenderedLinesCarryNoTrailingWhitespace keeps column padding out of the golden files, where it
// would be invisible and get stripped by an editor.
func TestRenderedLinesCarryNoTrailingWhitespace(t *testing.T) {
	t.Parallel()

	built := report.New(wideScan(), heldGates(),
		[]report.Divergence{corruptingStock(), unclassifiedReceipt(), guardedCharge()})

	for index, line := range strings.Split(built.String(), "\n") {
		if line != strings.TrimRight(line, " \t") {
			t.Errorf("line %d has trailing whitespace: %q", index+1, line)
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errWriteFailed
}

var errWriteFailed = errors.New("write failed")

func onlyFinding(t *testing.T, built report.Report) report.Finding {
	t.Helper()

	if len(built.Findings) != 1 {
		t.Fatalf("Findings = %v, want exactly one", built.Findings)
	}

	return built.Findings[0]
}

func assertGolden(t *testing.T, name, got string) {
	t.Helper()

	path := filepath.Join("testdata", name+".golden")

	if os.Getenv(updateEnv) != "" {
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}

		return
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (re-run with %s=1 to create it)", path, err, updateEnv)
	}

	want := string(raw)
	if got == want {
		return
	}

	t.Errorf("Render() differs from %s at %s\n--- got ---\n%s--- want ---\n%s",
		path, firstDifference(got, want), got, want)
}

// firstDifference names the line the two renderings part company on, because a whole-report diff
// buries a one-character change.
func firstDifference(got, want string) string {
	gotLines := strings.Split(got, "\n")
	wantLines := strings.Split(want, "\n")

	for index := range max(len(gotLines), len(wantLines)) {
		gotLine := lineAt(gotLines, index)

		wantLine := lineAt(wantLines, index)
		if gotLine != wantLine {
			return "line " + strconv.Itoa(index+1) + ":\n  got:  " + gotLine + "\n  want: " + wantLine
		}
	}

	return "no line differs (trailing bytes only)"
}

func lineAt(lines []string, index int) string {
	if index >= len(lines) {
		return "<missing>"
	}

	return lines[index]
}
