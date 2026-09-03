// Package report turns a completed run into the text a developer acts on and the exit code CI
// gates on.
//
// Two rules shape everything here. First, one false positive costs more than a missed finding: it
// voids trust in every other line, so a divergence is only ever reported as a failure when nothing
// at all is holding it back, and every reason it was held back is printed beside it. Second, a gate
// violation is not a test failure — it means no findings were computed, and conflating the two
// trains users to ignore it.
//
// Deciding and rendering are separate on purpose: ExitCode is computable, and testable, without
// producing a byte of text.
package report

import (
	"cmp"
	"slices"
	"strconv"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/policy"
)

// Exit codes. They are load-bearing and must not be conflated: a gate violation reported as a test
// failure is one a user learns to ignore, and the gates are the only thing standing between the
// report and noise.
const (
	// ExitPass means every consumer passed and every gate that ran held.
	ExitPass = 0
	// ExitFail means at least one consumer produced a finding Stutter is willing to stand behind.
	ExitFail = 1
	// ExitGateViolated means a gate was violated, so no findings were computed. NOT a test failure.
	ExitGateViolated = 2
	// ExitSetupError means the run never started: provisioning, the corpus, or the bus.
	ExitSetupError = 3
)

// Reservations that hold a divergence back from being reported as a failure.
const (
	// guardOverrideNote names the way out of a guard-dependent verdict, so the marking is a next
	// step rather than a shrug.
	guardOverrideNote = "a frozen reply may have decided this — " +
		"override that endpoint's stub to get a firm verdict"
	// missingReproNote states the design invariant that a finding without a minimal repro is not a
	// finding.
	missingReproNote = "no minimal repro was found, so this is not yet actionable"
	// missingClauseNote fires when the configuration clause was not recorded. A finding a user
	// cannot trace back to their own stream config is a finding they will not act on.
	missingClauseNote = "the configuration clause that made this fault legal was not recorded, " +
		"so it cannot be traced back to your stream config"
	// mailDivergenceNote covers the canonical acceptable duplicate: a second receipt.
	mailDivergenceNote = "a repeated mail submission is annoying rather than corrupting — " +
		"declare an invariant to silence it, or to promote it"
	// unjudgeableEgressNote covers outbound calls Stutter can see but cannot weigh. A repeated POST
	// is a double charge or a cache warm, and nothing in the effect record says which.
	unjudgeableEgressNote = "a repeated outbound call may be a charge or may be harmless and " +
		"Stutter cannot tell which — declare an invariant to promote or silence it"
	// opaqueDivergenceNote covers a divergence on an unparsed protocol. It is real divergence, but
	// the report can only say that it happened, never what it did.
	opaqueDivergenceNote = "the diverging effect is on a protocol Stutter does not parse, so this " +
		"names a difference it cannot describe and is not actionable as it stands"
	// unclassifiedNote is the fallback when even the protocol is unknown.
	unclassifiedNote = "may be acceptable — declare an invariant to silence"
)

// Sort ranks, so findings order the same way on every run and can be diffed between CI runs.
const (
	rankFail = iota
	rankWarn
	rankPass
	rankUnknown
)

// Status is the verdict rendered against one consumer.
type Status string

const (
	// StatusPass means nothing was found against the consumer.
	StatusPass Status = "PASS"
	// StatusWarn means a divergence was observed that Stutter will not stand behind as a failure.
	// It never affects the exit code.
	StatusWarn Status = "WARN"
	// StatusFail means a divergence was observed with nothing holding it back. It sets exit code 1.
	StatusFail Status = "FAIL"
)

// Confidence qualifies how far a finding can be trusted.
type Confidence string

const (
	// ConfidenceFirm means nothing in the run undermines the comparison behind this finding.
	ConfidenceFirm Confidence = "firm"
	// ConfidenceGuardDependent means the handler read a stubbed dependency before it diverged, so a
	// frozen reply may have manufactured the divergence, or hidden one.
	ConfidenceGuardDependent Confidence = "guard-dependent"
)

// Impact is what a divergence costs.
//
// An explicit value always wins, and is how a declared invariant will later promote or silence a
// divergence. Left unset, it is derived from the protocol that diverged: see defaultImpact.
type Impact string

const (
	// ImpactUnclassified means nobody has set an impact, so the diverging protocol decides.
	ImpactUnclassified Impact = ""
	// ImpactCorrupting means the divergence leaves the system holding wrong data. It fails.
	ImpactCorrupting Impact = "corrupting"
	// ImpactAcceptable means a declared invariant says this divergence is fine. It is silenced and
	// counted, never rendered as a finding.
	ImpactAcceptable Impact = "acceptable"
)

// Gate names one of the two checks that must hold before any finding is computed.
type Gate string

const (
	// GateDeterminism is two unmutated clean runs compared against each other.
	GateDeterminism Gate = "determinism"
	// GateRekeying is two clean runs over corpora pseudonymised under different keys.
	GateRekeying Gate = "differential re-keying"
)

// GateCheck is one gate's outcome.
type GateCheck struct {
	// Name identifies which gate ran.
	Name Gate
	// Result is what the comparison found, and how it classified any failure.
	Result gate.Result
}

// Scan is how much ground the run covered.
type Scan struct {
	// Consumers is how many consumers were replayed.
	Consumers int
	// Messages is how many recorded messages were replayed into them.
	Messages int
}

// Divergence is one mutated run that behaved differently from the reference run.
//
// It is the raw material of a finding, not a finding: this package rules on it.
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Divergence struct {
	// Consumer is the NATS consumer the finding attributes to.
	Consumer string
	// Fault is the delivery fault that provoked the divergence.
	Fault policy.Fault
	// Clause is the configuration that made the fault legal, from policy.Verdict.Clause by way of
	// replay.Result.Clause. Its absence holds the finding back from FAIL.
	Clause string
	// Summary is what the divergence did, in the user's own terms: "stock decremented twice".
	Summary string
	// Repro is the minimal message sequence that reproduces it.
	Repro string
	// Clean is the reference run's observable outcome, rendered for a human.
	Clean string
	// Mutated is the same outcome under the fault.
	Mutated string
	// Impact classifies what this divergence costs. Setting it overrides the protocol default.
	Impact Impact
	// DivergedKind is the protocol of the first effect that differed, from effect.Effect.Kind at
	// DivergedAt. It supplies the default impact when nobody has classified the divergence, and is
	// separated from DivergedAt here only to satisfy fieldalignment.
	DivergedKind effect.Kind
	// StubReads are the ordinals, within the mutated run's effect sequence, of effects answered
	// from a frozen stub rather than by a real dependency. They decide guard-dependence.
	StubReads []int
	// DivergedAt is the ordinal of the first effect that differed from the reference run, which is
	// gate.Result.Index from the comparison that found it. Negative means the position is unknown.
	DivergedAt int
}

// Finding is a divergence this package has ruled on.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Finding struct {
	// Consumer is the NATS consumer the finding attributes to.
	Consumer string
	// Fault is the delivery fault that provoked the divergence.
	Fault policy.Fault
	// Clause is the configuration that made the fault legal.
	Clause string
	// Summary is what the divergence did.
	Summary string
	// Repro is the minimal message sequence that reproduces it.
	Repro string
	// Clean is the reference run's observable outcome.
	Clean string
	// Mutated is the same outcome under the fault.
	Mutated string
	// Status is the verdict. Only StatusFail moves the exit code.
	Status Status
	// Confidence records whether a stubbed dependency could have decided this.
	Confidence Confidence
	// Reservations are every reason this finding warns rather than fails, in the order they are
	// rendered. Empty on a FAIL.
	Reservations []string
}

// Report is a completed run, ready to render and to exit on.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Report struct {
	// Setup is non-nil when the run never started, in which case nothing else here is meaningful.
	Setup error
	// Gates are the checks that ran. A report naming no gates claims none ran.
	Gates []GateCheck
	// Findings are the divergences that were ruled on. Always empty when a gate was violated.
	Findings []Finding
	// Scan is how much ground the run covered.
	Scan Scan
	// Silenced counts divergences a declared invariant classified as acceptable. Counted rather
	// than dropped silently, so a run that silenced everything does not read as a clean one.
	Silenced int
}

// New rules on a completed run.
//
// Divergences are discarded outright when any gate was violated. Rendering them with a caveat would
// still put a comparison the gates just declared meaningless in front of a user, which is the one
// thing §7 forbids.
func New(scan Scan, gates []GateCheck, divergences []Divergence) Report {
	built := Report{Scan: scan, Gates: gates}

	if !gatesHeld(gates) {
		return built
	}

	for _, divergence := range divergences {
		if divergence.effectiveImpact() == ImpactAcceptable {
			built.Silenced++

			continue
		}

		built.Findings = append(built.Findings, divergence.rule())
	}

	order(built.Findings)

	return built
}

// SetupFailed reports a run that never started.
//
// It is its own exit code because provisioning falling over says nothing about the consumers under
// test, and must not be read as either a pass or a finding.
func SetupFailed(err error) Report {
	return Report{Setup: err}
}

// ExitCode is the process exit status for this run.
//
// The order matters: a setup error means nothing ran, a violated gate means nothing was computed,
// and only then can a finding speak for the consumers under test.
func (r Report) ExitCode() int {
	switch {
	case r.Setup != nil:
		return ExitSetupError
	case !gatesHeld(r.Gates):
		return ExitGateViolated
	case r.failed():
		return ExitFail
	default:
		return ExitPass
	}
}

// Violations returns the gates that did not hold, in the order they were supplied.
func (r Report) Violations() []GateCheck {
	var violated []GateCheck

	for _, check := range r.Gates {
		if !check.Result.OK() {
			violated = append(violated, check)
		}
	}

	return violated
}

// failed reports whether any finding is one Stutter stands behind. WARN never counts.
func (r Report) failed() bool {
	return slices.ContainsFunc(r.Findings, func(f Finding) bool { return f.Status == StatusFail })
}

// gatesHeld reports whether every gate that ran held. A report with no gates makes no claim either
// way, and renders that absence rather than implying the gates passed.
func gatesHeld(gates []GateCheck) bool {
	return !slices.ContainsFunc(gates, func(c GateCheck) bool { return !c.Result.OK() })
}

// rule decides one divergence's verdict.
//
// A divergence fails exactly when nothing is holding it back. Every reason it was held back is kept
// and rendered, because a warning whose reason is invisible is a warning nobody can act on.
func (d Divergence) rule() Finding {
	ruled := Finding{
		Reservations: d.reservations(),
		Consumer:     d.Consumer,
		Fault:        d.Fault,
		Clause:       d.Clause,
		Summary:      d.Summary,
		Repro:        d.Repro,
		Clean:        d.Clean,
		Mutated:      d.Mutated,
		Status:       StatusFail,
		Confidence:   ConfidenceFirm,
	}

	if _, guarded := d.stubReadBeforeDivergence(); guarded {
		ruled.Confidence = ConfidenceGuardDependent
	}

	if len(ruled.Reservations) > 0 {
		ruled.Status = StatusWarn
	}

	return ruled
}

// reservations lists every reason this divergence warns rather than fails.
func (d Divergence) reservations() []string {
	var notes []string

	if at, guarded := d.stubReadBeforeDivergence(); guarded {
		notes = append(notes, d.guardNote(at), guardOverrideNote)
	}

	if d.Repro == "" {
		notes = append(notes, missingReproNote)
	}

	if d.Clause == "" {
		notes = append(notes, missingClauseNote)
	}

	if d.effectiveImpact() != ImpactCorrupting {
		notes = append(notes, d.unclassifiedReason())
	}

	return notes
}

// effectiveImpact is the classification this divergence is ruled under.
//
// An explicit Impact always wins: that is how a declared invariant promotes or silences one.
func (d Divergence) effectiveImpact() Impact {
	if d.Impact != ImpactUnclassified {
		return d.Impact
	}

	derived, _ := d.defaultImpact()

	return derived
}

// unclassifiedReason explains, in terms of the protocol that diverged, why this is not a failure.
func (d Divergence) unclassifiedReason() string {
	if _, note := d.defaultImpact(); note != "" {
		return note
	}

	return unclassifiedNote
}

// defaultImpact derives an impact, and the reservation that goes with it, from the protocol the
// divergence appeared on.
//
// This is what lets a first report fail CI with ZERO written declaration, which the concept
// document requires and which a "warn until the user classifies it" rule silently makes impossible:
// nothing sets an impact until user-declared invariants land, so every finding would warn and the
// exit code could never leave zero.
//
// The split is deliberately lopsided. Only a divergent write sequence is called corrupting on
// Stutter's own authority, because read-modify-write against a datastore is the entire bug class the
// tool exists to find and a doubled write is wrong data by construction. Every other protocol warns
// and says what it could not tell, because a repeated outbound call is a double charge or a cache
// warm and the effect record does not distinguish them.
func (d Divergence) defaultImpact() (Impact, string) {
	switch d.DivergedKind {
	case effect.KindPostgres:
		return ImpactCorrupting, ""
	case effect.KindSMTP:
		return ImpactUnclassified, mailDivergenceNote
	case effect.KindHTTP, effect.KindNATS:
		return ImpactUnclassified, unjudgeableEgressNote
	case effect.KindOpaque:
		return ImpactUnclassified, opaqueDivergenceNote
	default:
		return ImpactUnclassified, unclassifiedNote
	}
}

// stubReadBeforeDivergence returns the earliest stubbed read that preceded the first divergent
// effect, and whether there was one.
//
// This is the guard-dependent test, and it is the top false-positive source in the design: a
// handler whose own idempotency guard reads a dependency gets a frozen answer from the stub, so
// Stutter can report a double charge the real guard would have prevented. A read AFTER the
// divergence cannot have decided it, which is why the position matters rather than the mere
// presence of a stub. A negative DivergedAt means the position is unknown and every stubbed read
// then counts: under-stating confidence is the safe direction.
func (d Divergence) stubReadBeforeDivergence() (int, bool) {
	first, found := 0, false

	for _, at := range d.StubReads {
		if d.DivergedAt >= 0 && at >= d.DivergedAt {
			continue
		}

		if !found || at < first {
			first, found = at, true
		}
	}

	return first, found
}

// guardNote states the guard-dependence in terms of the positions that established it.
func (d Divergence) guardNote(at int) string {
	if d.DivergedAt < 0 {
		return "guard-dependent: the handler read a stubbed dependency at effect " +
			strconv.Itoa(at) + " and the divergence position is unknown, " +
			"so the stub cannot be ruled out"
	}

	return "guard-dependent: the handler read a stubbed dependency at effect " +
		strconv.Itoa(at) + ", before its first divergent effect at " + strconv.Itoa(d.DivergedAt)
}

// order sorts findings so failures lead and the rest is deterministic. A report whose line order
// depends on scheduling cannot be diffed between two CI runs.
func order(findings []Finding) {
	slices.SortStableFunc(findings, func(a, b Finding) int {
		if diff := cmp.Compare(rank(a.Status), rank(b.Status)); diff != 0 {
			return diff
		}

		if diff := cmp.Compare(a.Consumer, b.Consumer); diff != 0 {
			return diff
		}

		return cmp.Compare(string(a.Fault), string(b.Fault))
	})
}

func rank(status Status) int {
	switch status {
	case StatusFail:
		return rankFail
	case StatusWarn:
		return rankWarn
	case StatusPass:
		return rankPass
	default:
		return rankUnknown
	}
}
