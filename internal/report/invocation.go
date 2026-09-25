package report

import (
	"slices"

	"github.com/Wintersta7e/stutter/internal/gate"
)

// Outcome is how a discovered consumer's part in a compose check ended, before its report is read.
type Outcome string

const (
	// OutcomeChecked means the consumer got a consumer check; its report decides the rest.
	OutcomeChecked Outcome = "checked"
	// OutcomeNotCovered means the consumer could not be checked: no corpus message reaches it, it
	// reads another stream, or its name does not survive a start.
	OutcomeNotCovered Outcome = "not-covered"
	// OutcomeNotSelected means --consumer left the consumer out.
	OutcomeNotSelected Outcome = "not-selected"
	// OutcomeSetup means the consumer's check could not be carried out.
	OutcomeSetup Outcome = "setup"
)

// Bucket is where one consumer lands in a compose report. Every discovered consumer lands in exactly
// one; the first that applies wins, in the order below.
type Bucket string

const (
	// BucketNotSelected is a consumer --consumer left out.
	BucketNotSelected Bucket = "not-selected"
	// BucketNotCovered is a consumer that could not be checked.
	BucketNotCovered Bucket = "not-covered"
	// BucketSetup is a consumer whose check could not be carried out.
	BucketSetup Bucket = "setup"
	// BucketGate is a consumer whose check violated a gate.
	BucketGate Bucket = "gate"
	// BucketFail is a consumer with a finding nothing holds back.
	BucketFail Bucket = "fail"
	// BucketWarn is a consumer whose findings are all held back.
	BucketWarn Bucket = "warn"
	// BucketHeld is a consumer whose gates held with no fault injected: gate mode, or nothing legal and
	// expressible. Never a pass, which claims faults were survived.
	BucketHeld Bucket = "held"
	// BucketPass is a consumer that survived at least one fault with nothing found.
	BucketPass Bucket = "pass"
)

// ConsumerCheck is one discovered consumer's part in a compose check.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type ConsumerCheck struct {
	// Name is the consumer's name; empty for the unnamed check a service with no consumer gets.
	Name string
	// Reason says why a consumer was not checked, or which step its check failed at.
	Reason string
	// Outcome is how the consumer's part ended.
	Outcome Outcome
	// Report is the consumer check's own report; zero unless it was checked or its check failed.
	Report Report
	// Admitted is how many corpus messages the consumer's filters admit.
	Admitted int
}

// Bucket is the bucket the consumer lands in.
func (c ConsumerCheck) Bucket() Bucket {
	switch {
	case c.Outcome == OutcomeNotSelected:
		return BucketNotSelected
	case c.Outcome == OutcomeNotCovered:
		return BucketNotCovered
	case c.Outcome == OutcomeSetup, c.Report.Setup != nil:
		return BucketSetup
	case !gatesHeld(c.Report.Gates):
		return BucketGate
	case c.Report.failed():
		return BucketFail
	case len(c.Report.Findings) > 0:
		return BucketWarn
	case c.Report.GatesOnly || !c.Report.attempted():
		return BucketHeld
	default:
		return BucketPass
	}
}

// instability is the class of determinism violation that makes this consumer's service suspect for
// every other consumer too, when it has one. Different work or different values between two clean
// runs, or a divergence that did not come back, can appear under any consumer of the same service;
// the same work in another order, or no work at all, cannot manufacture a divergence elsewhere.
func (c ConsumerCheck) instability() (gate.Class, bool) {
	for _, violated := range c.Report.Violations() {
		if violated.Name != GateDeterminism {
			continue
		}

		switch violated.Result.Class { //nolint:exhaustive // only these three reach a sibling.
		case gate.ClassDivergentSet, gate.ClassFieldDrift, gate.ClassNotReproducible:
			return violated.Result.Class, true
		}
	}

	return "", false
}

// heldBy holds every FAIL finding back to WARN for each unstable sibling, naming it. Nothing is
// discarded, and the report the check produced is left untouched.
func (c ConsumerCheck) heldBy(siblings []ConsumerCheck) ConsumerCheck {
	var reservations []string

	for _, sibling := range siblings {
		if class, unstable := sibling.instability(); unstable && sibling.Name != c.Name {
			reservations = append(reservations, siblingReservation(sibling.Name, class))
		}
	}

	if len(reservations) == 0 || !c.Report.failed() {
		return c
	}

	c.Report.Findings = slices.Clone(c.Report.Findings)

	for index, finding := range c.Report.Findings {
		if finding.Status != StatusFail {
			continue
		}

		finding.Status = StatusWarn
		finding.Reservations = append(slices.Clone(finding.Reservations), reservations...)
		c.Report.Findings[index] = finding
	}

	order(c.Report.Findings)

	return c
}

// Invocation is a whole compose check: every discovered consumer's part in it, and what stopped it
// when it could not be carried out at all.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Invocation struct {
	// Setup is non-nil when the check could not be carried out as a whole: before the first consumer
	// check, or a change that withholds every verdict. Nothing else here is then a verdict.
	Setup error
	// Unnamed is the one check a service with no consumer gets; never counted as a discovered consumer.
	Unnamed *ConsumerCheck
	// Consumers are the discovered consumers, one each, in name order.
	Consumers []ConsumerCheck
}

// Judged returns every consumer check as the verdict reads it: an unstable consumer holds each other
// consumer's FAIL to WARN. Applied here, after every check ended, because the instability may be
// found after the finding it undermines. The exit code and the rendered report both read this.
func (i Invocation) Judged() []ConsumerCheck {
	judged := make([]ConsumerCheck, len(i.Consumers))

	for index, consumer := range i.Consumers {
		judged[index] = consumer.heldBy(i.Consumers)
	}

	return judged
}

// ExitCode is the process exit status for the whole check. FAIL leads, so another consumer's setup
// error never masks a reproduced finding; a check that judged no consumer at all is a setup error.
func (i Invocation) ExitCode() int {
	if i.Setup != nil {
		return ExitSetupError
	}

	counts := i.counts()

	switch {
	case counts[BucketFail] > 0:
		return ExitFail
	case counts[BucketSetup] > 0:
		return ExitSetupError
	case counts[BucketGate] > 0:
		return ExitGateViolated
	case counts[BucketWarn]+counts[BucketHeld]+counts[BucketPass] == 0:
		return ExitSetupError
	default:
		return ExitPass
	}
}

// counts is how many judged checks landed in each bucket, the unnamed check included.
func (i Invocation) counts() map[Bucket]int {
	counts := make(map[Bucket]int)

	for _, consumer := range i.Judged() {
		counts[consumer.Bucket()]++
	}

	if i.Unnamed != nil {
		counts[i.Unnamed.Bucket()]++
	}

	return counts
}

// attempted reports whether any fault was injected.
func (r Report) attempted() bool {
	return slices.ContainsFunc(r.Coverage, func(c Coverage) bool { return c.Attempted > 0 })
}
