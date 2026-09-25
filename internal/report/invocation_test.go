package report_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/report"
)

// attempted is the coverage of a consumer check that injected one fault.
func attempted() []report.Coverage {
	return []report.Coverage{{Fault: policy.FaultDuplicate, Pairs: 1, Attempted: 1, Legal: true}}
}

func checked(name string, built report.Report) report.ConsumerCheck {
	return report.ConsumerCheck{Name: name, Outcome: report.OutcomeChecked, Admitted: 1, Report: built}
}

func passing(name string) report.ConsumerCheck {
	built := report.New(report.Scan{Consumers: 1, Messages: 1}, heldGates(), nil)
	built.Coverage = attempted()

	return checked(name, built)
}

func failing(name string) report.ConsumerCheck {
	stock := corruptingStock()
	stock.Consumer = name

	built := report.New(report.Scan{Consumers: 1, Messages: 1}, heldGates(), []report.Divergence{stock})
	built.Coverage = attempted()

	return checked(name, built)
}

func warning(name string) report.ConsumerCheck {
	receipt := unclassifiedReceipt()
	receipt.Consumer = name

	built := report.New(report.Scan{Consumers: 1, Messages: 1}, heldGates(), []report.Divergence{receipt})
	built.Coverage = attempted()

	return checked(name, built)
}

func gated(name string) report.ConsumerCheck {
	return checked(name, report.New(report.Scan{Consumers: 1, Messages: 1}, violatedGates(), nil))
}

func unstable(name string, class gate.Class) report.ConsumerCheck {
	violation := report.GateCheck{Name: report.GateDeterminism, Result: gate.Result{
		Class: class, Want: "pg.query UPDATE stock", Got: "pg.query DELETE FROM stock", Message: 1,
	}}

	return checked(name, report.New(report.Scan{Consumers: 1, Messages: 1}, []report.GateCheck{violation}, nil))
}

func held(name string) report.ConsumerCheck {
	return checked(name, report.New(report.Scan{Consumers: 1, Messages: 1}, heldGates(), nil))
}

func setUp(name string) report.ConsumerCheck {
	return report.ConsumerCheck{
		Name: name, Outcome: report.OutcomeSetup, Reason: "the run was interrupted",
		Report: report.SetupFailed(errors.New("interrupted")),
	}
}

func notCovered(name string) report.ConsumerCheck {
	return report.ConsumerCheck{Name: name, Outcome: report.OutcomeNotCovered, Reason: "no corpus message matches"}
}

func notSelected(name string) report.ConsumerCheck {
	return report.ConsumerCheck{Name: name, Outcome: report.OutcomeNotSelected, Reason: "not named by --consumer"}
}

// TestInvocationExitCodeIsFailFirst: another consumer's setup error never masks a reproduced finding,
// and a check that judged nothing is a setup error, never a pass.
func TestInvocationExitCodeIsFailFirst(t *testing.T) {
	t.Parallel()

	unnamedGate := gated("")

	cases := []struct {
		name string
		inv  report.Invocation
		want int
	}{
		{"fail+setup", report.Invocation{Consumers: []report.ConsumerCheck{failing("a"), setUp("b")}}, 1},
		{"setup+gate", report.Invocation{Consumers: []report.ConsumerCheck{setUp("a"), gated("b")}}, 3},
		{"gate+pass", report.Invocation{Consumers: []report.ConsumerCheck{gated("a"), passing("b")}}, 2},
		{"only not-covered and not-selected", report.Invocation{
			Consumers: []report.ConsumerCheck{notCovered("a"), notSelected("b")},
		}, 3},
		{"all pass", report.Invocation{Consumers: []report.ConsumerCheck{passing("a"), passing("b")}}, 0},
		{"whole-check setup", report.Invocation{
			Setup: errors.New("the engine refused"), Consumers: []report.ConsumerCheck{passing("a")},
		}, 3},
		{"unnamed gate", report.Invocation{Unnamed: &unnamedGate}, 2},
		{"held only", report.Invocation{Consumers: []report.ConsumerCheck{held("a")}}, 0},
	}

	for _, testCase := range cases {
		if got := testCase.inv.ExitCode(); got != testCase.want {
			t.Errorf("%s: ExitCode() = %d, want %d", testCase.name, got, testCase.want)
		}
	}

	t.Logf("cases=%d", len(cases))
}

// TestEveryConsumerLandsInExactlyOneBucket: first match wins, so a consumer is counted once.
func TestEveryConsumerLandsInExactlyOneBucket(t *testing.T) {
	t.Parallel()

	cases := []struct {
		want  report.Bucket
		check report.ConsumerCheck
	}{
		{report.BucketNotSelected, notSelected("a")},
		{report.BucketNotCovered, notCovered("b")},
		{report.BucketSetup, setUp("c")},
		{report.BucketGate, gated("d")},
		{report.BucketFail, failing("e")},
		{report.BucketWarn, warning("f")},
		{report.BucketHeld, held("g")},
		{report.BucketPass, passing("h")},
	}

	seen := make(map[report.Bucket]int, len(cases))

	for _, testCase := range cases {
		got := testCase.check.Bucket()
		if got != testCase.want {
			t.Errorf("consumer %s: Bucket() = %q, want %q", testCase.check.Name, got, testCase.want)
		}

		seen[got]++
	}

	for bucket, count := range seen {
		if count != 1 {
			t.Errorf("bucket %q holds %d consumers, want 1", bucket, count)
		}
	}

	t.Logf("buckets=%d", len(seen))

	if len(seen) == 0 {
		t.Fatal("no bucket was derived")
	}
}

// TestNoLegalFaultIsHeldNeverPass: a consumer that licensed no fault was never tested against one,
// and gate mode injects nothing; neither is a pass.
func TestNoLegalFaultIsHeldNeverPass(t *testing.T) {
	t.Parallel()

	const noAcks = "AckPolicy: none"

	illegal := held("orders")
	illegal.Report.Coverage = []report.Coverage{
		{Fault: policy.FaultDuplicate, Clause: noAcks},
		{Fault: policy.FaultCrashBeforeAck, Clause: noAcks},
		{Fault: policy.FaultDelay, Clause: noAcks},
		{Fault: policy.FaultReorder, Clause: "MaxAckPending 1"},
	}

	gatesOnly := held("orders")
	gatesOnly.Report.GatesOnly = true

	for name, subject := range map[string]report.ConsumerCheck{"no legal fault": illegal, "gate mode": gatesOnly} {
		if got := subject.Bucket(); got != report.BucketHeld {
			t.Errorf("%s: Bucket() = %q, want %q — nothing was injected, so nothing was survived",
				name, got, report.BucketHeld)
		}
	}
}

// TestAnUnstableSiblingHoldsFailToWarn: a service that did different work between two clean runs of
// one consumer may manufacture a divergence in another, so that other consumer's FAIL is held back.
func TestAnUnstableSiblingHoldsFailToWarn(t *testing.T) {
	t.Parallel()

	cases := []struct {
		class    gate.Class
		holds    bool
		wantExit int
	}{
		{gate.ClassDivergentSet, true, report.ExitGateViolated},
		{gate.ClassFieldDrift, true, report.ExitGateViolated},
		{gate.ClassNotReproducible, true, report.ExitGateViolated},
		{gate.ClassReordered, false, report.ExitFail},
		{gate.ClassUnobserved, false, report.ExitFail},
	}

	for _, testCase := range cases {
		inv := report.Invocation{Consumers: []report.ConsumerCheck{
			failing("orders"), unstable("billing", testCase.class),
		}}

		if got := inv.ExitCode(); got != testCase.wantExit {
			t.Errorf("%s: ExitCode() = %d, want %d", testCase.class, got, testCase.wantExit)
		}

		judged := inv.Judged()[0]
		wantBucket := report.BucketFail

		if testCase.holds {
			wantBucket = report.BucketWarn
		}

		if got := judged.Bucket(); got != wantBucket {
			t.Errorf("%s: consumer orders' bucket = %q, want %q", testCase.class, got, wantBucket)
		}

		reservations := strings.Join(judged.Report.Findings[0].Reservations, "\n")
		named := strings.Contains(reservations, "billing") && strings.Contains(reservations, string(testCase.class))

		if named != testCase.holds {
			t.Errorf("%s: a reservation naming consumer billing and the class = %t, want %t; reservations:\n%s",
				testCase.class, named, testCase.holds, reservations)
		}
	}

	if failing("orders").Report.Findings[0].Status != report.StatusFail {
		t.Error("the fixture's finding is not a FAIL, so nothing here could have been held back")
	}
}
