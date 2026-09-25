package report_test

import (
	"errors"
	"strconv"
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

func rendered(t *testing.T, inv report.Invocation) string {
	t.Helper()

	var out strings.Builder
	if err := inv.Render(&out); err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	return out.String()
}

// closing parses the closing line: each bucket's count, and the discovered total it states.
func closing(t *testing.T, text string) (map[string]int, int) {
	t.Helper()

	var line string

	for candidate := range strings.Lines(text) {
		if strings.HasPrefix(candidate, "Consumers:") {
			line = strings.TrimSpace(candidate)
		}
	}

	if line == "" {
		t.Fatalf("no closing line in:\n%s", text)
	}

	counts := make(map[string]int)
	body, total, found := strings.Cut(strings.TrimPrefix(line, "Consumers:"), "=")

	if !found {
		t.Fatalf("the closing line %q states no total", line)
	}

	for part := range strings.SplitSeq(body, ",") {
		fields := strings.Fields(part)
		if len(fields) < 2 {
			continue
		}

		count, err := strconv.Atoi(fields[len(fields)-1])
		if err != nil {
			t.Fatalf("closing line %q: %q is not a count", line, part)
		}

		counts[strings.Join(fields[:len(fields)-1], " ")] = count
	}

	discovered, err := strconv.Atoi(strings.Fields(total)[0])
	if err != nil {
		t.Fatalf("closing line %q: the total is not a count", line)
	}

	return counts, discovered
}

// TestClosingLineSumsToTheDiscoveredTotal: every discovered consumer is counted in exactly one
// bucket, and the unnamed check is named apart from them.
func TestClosingLineSumsToTheDiscoveredTotal(t *testing.T) {
	t.Parallel()

	unnamed := gated("")
	inv := report.Invocation{
		Consumers: []report.ConsumerCheck{
			passing("a"), passing("b"), failing("c"), notCovered("d"), notSelected("e"),
		},
		Unnamed: &unnamed,
	}

	text := rendered(t, inv)
	counts, discovered := closing(t, text)

	sum := 0
	for _, count := range counts {
		sum += count
	}

	t.Logf("sum=%d discovered=%d", sum, discovered)

	if sum != len(inv.Consumers) || discovered != len(inv.Consumers) {
		t.Errorf("bucket counts sum to %d and the line states %d discovered, want %d:\n%s",
			sum, discovered, len(inv.Consumers), text)
	}

	if sum == 0 {
		t.Fatal("the closing line counts nothing")
	}

	if !strings.Contains(text, "unnamed check") {
		t.Errorf("the unnamed check is not named apart from the discovered consumers:\n%s", text)
	}
}

// TestStdoutCarriesNoCheckIDDirectoryOrDuration: stdout is diffable between CI runs.
func TestStdoutCarriesNoCheckIDDirectoryOrDuration(t *testing.T) {
	t.Parallel()

	const (
		checkID    = "c0ffee42"
		privateDir = "/tmp/stutter-c0ffee42"
		measured   = "1.234567s"
	)

	interrupted := setUp("orders")
	interrupted.Reason = "the run stopped after " + measured + ": container stutter-" + checkID + "-target exited"
	interrupted.Report = report.SetupFailed(errors.New("its log is " + privateDir + "/logs/target.log"))

	failed := failing("billing")
	failed.Report.Findings[0].Notes = []string{"stutter-" + checkID + "-restore-db was replaced"}

	text := rendered(t, report.Invocation{
		Consumers: []report.ConsumerCheck{failed, interrupted},
		Scrub:     []string{checkID, privateDir},
	})

	ids, dirs, durations := strings.Count(text, checkID), strings.Count(text, privateDir), strings.Count(text, measured)
	t.Logf("check-id=%d dir=%d duration=%d bytes=%d", ids, dirs, durations, len(text))

	if ids+dirs+durations != 0 {
		t.Errorf("stdout carries the check ID %d, the directory %d and a duration %d times:\n%s",
			ids, dirs, durations, text)
	}

	if len(text) == 0 {
		t.Fatal("nothing was rendered")
	}
}

// TestNoConsumerRendersTheUnnamedBlockAndNoPass: a service with no consumer gets one unnamed check,
// which is never a discovered consumer and never a pass.
func TestNoConsumerRendersTheUnnamedBlockAndNoPass(t *testing.T) {
	t.Parallel()

	unnamed := report.ConsumerCheck{
		Outcome: report.OutcomeChecked, Reason: "the bus refused 1 request",
		Report: report.New(report.Scan{Consumers: 1, Messages: 3}, unobservedGates(), nil),
	}
	inv := report.Invocation{Unnamed: &unnamed}
	text := rendered(t, inv)

	for line := range strings.Lines(text) {
		if strings.HasPrefix(line, string(report.StatusPass)) {
			t.Errorf("a check with no consumer renders a PASS line: %q", line)
		}
	}

	if !strings.Contains(text, "unnamed check") || !strings.Contains(text, string(report.GateObservation)) {
		t.Errorf("the unnamed block with its observation diagnosis is missing:\n%s", text)
	}

	if _, discovered := closing(t, text); discovered != 0 {
		t.Errorf("the closing line states %d discovered consumers, want 0", discovered)
	}

	if got := inv.ExitCode(); got != report.ExitGateViolated {
		t.Errorf("ExitCode() = %d, want %d", got, report.ExitGateViolated)
	}
}

// TestGateModeRendersNoPassFailOrWarnBucket: gate mode injects nothing, so nothing passed, failed or
// warned.
func TestGateModeRendersNoPassFailOrWarnBucket(t *testing.T) {
	t.Parallel()

	gatesOnly := held("orders")
	gatesOnly.Report.GatesOnly = true

	text := rendered(
		t,
		report.Invocation{Consumers: []report.ConsumerCheck{gatesOnly, gated("billing")}, GatesOnly: true},
	)
	counts, _ := closing(t, text)

	for _, bucket := range []report.Bucket{report.BucketPass, report.BucketFail, report.BucketWarn} {
		if _, shown := counts[string(bucket)]; shown {
			t.Errorf("gate mode's closing line shows the %q bucket:\n%s", bucket, text)
		}
	}

	for line := range strings.Lines(text) {
		for _, status := range []report.Status{report.StatusPass, report.StatusFail, report.StatusWarn} {
			if strings.HasPrefix(line, string(status)) {
				t.Errorf("gate mode renders a line opening %s: %q", status, line)
			}
		}
	}
}

// TestNothingJudgedPrintsEveryReason: a check that judged no consumer exits 3 and says why for each.
func TestNothingJudgedPrintsEveryReason(t *testing.T) {
	t.Parallel()

	consumers := []report.ConsumerCheck{notCovered("audit"), notSelected("billing"), notCovered("orders")}
	consumers[2].Reason = "it reads stream PAYMENTS, not the corpus stream"
	inv := report.Invocation{Consumers: consumers}
	text := rendered(t, inv)

	for _, consumer := range consumers {
		if !strings.Contains(text, consumer.Reason) {
			t.Errorf("consumer %s's reason %q is not printed:\n%s", consumer.Name, consumer.Reason, text)
		}
	}

	if got := inv.ExitCode(); got != report.ExitSetupError {
		t.Errorf("ExitCode() = %d, want %d", got, report.ExitSetupError)
	}
}

// TestASiblingReservationIsRendered: the rendered report and the exit code read the same view.
func TestASiblingReservationIsRendered(t *testing.T) {
	t.Parallel()

	inv := report.Invocation{Consumers: []report.ConsumerCheck{
		failing("orders"), unstable("billing", gate.ClassDivergentSet),
	}}
	text := rendered(t, inv)

	var finding string

	for line := range strings.Lines(text) {
		if strings.Contains(line, "orders") && strings.Contains(line, "duplicate delivery") {
			finding = line
		}
	}

	if !strings.HasPrefix(finding, string(report.StatusWarn)) {
		t.Errorf("the held finding renders as %q, want WARN (exit %d):\n%s", finding, inv.ExitCode(), text)
	}

	if !strings.Contains(text, "billing") || !strings.Contains(text, string(gate.ClassDivergentSet)) {
		t.Errorf("the reservation naming billing and %s is not rendered:\n%s", gate.ClassDivergentSet, text)
	}
}
