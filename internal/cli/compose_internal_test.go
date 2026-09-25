package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
)

const failedLog = "/tmp/stutter-c0ffee42/logs/target-7.log"

// heldReport is a consumer check whose gates held, one fault attempted, nothing found.
func heldReport(findings ...report.Divergence) report.Report {
	gates := []report.GateCheck{{Name: report.GateDeterminism, Result: gate.Result{Class: gate.ClassMatch, Index: -1}}}
	built := report.New(report.Scan{Consumers: 1, Messages: 1}, gates, findings)
	built.Coverage = []report.Coverage{{Pairs: 1, Attempted: 1, Legal: true}}

	return built
}

func failingFinding() report.Divergence {
	return report.Divergence{
		Consumer: testService, Fault: "duplicate", Clause: "AckPolicy: explicit", Summary: "stock decremented twice",
		Repro: "messages #1", DivergedKind: "pg.query", DivergedAt: 0,
	}
}

// TestLogsAreKeptWheneverAnOutcomeNamesOne: a log a rendered outcome points at must still exist after
// Stutter exits; with nothing to point at, the check leaves nothing behind.
func TestLogsAreKeptWheneverAnOutcomeNamesOne(t *testing.T) {
	t.Parallel()

	exited := &replay.ExitError{Exit: replay.Exit{Log: failedLog, Code: 3, Exited: true}}
	setUp := report.ConsumerCheck{
		Name: "billing", Outcome: report.OutcomeSetup,
		Report: report.SetupFailed(fmt.Errorf("run clean-1: %w", exited)),
	}
	failed := report.ConsumerCheck{
		Name:    testService,
		Outcome: report.OutcomeChecked,
		Report:  heldReport(failingFinding()),
	}
	passed := report.ConsumerCheck{Name: testService, Outcome: report.OutcomeChecked, Report: heldReport()}
	gated := report.ConsumerCheck{Name: testService, Outcome: report.OutcomeChecked, Report: report.New(
		report.Scan{}, []report.GateCheck{{Name: report.GateObservation, Result: gate.Observed(nil)}}, nil)}

	cases := []struct {
		name        string
		inv         report.Invocation
		want        provision.Retention
		interrupted bool
	}{
		{
			"exit 1 beside a setup naming a log",
			report.Invocation{Consumers: []report.ConsumerCheck{failed, setUp}},
			provision.KeepLogs, false,
		},
		{"interrupted exit 1", report.Invocation{Consumers: []report.ConsumerCheck{failed}}, provision.KeepLogs, true},
		{"exit 2", report.Invocation{Consumers: []report.ConsumerCheck{gated}}, provision.KeepLogs, false},
		{"exit 3", report.Invocation{Setup: errors.New("the engine refused")}, provision.KeepLogs, false},
		{"exit 0", report.Invocation{Consumers: []report.ConsumerCheck{passed}}, provision.DiscardLogs, false},
		{
			"exit 1 naming no log",
			report.Invocation{Consumers: []report.ConsumerCheck{failed}},
			provision.DiscardLogs,
			false,
		},
	}

	interrupted, cancel := context.WithCancel(t.Context())
	cancel()

	for _, testCase := range cases {
		ctx := t.Context()
		if testCase.interrupted {
			ctx = interrupted
		}

		got, logs := retention(ctx, testCase.inv, "")
		if got != testCase.want {
			t.Errorf("%s (exit %d): retention = %v, want %v", testCase.name, testCase.inv.ExitCode(), got,
				testCase.want)
		}

		if testCase.name == "exit 1 beside a setup naming a log" && !slices.Contains(logs, failedLog) {
			t.Errorf("%s: the implicated logs %v do not include %s", testCase.name, logs, failedLog)
		}
	}

	// Discovery's log is implicated only when discovery is what stopped the check.
	const discoveryLog = "/tmp/stutter-c0ffee42/logs/discovery-15.log"

	passing := report.Invocation{Consumers: []report.ConsumerCheck{passed}}
	if got, logs := retention(t.Context(), passing, discoveryLog); got != provision.DiscardLogs || len(logs) != 0 {
		t.Errorf("a passing check whose discovery left a log = (%v, %v), want nothing kept or named", got, logs)
	}

	stopped := report.Invocation{Setup: errors.New("the service exited before creating any consumer")}
	if _, logs := retention(t.Context(), stopped, discoveryLog); !slices.Contains(logs, discoveryLog) {
		t.Errorf("a check discovery stopped names logs %v, want %s among them", logs, discoveryLog)
	}
}

// TestTheReportIsRenderedBeforeTeardown: the verdict is on stdout before anything is torn down, so a
// second interrupt during teardown still leaves it.
func TestTheReportIsRenderedBeforeTeardown(t *testing.T) {
	t.Parallel()

	var code int

	names := stepNames(newComposeCheck(composeRun{}, nil).finalSteps(&report.Invocation{}, nil, &code))
	if want := []string{"render", "teardown", "stderr"}; !slices.Equal(names, want) {
		t.Errorf("final phases = %v, want %v", names, want)
	}
}

// TestTeardownFollowsTheTeardownOrder: the relays go before the listeners they pipe to, and the engine's
// own teardown — every container, network, volume and image the ledger names — comes last.
func TestTeardownFollowsTheTeardownOrder(t *testing.T) {
	t.Parallel()

	var down provision.Teardown

	names := stepNames(newComposeCheck(composeRun{}, nil).teardownSteps(provision.DiscardLogs, &down))
	if want := []string{"topology", stepBus, "engine"}; !slices.Equal(names, want) {
		t.Errorf("teardown = %v, want %v", names, want)
	}
}

// TestTheFirstInterruptPrintsOneLine: the interrupt line is printed once, however often the context is
// cancelled.
func TestTheFirstInterruptPrintsOneLine(t *testing.T) {
	t.Parallel()

	var stderr strings.Builder

	out := newLockedWriter(&stderr)
	ctx, cancel := context.WithCancel(t.Context())

	stop, printed := watchInterrupt(ctx, out)
	defer stop()

	cancel()
	cancel()

	select {
	case <-printed:
	case <-time.After(5 * time.Second):
		t.Fatal("no interrupt line within 5s of the interrupt")
	}

	if count := strings.Count(stderr.String(), interruptLine()); count != 1 {
		t.Errorf("%d interrupt lines, want 1:\n%s", count, stderr.String())
	}
}

// TestABindChangedDuringTheCheckWithholdsEveryVerdict: runs that read different input cannot be
// compared, so every verdict is withheld and the changed path named.
func TestABindChangedDuringTheCheckWithholdsEveryVerdict(t *testing.T) {
	t.Parallel()

	const changed = "/srv/config/app.yaml"

	inv := report.Invocation{Consumers: []report.ConsumerCheck{
		{Name: testService, Outcome: report.OutcomeChecked, Admitted: 1, Report: heldReport(failingFinding())},
	}}

	withheld := endWalk(inv, []string{changed})

	if got := withheld.ExitCode(); got != report.ExitSetupError {
		t.Errorf("ExitCode() = %d, want %d", got, report.ExitSetupError)
	}

	var out strings.Builder
	if err := withheld.Render(&out); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out.String(), changed) {
		t.Errorf("the report does not name %s:\n%s", changed, out.String())
	}

	for line := range strings.Lines(out.String()) {
		if strings.HasPrefix(line, string(report.StatusFail)) {
			t.Errorf("a verdict was rendered: %q", line)
		}
	}

	if unchanged := endWalk(inv, nil); unchanged.Setup != nil {
		t.Errorf("an unchanged walk withheld the verdicts: %v", unchanged.Setup)
	}
}
