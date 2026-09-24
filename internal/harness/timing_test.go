package harness_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// TestTheStartupLimitHasOneOwner: the startup limit is stated once, as a constant the CLI's usage can
// read. A second statement of it — a comment naming a number, an unexported copy — drifted from the
// value in force the moment the value changed.
func TestTheStartupLimitHasOneOwner(t *testing.T) {
	t.Parallel()

	if harness.DefaultStartup != 60*time.Second {
		t.Errorf("DefaultStartup = %s, want 1m0s", harness.DefaultStartup)
	}

	scanned := 0

	for _, name := range []string{"harness.go", "observed.go"} {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}

		scanned++

		for _, restated := range []string{"Zero is ten seconds.", "defaultStartup"} {
			if strings.Contains(string(source), restated) {
				t.Errorf("%s restates the startup limit: %q", name, restated)
			}
		}
	}

	t.Logf("scanned %d files", scanned)

	if scanned == 0 {
		t.Fatal("scanned no files, so the scan proves nothing")
	}
}

// errNeverCalled is what a service constructor that no test ever calls would return.
var errNeverCalled = errors.New("never called")

// TestADrainBelowTheFloorIsRefused: a run that stopped waiting before the longest redelivery its
// configuration allows would call a message owed whose redelivery was still coming, so a drain below
// that floor is refused before any run — and one that only lengthens the wait is taken.
func TestADrainBelowTheFloorIsRefused(t *testing.T) {
	t.Parallel()

	short := policy.Config{AckMode: policy.AckExplicit, AckWait: 500 * time.Millisecond, MaxDeliver: 3}
	curved := policy.Config{
		AckMode:    policy.AckExplicit,
		BackOff:    []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute},
		MaxDeliver: 100,
	}

	floors := []struct {
		config  policy.Config
		quiesce time.Duration
		want    time.Duration
	}{
		{config: short, quiesce: 50 * time.Millisecond, want: 1050 * time.Millisecond},
		{config: curved, quiesce: 200 * time.Millisecond, want: 120*time.Second + 200*time.Millisecond},
	}

	for _, row := range floors {
		got := harness.DrainFloor(row.config, row.quiesce)
		t.Logf("floor %s (quiesce %s)", got, row.quiesce)

		if got != row.want {
			t.Errorf("DrainFloor() = %s, want %s", got, row.want)
		}
	}

	observed := func(drain time.Duration) error {
		_, err := harness.New(harness.Config{
			Policy:  short,
			Quiesce: 50 * time.Millisecond,
			Drain:   drain,
			Start:   func(context.Context, harness.Addresses) (harness.Consumer, error) { return absent{}, nil },
		})

		return err
	}

	floor := floors[0].want

	err := observed(floor - time.Millisecond)
	if !errors.Is(err, harness.ErrDrainBelowFloor) || !strings.Contains(err.Error(), floor.String()) {
		t.Errorf("New() with a drain below the floor: error = %v, want ErrDrainBelowFloor naming %s", err, floor)
	}

	if err := observed(floor); err != nil {
		t.Errorf("New() with a drain at the floor: error = %v, want it taken", err)
	}

	if _, err := harness.New(harness.Config{
		Policy:  short,
		Drain:   time.Millisecond,
		Connect: func(context.Context, harness.Addresses) (harness.Service, error) { return nil, errNeverCalled },
	}); err != nil {
		t.Errorf("New() dispatching with a drain it never uses: error = %v, want it ignored", err)
	}
}

// TestADrainOnlyLengthensTheOwedLimit: a drain above the floor is how long a run waits for messages
// still owed, so a service that stops consuming is waited for that long.
func TestADrainOnlyLengthensTheOwedLimit(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	drain := harness.DrainFloor(config, toy.DefaultQuiesce) + time.Second

	timings := &timeline{}

	built, _ := quirkySandbox(t, config, quirks{stopAfter: 1, timeline: timings}, func(settings *harness.Config) {
		settings.Drain = drain
	}, "ORD-DRAIN-1", "ORD-DRAIN-2")

	result, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	noted := timings.read()
	t.Logf("closed %s after the last acknowledgement, drain %s; owed %d", noted.closed.Sub(noted.lastAck), drain,
		result.Owed)

	if waited := noted.closed.Sub(noted.lastAck); waited < drain {
		t.Errorf("the run ended %s after the service fell silent, before the drain of %s", waited, drain)
	}
}

// TestTimingsReportWhatTheSandboxUses: a report that shows the timings a check ran with has to show
// the values in force — a default where nothing was set, never a zero — and a set value as set.
func TestTimingsReportWhatTheSandboxUses(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	start := func(context.Context, harness.Addresses) (harness.Consumer, error) { return absent{}, nil }

	defaults, err := harness.New(harness.Config{Policy: config, Start: start})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	want := harness.Timings{
		Startup: harness.DefaultStartup,
		Quiesce: replay.DefaultQuiesce,
		Drain:   harness.DrainFloor(config, 0),
	}

	got := defaults.Timings()
	t.Logf("defaults: %+v", got)

	if got != want {
		t.Errorf("Timings() = %+v, want %+v", got, want)
	}

	set := harness.Timings{Startup: 7 * time.Second, Quiesce: 30 * time.Millisecond, Drain: 9 * time.Second}

	configured, err := harness.New(harness.Config{
		Policy:  config,
		Start:   start,
		Startup: set.Startup,
		Quiesce: set.Quiesce,
		Drain:   set.Drain,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if got := configured.Timings(); got != set {
		t.Errorf("Timings() = %+v, want what was set, %+v", got, set)
	}
}

// TestTheSpanStartsAtPublication: a run's span is the time it spent on the bus, from the corpus being
// published to the run's end, so Stutter's own overhead — starting the service, waiting for it to
// settle — is whatever of the run's elapsed time lies outside it.
func TestTheSpanStartsAtPublication(t *testing.T) {
	t.Parallel()

	const slowStart = 400 * time.Millisecond

	config := observedConfig()

	built, _ := quirkySandbox(t, config, quirks{}, func(settings *harness.Config) {
		settings.Start = func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			time.Sleep(slowStart)

			service, err := startPulling(ctx, at, config, quirks{})
			if err != nil {
				return nil, err
			}

			return service, nil
		}
	}, "ORD-SPAN-1")

	began := time.Now()

	result, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	elapsed := time.Since(began)
	settle := settleMargin * toy.DefaultQuiesce

	t.Logf("span %s of %s elapsed (settle %s, a %s start)", result.Span, elapsed, settle, slowStart)

	if result.Span < settle {
		t.Errorf("Span = %s, want at least the settle period %s", result.Span, settle)
	}

	if result.Span > elapsed-slowStart+50*time.Millisecond {
		t.Errorf("Span = %s of %s elapsed, want the %s start left out of it", result.Span, elapsed, slowStart)
	}
}
