package harness_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
)

// TestAConsumerThatDiffersFromDiscoveryStopsTheRun: every fault a check injects was licensed by the
// configuration it was given, so a service whose consumer is not that configuration would be faulted
// under a contract it does not have. The run stops, naming what differs.
func TestAConsumerThatDiffersFromDiscoveryStopsTheRun(t *testing.T) {
	t.Parallel()

	declared := observedConfig()
	created := declared
	created.MaxDeliver = 5

	built, recorded := quirkySandbox(t, declared, quirks{}, func(settings *harness.Config) {
		settings.Start = func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			service, err := startPulling(ctx, at, created, quirks{})
			if err != nil {
				return nil, err
			}

			return service, nil
		}
	}, "ORD-DRIFT-1")

	result, err := check.Run(t.Context(), built, check.Options{
		Messages: recorded,
		Consumer: observedConsumer,
		Config:   declared,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if got := result.ExitCode(); got != report.ExitSetupError {
		t.Fatalf("ExitCode() = %d, want %d (setup):\n%s", got, report.ExitSetupError, result)
	}

	if !strings.Contains(result.String(), "MaxDeliver") {
		t.Errorf("the setup error does not name MaxDeliver:\n%s", result)
	}
}

// TestAConsumerRecreatedMidRunStopsTheRun: a service that re-creates its consumer part way through a run
// undoes the rewrite that held it to one message in flight, and from then on its effects cannot be
// attributed. Nothing on the wire says so; reading the consumer back once the run is over does.
func TestAConsumerRecreatedMidRunStopsTheRun(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	config.MaxAckPending = 10

	built, _ := quirkySandbox(t, config, quirks{recreate: true}, nil, "ORD-DRIFT-1", "ORD-DRIFT-2")

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err == nil || !strings.Contains(err.Error(), "MaxAckPending") {
		t.Fatalf("Run() error = %v, want the run stopped naming MaxAckPending", err)
	}

	t.Logf("the run stopped: %v", err)
}
