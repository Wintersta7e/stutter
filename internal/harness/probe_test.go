package harness_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/harness"
)

// probeSlack is how far past the startup limit a probe start may end: ten polls of the wait.
const probeSlack = 10 * 25 * time.Millisecond

// createOrders is a service creating the ORDERS stream itself, as its startup does.
func createOrders(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{ordersFilter}})

	return err
}

// TestAProbeStartCountsQuietOnlyAfterTheFirstBusRequest: a service that initialises for longer than the
// settle period before it first touches JetStream is still starting, not finished; counting quiet from
// its start would read it as a service that never creates its stream.
func TestAProbeStartCountsQuietOnlyAfterTheFirstBusRequest(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t)
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		select {
		case <-ctx.Done():
			return 0
		case <-time.After(3 * 100 * time.Millisecond):
		}

		if createOrders(ctx, js) != nil {
			return 2
		}

		return idle(ctx)
	}

	cfg := startConfig(store, checkpoint, service)
	cfg.Startup = 3 * time.Second

	began := time.Now()

	probed, err := harness.ProbeStart(startContext(t), cfg)
	if err != nil {
		t.Fatalf("ProbeStart() error = %v", err)
	}

	t.Logf("the probe start took %s", time.Since(began))

	if !probed.Created {
		t.Error("the probe start read the stream as never created, want created")
	}
}

// TestAProbeStartEndsWhenItsTargetExits: a service that exits during the probe start ends it at once,
// and that is no error — the stream's owner is read at that moment.
func TestAProbeStartEndsWhenItsTargetExits(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t)
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		if createDurable(ctx, js, "ORDERS", reserve) == nil {
			return 2
		}

		return 1
	}

	cfg := startConfig(store, checkpoint, service)
	began := time.Now()

	probed, err := harness.ProbeStart(startContext(t), cfg)
	if err != nil {
		t.Fatalf("ProbeStart() error = %v", err)
	}

	elapsed := time.Since(began)
	t.Logf("the probe start ended after %s", elapsed)

	if probed.Created || probed.Discovery != nil {
		t.Errorf("probe = %+v, want the stream not created and no discovery", probed)
	}

	if elapsed >= cfg.Startup {
		t.Errorf("the probe start ended after %s, want before the %s startup limit", elapsed, cfg.Startup)
	}
}

// TestAProbeStartEndsWhenTheServiceFallsQuietAfterItsFirstRequest: a service that asked JetStream for
// something and then went quiet for the settle period has finished starting without the stream.
func TestAProbeStartEndsWhenTheServiceFallsQuietAfterItsFirstRequest(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t)
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		if _, err := js.Stream(ctx, "ORDERS"); err == nil {
			return 2
		}

		return idle(ctx)
	}

	began := time.Now()

	probed, err := harness.ProbeStart(startContext(t), startConfig(store, checkpoint, service))
	if err != nil {
		t.Fatalf("ProbeStart() error = %v", err)
	}

	elapsed := time.Since(began)
	t.Logf("the probe start ended after %s", elapsed)

	if probed.Created {
		t.Error("the probe start read the stream as created, want not")
	}

	if elapsed >= 2500*time.Millisecond {
		t.Errorf("the probe start ended after %s, want it well inside the 5s startup limit", elapsed)
	}
}

// TestAProbeStartGivesUpAtTheStartupLimit: a service that keeps talking to JetStream and never creates
// the stream is given up on at the startup limit, and that is no error: the stream is Stutter's to make.
func TestAProbeStartGivesUpAtTheStartupLimit(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t)
	service := &fakeService{}
	service.script = func(ctx context.Context, js jetstream.JetStream, _ *nats.Conn, _ harness.Addresses) int {
		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return 0
			case <-ticker.C:
				if _, err := js.AccountInfo(ctx); err != nil && ctx.Err() == nil {
					return 2
				}
			}
		}
	}

	const limit = 600 * time.Millisecond

	cfg := startConfig(store, checkpoint, service)
	cfg.Startup = limit

	began := time.Now()

	probed, err := harness.ProbeStart(startContext(t), cfg)
	if err != nil {
		t.Fatalf("ProbeStart() error = %v", err)
	}

	elapsed := time.Since(began)
	t.Logf("the probe start gave up after %s", elapsed)

	if probed.Created {
		t.Error("the probe start read the stream as created, want not")
	}

	if elapsed < limit || elapsed >= limit+probeSlack {
		t.Errorf("the probe start gave up after %s, want within %s of the %s limit", elapsed, probeSlack, limit)
	}
}

// TestAnEnvironmentStopDuringTheProbeStartIsASetupError: a dependency the proxy cannot reach stops the
// probe start at once, naming it and the start.
func TestAnEnvironmentStopDuringTheProbeStartIsASetupError(t *testing.T) {
	t.Parallel()

	store, checkpoint := newBus(t)
	cfg := startConfig(store, checkpoint, &fakeService{script: dialsCache})
	cfg.Opaque = map[string]string{"cache": closedPort(t).String()}

	began := time.Now()

	_, err := harness.ProbeStart(startContext(t), cfg)

	elapsed := time.Since(began)
	t.Logf("the probe start stopped after %s: %v", elapsed, err)

	if err == nil || !strings.Contains(err.Error(), "the probe start") || !strings.Contains(err.Error(), "cache") {
		t.Fatalf("ProbeStart() error = %v, want one naming the probe start and cache", err)
	}

	if elapsed >= 2500*time.Millisecond {
		t.Errorf("the probe start stopped after %s, want it well inside the 5s startup limit", elapsed)
	}
}
