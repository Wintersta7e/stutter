package harness_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// exitStartup bounds the wait for a consumer in the exit tests, so a run that failed to notice the
// exit stops at the startup limit instead of waiting out the default.
const exitStartup = 2 * time.Second

// errRelayGone is the error a service's Close reports when something it depended on died mid-run.
var errRelayGone = errors.New("a relay died during the run")

// exiting is a service that can stop by itself. Its Exited channel closes when die is called, and
// Close reports a scripted exit and error, as a container inspected before it is killed would.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type exiting struct {
	err error
	// inner is the pulling service it wraps; nil for a service that dies before it consumes.
	inner *pulling
	// exited is nil for a service that never stops by itself: a nil channel never fires.
	exited chan struct{}
	// diedAt is when die was called, on the monotonic clock.
	diedAt time.Time
	exit   replay.Exit
	once   sync.Once
	mu     sync.Mutex
}

func (e *exiting) Exited() <-chan struct{} {
	return e.exited
}

func (e *exiting) Close(ctx context.Context) (replay.Exit, error) {
	if e.inner != nil {
		if _, err := e.inner.Close(ctx); err != nil {
			return replay.Exit{}, err
		}
	}

	return e.exit, e.err
}

// die stops the service by itself.
func (e *exiting) die() {
	e.once.Do(func() {
		e.mu.Lock()
		e.diedAt = time.Now()
		e.mu.Unlock()

		close(e.exited)
	})
}

// died reports when die was called.
func (e *exiting) died() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.diedAt
}

// messageCount is how many messages the corpus stream holds, read on Stutter's own connection.
func messageCount(ctx context.Context, t *testing.T, store *corpus.Corpus) int {
	t.Helper()

	held, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	return len(held)
}

// TestATargetThatExitsBeforeItsFirstDeliveryStopsTheRun: a service that dies while starting has no
// outcome to compare, and publishing the corpus to it would only wait out the startup limit. The run
// stops at the exit, before anything is published, saying how the service ended and where its log is.
func TestATargetThatExitsBeforeItsFirstDeliveryStopsTheRun(t *testing.T) {
	t.Parallel()

	scripted := replay.Exit{Log: "logs/target-1.log", Code: 137, Exited: true, OOMKilled: true}

	var (
		store   *corpus.Corpus
		atStart int
	)

	built, _ := quirkySandbox(t, observedConfig(), quirks{}, func(settings *harness.Config) {
		store = settings.Corpus
		settings.Startup = exitStartup
		settings.Start = func(ctx context.Context, _ harness.Addresses) (harness.Consumer, error) {
			atStart = messageCount(ctx, t, store)

			service := &exiting{exited: make(chan struct{}), exit: scripted}
			service.die()

			return service, nil
		}
	}, "ORD-EXIT-1")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)

	after := messageCount(t.Context(), t, store)
	t.Logf("the corpus stream held %d messages when the service started and %d after the run", atStart, after)

	var exited *replay.ExitError
	if !errors.As(err, &exited) {
		t.Fatalf("Run() error = %v, want a *replay.ExitError", err)
	}

	if exited.Exit.After != 0 {
		t.Errorf("Exit.After = %d, want 0 — nothing was delivered", exited.Exit.After)
	}

	for _, want := range []string{"137", "OOM-killed", "logs/target-1.log", "before its first delivery"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Run() error = %q, want it to name %q", err, want)
		}
	}

	if after != atStart {
		t.Errorf("the corpus was published to a service that had already exited: %d messages, want %d",
			after, atStart)
	}
}

// TestTheRunEndsAtOnceWhenTheTargetExits: a service that dies mid-run produces nothing more, and a run
// that waited for the bus to go quiet would sit out the whole redelivery horizon before noticing. It
// ends at the exit, carrying how the service ended and the message it followed.
func TestTheRunEndsAtOnceWhenTheTargetExits(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	// Long enough that waiting for silence would take ten seconds or more.
	config.AckWait = 5 * time.Second

	var service *exiting

	built, recorded := quirkySandbox(t, config, quirks{}, func(settings *harness.Config) {
		settings.Start = func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			service = &exiting{exited: make(chan struct{}), exit: replay.Exit{Code: 1, Exited: true}}

			inner, err := startPulling(ctx, at, config, quirks{stopAfter: 1, onStop: service.die})
			if err != nil {
				return nil, err
			}

			service.inner = inner

			return service, nil
		}
	}, "ORD-EXIT-1", "ORD-EXIT-2")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	result, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)

	if service == nil || service.died().IsZero() {
		t.Fatalf("the service never exited (Run() error = %v)", err)
	}

	elapsed := time.Since(service.died())
	t.Logf("the run returned %s after the service exited", elapsed)

	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if elapsed > 2*time.Second {
		t.Errorf("the run returned %s after the service exited, want within 2s", elapsed)
	}

	if !result.Exit.Exited || result.Exit.After != recorded[0] {
		t.Errorf("Exit = %+v, want Exited after message %d", result.Exit, recorded[0])
	}
}

// TestARestartedTargetStopsTheRun: a service the engine restarted mid-run came back with none of its
// state, and whatever it did next cannot be attributed to the message it was handling.
func TestARestartedTargetStopsTheRun(t *testing.T) {
	t.Parallel()

	built := scriptedExit(t, replay.Exit{Restarts: 1}, nil)

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err == nil || !strings.Contains(err.Error(), "RestartCount 1") {
		t.Fatalf("Run() error = %v, want the run stopped naming RestartCount 1", err)
	}
}

// TestACloseErrorStopsTheRun: what the service's Close reports — a relay that died beside it — is the
// run's error, never dropped on the way to a verdict.
func TestACloseErrorStopsTheRun(t *testing.T) {
	t.Parallel()

	built := scriptedExit(t, replay.Exit{}, errRelayGone)

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if !errors.Is(err, errRelayGone) {
		t.Fatalf("Run() error = %v, want %v", err, errRelayGone)
	}
}

// scriptedExit builds a sandbox around a pulling service that never stops by itself and whose Close
// reports exit and err.
func scriptedExit(t *testing.T, exit replay.Exit, err error) *harness.Sandbox {
	t.Helper()

	config := observedConfig()

	built, _ := quirkySandbox(t, config, quirks{}, func(settings *harness.Config) {
		settings.Start = func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			inner, startErr := startPulling(ctx, at, config, quirks{})
			if startErr != nil {
				return nil, startErr
			}

			return &exiting{inner: inner, exit: exit, err: err}, nil
		}
	}, "ORD-EXIT-1")

	return built
}
