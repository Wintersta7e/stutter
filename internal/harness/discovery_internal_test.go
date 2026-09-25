package harness

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// startOnlyBus opens a bus bound to stream and checkpoints it, empty, beside its store.
func startOnlyBus(t *testing.T, stream string) (*corpus.Corpus, *corpus.Checkpoint) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "bus")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	store, err := corpus.Open(t.Context(), filepath.Join(dir, "store"), stream)
	if err != nil {
		t.Fatalf("corpus.Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	checkpoint, err := store.Checkpoint(t.Context(), filepath.Join(dir, "B"))
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	return store, &checkpoint
}

// stubCaller is a service that makes one call to the HTTP stub as it starts, and then idles.
type stubCaller struct {
	status atomic.Int64
}

// starter is the caller as a Config.Start.
func (c *stubCaller) starter() Start {
	return func(ctx context.Context, at Addresses) (Consumer, error) {
		status, err := c.call(ctx, at)
		if err != nil {
			return nil, err
		}

		c.status.Store(int64(status))

		return idleService{}, nil
	}
}

// call makes the one call, and reports the stub's status.
func (*stubCaller) call(ctx context.Context, at Addresses) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, at.HTTP+"/rates", http.NoBody)
	if err != nil {
		return 0, err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}

	_ = response.Body.Close()

	return response.StatusCode, nil
}

// idleService never stops by itself and has nothing to release.
type idleService struct{}

func (idleService) Close(context.Context) (replay.Exit, error) { return replay.Exit{}, nil }

func (idleService) Exited() <-chan struct{} { return nil }

// TestAStartThatPublishesNothingRefusesAConfigItCannotRun: the probe start and discovery watch a
// service that consumes for itself, from a checkpoint, on a bound stream; a configuration missing any
// of the three is refused before a service starts.
func TestAStartThatPublishesNothingRefusesAConfigItCannotRun(t *testing.T) {
	t.Parallel()

	store, checkpoint := startOnlyBus(t, "ORDERS")
	unbound, unboundCheckpoint := startOnlyBus(t, "")

	cases := map[string]struct {
		want error
		cfg  func(start Start, connect Connect) Config
	}{
		"connect": {errStartOnly, func(_ Start, connect Connect) Config {
			return Config{Corpus: store, Baseline: checkpoint, Connect: connect}
		}},
		"both": {errStartOnly, func(start Start, connect Connect) Config {
			return Config{Corpus: store, Baseline: checkpoint, Connect: connect, Start: start}
		}},
		"baseline": {errNoBaseline, func(start Start, _ Connect) Config {
			return Config{Corpus: store, Start: start}
		}},
		"unbound": {errUnbound, func(start Start, _ Connect) Config {
			return Config{Corpus: unbound, Baseline: unboundCheckpoint, Start: start}
		}},
		"corpus": {errUnbound, func(start Start, _ Connect) Config {
			return Config{Baseline: checkpoint, Start: start}
		}},
	}

	entries := map[string]func(ctx context.Context, cfg Config) error{
		"discovery": func(ctx context.Context, cfg Config) error {
			_, err := Discover(ctx, cfg)

			return err
		},
		"probe": func(ctx context.Context, cfg Config) error {
			_, err := ProbeStart(ctx, cfg)

			return err
		},
	}

	for entry, run := range entries {
		for name, current := range cases {
			t.Run(entry+"/"+name, func(t *testing.T) {
				t.Parallel()

				var starts atomic.Int32

				start := func(context.Context, Addresses) (Consumer, error) {
					starts.Add(1)

					return idleService{}, nil
				}
				connect := func(context.Context, Addresses) (Service, error) {
					starts.Add(1)

					return nil, errNoService
				}

				if err := run(t.Context(), current.cfg(start, connect)); !errors.Is(err, current.want) {
					t.Errorf("error = %v, want %v", err, current.want)
				}

				if got := starts.Load(); got != 0 {
					t.Errorf("a service was started %d times, want 0", got)
				}
			})
		}
	}
}

// TestDiscoveryNeverFreezesItsScript: a start that publishes nothing aborts its HTTP script, so a call
// it made is never frozen into the replies every run is answered with.
func TestDiscoveryNeverFreezesItsScript(t *testing.T) {
	t.Parallel()

	sandbox, caller := stubCallingSandbox(t)

	if _, err := sandbox.discover(stubContext(t)); err != nil {
		t.Fatalf("discover() error = %v", err)
	}

	if got := caller.status.Load(); got != http.StatusOK {
		t.Fatalf("the stub answered %d, want 200", got)
	}

	assertNeverFrozen(t, sandbox)
}

// TestAProbeStartNeverFreezesItsScript: the probe start aborts its HTTP script too.
func TestAProbeStartNeverFreezesItsScript(t *testing.T) {
	t.Parallel()

	sandbox, caller := stubCallingSandbox(t)

	if _, err := sandbox.probe(stubContext(t)); err != nil {
		t.Fatalf("probe() error = %v", err)
	}

	if got := caller.status.Load(); got != http.StatusOK {
		t.Fatalf("the stub answered %d, want 200", got)
	}

	assertNeverFrozen(t, sandbox)
}

// stubCallingSandbox is the sandbox a start that publishes nothing builds, around a service that calls
// the stub once as it starts and creates nothing, so the start ends at a short startup limit.
func stubCallingSandbox(t *testing.T) (*Sandbox, *stubCaller) {
	t.Helper()

	store, checkpoint := startOnlyBus(t, "ORDERS")
	caller := &stubCaller{}

	sandbox, err := New(Config{
		Corpus:   store,
		Baseline: checkpoint,
		Start:    caller.starter(),
		Quiesce:  20 * time.Millisecond,
		Startup:  300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return sandbox, caller
}

// stubContext bounds one start, so a broken end rule fails at its own deadline instead of hanging.
func stubContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)

	return ctx
}

// assertNeverFrozen fails when the sandbox's next run would be answered from frozen replies: the same
// call made in it comes back stubbed rather than captured.
func assertNeverFrozen(t *testing.T, sandbox *Sandbox) {
	t.Helper()

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), nil)
	recorder.Open("next", 1, nil)

	observed, err := sandbox.observe(t.Context(), recorder, natsproxy.Options{})
	if err != nil {
		t.Fatalf("observe() error = %v", err)
	}

	status, err := (&stubCaller{}).call(t.Context(), observed.at)

	closeErr := errors.Join(observed.close(t.Context()), observed.httpRun.Abort())
	if err != nil || closeErr != nil || status != http.StatusOK {
		t.Fatalf("the next run's call: status %d, error %v, teardown %v", status, err, closeErr)
	}

	effects := recorder.Effects()
	if len(effects) != 1 {
		t.Fatalf("the next run recorded %d effects, want the one call", len(effects))
	}

	if effects[0].Stubbed {
		t.Error("the next run was answered from a frozen reply, want it to capture")
	}
}

// A service that stopped by itself before any consumer existed is E11 however the wait for it ended: the
// discard reads an exit that landed after the startup limit ended the wait.
func TestAServiceThatStoppedByItselfIsE11HoweverTheWaitEnded(t *testing.T) {
	t.Parallel()

	stopped := Discovery{Exit: replay.Exit{Code: 1, Exited: true}}
	if err := stopped.verdict(); !errors.Is(err, ErrExitedBeforeConsumer) {
		t.Errorf("verdict() of a service that stopped by itself = %v, want %v", err, ErrExitedBeforeConsumer)
	}

	running := Discovery{Exit: replay.Exit{Code: 137}}
	if err := running.verdict(); err != nil {
		t.Errorf("verdict() of a service Stutter stopped = %v, want none: the check runs unnamed", err)
	}
}
