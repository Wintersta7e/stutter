//go:build linux

package provision

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// testService is the compose service tests name.
const testService = "worker"

// fakeCaller is a scripted engine: it records every request and answers each from answer.
type fakeCaller struct {
	answer    func(req request) (result, error)
	logCall   func(callLine)
	configDir string
	calls     []request
	mu        sync.Mutex
}

func (f *fakeCaller) call(_ context.Context, req request) (result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, req)
	f.mu.Unlock()

	if f.answer == nil {
		return result{}, nil
	}

	return f.answer(req)
}

func (f *fakeCaller) attach(configDir string, logCall func(callLine), _ bool) {
	f.configDir, f.logCall = configDir, logCall
}

func testIdentity() Identity {
	return Identity{
		CLIPath: "/usr/bin/docker", EngineID: "engine-a", Endpoint: "unix:///var/run/docker.sock",
		Context: "default", OS: "linux", Arch: "amd64",
	}
}

// shimEngine is an Engine opened against a docker shim, with the files a test inspects.
type shimEngine struct {
	*Engine

	argv  string
	state string
	temp  string
}

// openShimEngine opens an Engine whose runner spawns a docker shim answering the preconditions and,
// through extra, whatever else the test calls. The Engine is released when the test ends.
func openShimEngine(t *testing.T, extra string) shimEngine {
	t.Helper()

	shimDir := t.TempDir()
	out := shimEngine{
		argv: filepath.Join(t.TempDir(), "argv"), state: filepath.Join(t.TempDir(), "state"), temp: t.TempDir(),
	}

	answers := healthyAnswers()
	answers.extra = extra
	writeShim(t, shimDir, answers.script())

	env := []string{"PATH=" + shimDir + ":/usr/bin:/bin", argvLogVar + "=" + out.argv}
	root := t.TempDir()

	engine, err := openWith(t.Context(), Options{StateDir: out.state, TempDir: out.temp}, openDeps{
		admit: func(ctx context.Context) (Identity, engineCaller, error) {
			identity, runner, err := preconditionsIn(ctx, env, root)
			if err != nil {
				return Identity{}, nil, err
			}

			return identity, runner, nil
		},
		env:  env,
		host: defaultHostFS(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() {
		if err := engine.release(); err != nil {
			t.Logf("release: %v", err)
		}
	})

	out.Engine = engine

	return out
}
