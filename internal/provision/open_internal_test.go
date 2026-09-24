//go:build linux

package provision

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// Open refuses before it writes: the engine is accepted first, then the ledger is placed, then the
// private directory made. A refused engine leaves no ledger at all.
func TestOpenFollowsTheRefusalFirstOrder(t *testing.T) {
	t.Parallel()

	// The order of each step's first appearance.
	var events []string

	note := func(event string) {
		if !slices.Contains(events, event) {
			events = append(events, event)
		}
	}

	host := defaultHostFS()
	realSync, realLstat := host.sync, host.lstat
	host.sync = func(file *os.File) error {
		if strings.HasSuffix(file.Name(), ".ledger.tmp") {
			note("ledger")
		}

		return realSync(file)
	}
	host.lstat = func(path string) (fs.FileInfo, error) {
		if strings.HasPrefix(filepath.Base(path), "stutter-") {
			note("private")
		}

		return realLstat(path)
	}

	fake := &fakeCaller{}
	opts := Options{StateDir: filepath.Join(t.TempDir(), "state"), TempDir: t.TempDir()}

	engine, err := openWith(t.Context(), opts, openDeps{
		admit: func(context.Context) (Identity, engineCaller, error) {
			note("preconditions")

			return testIdentity(), fake, nil
		},
		host: host,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() {
		if released := engine.release(); released != nil {
			t.Log(released)
		}
	})

	if want := []string{"preconditions", "ledger", "private"}; !slices.Equal(events, want) {
		t.Errorf("Open ran %v, want %v", events, want)
	}

	if fake.configDir != filepath.Join(engine.PrivateDir(), "docker-config") || fake.logCall == nil {
		t.Errorf("the runner was not handed the private config directory and log: %q", fake.configDir)
	}

	refused := Options{StateDir: filepath.Join(t.TempDir(), "state"), TempDir: t.TempDir()}

	_, err = openWith(t.Context(), refused, openDeps{
		admit: func(context.Context) (Identity, engineCaller, error) {
			return Identity{}, nil, ErrPrecondition
		},
		host: defaultHostFS(),
	})
	if !errors.Is(err, ErrPrecondition) {
		t.Fatalf("open = %v, want the precondition refusal", err)
	}

	if _, err := os.Lstat(refused.StateDir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a refused engine still made the state directory: %v", err)
	}
}

// A refused Open leaves no ledger for its check ID and no private directory.
func TestARefusedOpenLeavesNoLedger(t *testing.T) {
	t.Parallel()

	wrongMode := defaultHostFS()
	wrongMode.lstat = func(path string) (fs.FileInfo, error) {
		info, err := os.Lstat(path)
		if err == nil && strings.HasPrefix(filepath.Base(path), "stutter-") {
			return modeInfo{FileInfo: info, mode: fs.ModeDir | 0o755}, nil
		}

		return info, err
	}

	cases := []struct {
		admit func(context.Context) (Identity, engineCaller, error)
		name  string
		want  error
		host  hostFS
	}{
		{
			name: "precondition refused", want: ErrPrecondition, host: defaultHostFS(),
			admit: func(context.Context) (Identity, engineCaller, error) {
				return Identity{}, nil, ErrPrecondition
			},
		},
		{
			name: "private directory refused", want: ErrPrivateDir, host: wrongMode,
			admit: func(context.Context) (Identity, engineCaller, error) {
				return testIdentity(), &fakeCaller{}, nil
			},
		},
	}

	for _, tc := range cases {
		state, temp := filepath.Join(t.TempDir(), "state"), t.TempDir()

		opts := Options{StateDir: state, TempDir: temp}

		_, err := openWith(t.Context(), opts, openDeps{admit: tc.admit, host: tc.host})
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: open = %v, want %v", tc.name, err, tc.want)
		}

		left := append(globbed(t, filepath.Join(state, "*.ledger*")), globbed(t, filepath.Join(temp, "stutter-*"))...)
		if len(left) != 0 {
			t.Errorf("%s: a refused Open left %v", tc.name, left)
		}
	}
}

func globbed(t *testing.T, pattern string) []string {
	t.Helper()

	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}

	return matches
}

// Compose's stderr can echo interpolated secrets: a failed config read is reported by exit code and
// line count only, in the error and in the log alike.
func TestComposeConfigNeverQuotesItsStderr(t *testing.T) {
	t.Parallel()

	const sentinel = "SENTINEL-interpolated-7c3e"

	engine := openShimEngine(t, "compose) printf '%s\\n' '"+sentinel+"' >&2; exit 1 ;;\n")
	dir := t.TempDir()

	_, lines, err := engine.ComposeConfig(t.Context(), dir, []string{"-f", "compose.yaml"}, compose.ConfigRead{})

	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.ExitCode() != 1 || callErr.Stderr != "" {
		t.Fatalf("ComposeConfig = %v, want a *CallError with exit 1 and no stderr", err)
	}

	if strings.Contains(err.Error(), sentinel) || lines != 1 {
		t.Errorf("error %q, stderr lines %d: want the sentinel unquoted and one line counted", err, lines)
	}

	for _, read := range []compose.ConfigRead{{Environment: true}, {Service: "worker"}} {
		if _, _, err := engine.ComposeConfig(t.Context(), dir, []string{"-f", "compose.yaml"}, read); err == nil {
			t.Fatalf("ComposeConfig(%+v) succeeded against a failing shim", read)
		}
	}

	if log := readFile(t, filepath.Join(engine.PrivateDir(), invocationLog)); strings.Contains(log, sentinel) {
		t.Errorf("the invocation log quotes compose's stderr:\n%s", log)
	}

	argv := readLines(t, engine.argv)
	want := []string{
		"compose -f compose.yaml config --format json",
		"compose -f compose.yaml config --environment",
		"compose -f compose.yaml config --format json worker",
	}

	if got := argv[len(argv)-3:]; !slices.Equal(got, want) {
		t.Errorf("compose argv = %q, want %q", got, want)
	}
}
