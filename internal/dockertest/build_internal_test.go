package dockertest

import (
	"context"
	"debug/elf"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// outcomeOf runs fn, which reports through a fake reporter, and returns how it ended: "fatal",
// "skip" or "run".
func outcomeOf(fn func()) string {
	outcome := outcomeRun

	func() {
		defer func() {
			if v := recover(); v != nil {
				s, ok := v.(stopped)
				if !ok {
					panic(v)
				}

				outcome = s.outcome
			}
		}()

		fn()
	}()

	return outcome
}

func installed() bool { return true }

func notInstalled() bool { return false }

// countingMake is the real make runner, counted.
func countingMake(runs *atomic.Int64) makeRunner {
	return func(ctx context.Context, root string, env []string, args ...string) ([]byte, error) {
		runs.Add(1)

		return runMake(ctx, root, env, args...)
	}
}

// interpHeaders counts the ELF program headers that name an interpreter.
func interpHeaders(t *testing.T, path string) int {
	t.Helper()

	f, err := elf.Open(path)
	if err != nil {
		t.Fatalf("reading %s as ELF: %v", path, err)
	}

	defer func() { _ = f.Close() }()

	n := 0

	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			n++
		}
	}

	return n
}

func TestAFailedBuildFailsTheTest(t *testing.T) {
	t.Parallel()

	var runs atomic.Int64

	b := newBuilder(countingMake(&runs), installed)
	t.Cleanup(b.remove)

	for attempt := 1; attempt <= 2; attempt++ {
		r := &fakeReporter{parent: t}

		outcome := outcomeOf(func() { b.binary(r, "./does/not/exist", runtime.GOARCH) })
		if outcome != outcomeFatal || !strings.Contains(r.message, "does/not/exist") {
			t.Fatalf("attempt %d: %s (%q); want a failure carrying make's output", attempt, outcome, r.message)
		}
	}

	if n := runs.Load(); n != 1 {
		t.Fatalf("make ran %d times for one package; a failed build is remembered, not retried", n)
	}
}

func TestTheBuildHelperNeedsMain(t *testing.T) {
	t.Parallel()

	var runs atomic.Int64

	b := newBuilder(countingMake(&runs), notInstalled)
	t.Cleanup(b.remove)

	r := &fakeReporter{parent: t}

	outcome := outcomeOf(func() { b.binary(r, "./cmd/stutter", runtime.GOARCH) })
	if outcome != outcomeFatal || !strings.Contains(r.message, "dockertest.Main") || runs.Load() != 0 {
		t.Fatalf("without Main: %s (%q) after %d builds; want a failure naming dockertest.Main and no build",
			outcome, r.message, runs.Load())
	}
}

func TestTheBuildDefaultIsStatic(t *testing.T) {
	t.Parallel()

	start, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	root, err := moduleRoot(start)
	if err != nil {
		t.Fatal(err)
	}

	makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}

	scan := testgate.ScanBuilds([]testgate.SourceFile{{Path: "Makefile", Content: makefile}})
	if scan.CgoDefault != "0" || len(scan.CgoAssignments) != 0 {
		t.Errorf("the Makefile's cgo default is %q with %d other settings; want exactly CGO = 0", scan.CgoDefault,
			len(scan.CgoAssignments))
	}

	// A CGO in the environment must not reach the build; one on make's command line must.
	env := append(childEnv(os.Environ(), runtime.GOARCH), "CGO=1")
	dir := t.TempDir()

	for _, c := range []struct {
		name   string
		args   []string
		interp int
	}{
		{name: "env", args: nil, interp: 0},
		{name: "command-line", args: []string{"CGO=1"}, interp: 1},
	} {
		out := filepath.Join(dir, c.name, "stutter")

		output, err := runMake(t.Context(), root, env, append([]string{"build", "BIN=" + out}, c.args...)...)
		if err != nil {
			t.Fatalf("%s: make build: %v\n%s", c.name, err, output)
		}

		if got := interpHeaders(t, out); got != c.interp {
			t.Errorf("%s CGO=1: %d interpreter headers, want %d", c.name, got, c.interp)
		}
	}
}
