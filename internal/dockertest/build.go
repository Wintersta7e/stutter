package dockertest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// buildOnly are the variables a Docker test's own process may carry that must never reach the build
// of the binary it runs: toolchain flags and targets, coverage, the cgo switch, and an outer make's
// command line, which would pass its own CGO=1 through to the inner make.
func buildOnly() []string {
	return []string{
		"GOFLAGS", "GOOS", "GOARCH", "GOCOVERDIR", "CGO_ENABLED", "MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES",
		"MAKELEVEL",
	}
}

// errNoModule means no go.mod was found above the directory the tests run in.
var errNoModule = errors.New("no go.mod")

// makeRunner runs make in root with env and args, and returns its combined output.
type makeRunner func(ctx context.Context, root string, env []string, args ...string) ([]byte, error)

// built is one package's build: its path, or the error every caller then fails with.
type built struct {
	err  error
	path string
	once sync.Once
}

// builder builds each package once, into one directory Main removes.
type builder struct {
	results map[string]*built
	run     makeRunner
	// installed reports whether the package's TestMain called Main.
	installed func() bool
	dir       string
	mu        sync.Mutex
}

//nolint:gochecknoglobals // one build directory and one build per package, per test process.
var processBuilder = newBuilder(runMake, mainInstalled.Load)

// newBuilder returns a builder that runs make through run, once installed reports Main ran.
func newBuilder(run makeRunner, installed func() bool) *builder {
	return &builder{results: map[string]*built{}, run: run, installed: installed}
}

// Binary returns the static binary of pkg, a package path relative to the module root, built by the
// Makefile's build recipe for Linux on the engine's architecture. Each package is built once per
// test process; a failed build FAILS every test that asks for it, and it is never skipped. The
// package's TestMain must call Main, which removes the binaries when the tests end.
func (e Engine) Binary(tb testing.TB, pkg string) string {
	tb.Helper()

	return processBuilder.binary(tb, pkg, e.Arch())
}

// binary builds pkg once, or fails r with the first build's error.
func (b *builder) binary(r reporter, pkg, arch string) string {
	r.Helper()

	if !b.installed() {
		r.Fatalf("building %s needs the package's TestMain to call dockertest.Main(m), which removes the "+
			"binaries when the tests end", pkg)
	}

	b.mu.Lock()
	result, ok := b.results[pkg]

	if !ok {
		result = &built{}
		b.results[pkg] = result
	}

	index := len(b.results)
	b.mu.Unlock()

	result.once.Do(func() {
		// Detached from the first caller: a test that ends early must not fail every later one.
		result.path, result.err = b.build(context.WithoutCancel(r.Context()), pkg, arch, index)
	})

	if result.err != nil {
		r.Fatalf("building %s failed, and every test that needs it fails: %v", pkg, result.err)
	}

	return result.path
}

// build runs the Makefile's build recipe for pkg into its own directory.
func (b *builder) build(ctx context.Context, pkg, arch string, index int) (string, error) {
	start, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("reading the working directory: %w", err)
	}

	root, err := moduleRoot(start)
	if err != nil {
		return "", err
	}

	dir, err := b.directory()
	if err != nil {
		return "", err
	}

	out := filepath.Join(dir, strconv.Itoa(index), path.Base(pkg))

	output, err := b.run(ctx, root, childEnv(os.Environ(), arch), "build", "BIN="+out, "PKG="+pkg)
	if err != nil {
		return "", fmt.Errorf("make build PKG=%s: %w\n%s", pkg, err, output)
	}

	return out, nil
}

// directory returns the process's build directory, making it on first use.
func (b *builder) directory() (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.dir == "" {
		dir, err := os.MkdirTemp("", "stutter-build")
		if err != nil {
			return "", fmt.Errorf("making the build directory: %w", err)
		}

		b.dir = dir
	}

	return b.dir, nil
}

// remove removes the build directory, if one was made.
func (b *builder) remove() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.dir != "" {
		_ = os.RemoveAll(b.dir)
		b.dir = ""
	}
}

// childEnv is env without the variables that must not reach the build, targeting Linux on arch.
func childEnv(env []string, arch string) []string {
	target := []string{"GOOS=linux", "GOARCH=" + arch}
	out := make([]string, 0, len(env)+len(target))

	for _, pair := range env {
		if name, _, _ := strings.Cut(pair, "="); !slices.Contains(buildOnly(), name) {
			out = append(out, pair)
		}
	}

	return append(out, target...)
}

// moduleRoot walks up from start to the directory holding go.mod. None found and one that cannot be
// read are different failures.
func moduleRoot(start string) (string, error) {
	for dir := start; ; dir = filepath.Dir(dir) {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))

		switch {
		case err == nil:
			return dir, nil
		case !errors.Is(err, fs.ErrNotExist):
			return "", fmt.Errorf("reading %s: %w", filepath.Join(dir, "go.mod"), err)
		case filepath.Dir(dir) == dir:
			return "", fmt.Errorf("%w in %s or any directory above it", errNoModule, start)
		default:
		}
	}
}

// runMake runs make in root. It is this file's one spawn: the Makefile's build recipe, and nothing
// else, builds the product.
func runMake(ctx context.Context, root string, env []string, args ...string) ([]byte, error) {
	argv := append([]string{"-C", root}, args...)
	cmd := exec.CommandContext(ctx, "make", argv...)
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("make: %w", err)
	}

	return output, nil
}
