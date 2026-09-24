package dockertest

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// ComposeFloorVariable names the compose plugin binary of the oldest release Stutter supports. make
// test downloads it, verifies its checksum, and sets this for the test process. No product code
// reads it.
const ComposeFloorVariable = "STUTTER_TEST_COMPOSE_FLOOR"

// privateDir is the mode of a directory only the test's user may read.
const privateDir = 0o700

// ComposeFloor returns a fresh DOCKER_CONFIG directory whose compose plugin is the oldest supported
// release, which then replaces the installed one for any docker command run with it. A private
// DOCKER_CONFIG also hides the user's contexts, so the caller sets DOCKER_HOST for what it runs.
// Without the plugin the test FAILS; it never skips.
func (Engine) ComposeFloor(tb testing.TB) string {
	tb.Helper()

	return floorConfig(tb, os.Getenv(ComposeFloorVariable), tb.TempDir())
}

// floorConfig makes dir a DOCKER_CONFIG whose compose plugin is plugin, or fails r saying which of
// unset, absent or unreadable the plugin is.
func floorConfig(r reporter, plugin, dir string) string {
	r.Helper()

	if plugin == "" {
		r.Fatalf("%s is not set: run the tests through make test, or run make compose-floor DEST=<dir> and set "+
			"%s=<dir>/docker-compose", ComposeFloorVariable, ComposeFloorVariable)
	}

	info, err := os.Stat(plugin) //nolint:gosec // the test environment names the plugin; nothing else reads it

	switch {
	case errors.Is(err, fs.ErrNotExist):
		r.Fatalf("%s=%s: absent; make compose-floor installs it", ComposeFloorVariable, plugin)
	case err != nil:
		r.Fatalf("%s=%s: unreadable: %v", ComposeFloorVariable, plugin, err)
	case !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0:
		r.Fatalf("%s=%s: unreadable: not an executable file (%s)", ComposeFloorVariable, plugin, info.Mode())
	default:
	}

	target, err := filepath.Abs(plugin)
	if err != nil {
		r.Fatalf("%s=%s: %v", ComposeFloorVariable, plugin, err)
	}

	plugins := filepath.Join(dir, "cli-plugins")
	if err := os.MkdirAll(plugins, privateDir); err != nil {
		r.Fatalf("making %s: %v", plugins, err)
	}

	if err := os.Symlink(target, filepath.Join(plugins, "docker-compose")); err != nil {
		r.Fatalf("linking the compose floor into %s: %v", plugins, err)
	}

	return dir
}
