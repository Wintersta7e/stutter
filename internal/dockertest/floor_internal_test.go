package dockertest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

func TestAnUnsetComposeFloorFailsTheTest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	notExecutable := filepath.Join(dir, "docker-compose")

	if err := os.WriteFile(notExecutable, []byte("not a plugin"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		plugin string
		want   string
	}{
		{name: "unset", plugin: "", want: "make compose-floor"},
		{name: "absent", plugin: filepath.Join(dir, "missing", "docker-compose"), want: "absent"},
		{name: "not executable", plugin: notExecutable, want: "unreadable"},
	}

	seen := map[string]bool{}

	for _, tc := range cases {
		r := &fakeReporter{parent: t}

		outcome := outcomeOf(func() { floorConfig(r, tc.plugin, t.TempDir()) })
		if outcome != outcomeFatal || !strings.Contains(r.message, tc.want) ||
			!strings.Contains(r.message, ComposeFloorVariable) {
			t.Errorf("%s: %s (%q); want a failure naming %s and %q", tc.name, outcome, r.message,
				ComposeFloorVariable, tc.want)
		}

		if seen[r.message] {
			t.Errorf("%s: the same failure as another case: %q", tc.name, r.message)
		}

		seen[r.message] = true
	}
}

func TestTheComposeFloorReplacesThePluginDockerInfoReports(t *testing.T) {
	t.Parallel()

	engine := Require(t)
	config := engine.ComposeFloor(t)

	path, version := engine.Docker(t).Plugin(t, config, "compose")
	if !strings.HasPrefix(path, config+string(filepath.Separator)) || version != "v"+compose.MinVersion {
		t.Fatalf("docker info reports compose %s at %s; want v%s under %s", version, path, compose.MinVersion,
			config)
	}
}
