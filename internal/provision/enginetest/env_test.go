//go:build linux

package enginetest_test

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
)

// proxyNames are the eight variables that route a process's HTTP through a proxy.
func proxyNames() []string {
	return []string{
		"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy",
	}
}

// containerEnv reads a container's `.Config.Env` from the engine.
func containerEnv(t *testing.T, docker *dockertest.Docker, id string) []string {
	t.Helper()

	return texts(field(inspected(t, docker.Inspect(t, dockertest.ObjectContainer, id)), "Config", "Env"))
}

// Every value reaches the container exactly as given: an env file on stdin takes a line literally,
// and a multi-line value travels in the CLI's own environment instead.
func TestEnvironmentReachesTheContainerByteForByte(t *testing.T) {
	t.Parallel()

	docker := dockertest.Require(t).Docker(t)
	engine := openEngine(t, provision.Options{})

	want := map[string]string{
		"LEADING":  "  lead",
		"TRAILING": "trail  ",
		"EQUALS":   "a=b=c",
		"HASH":     "# not a comment",
		"QUOTES":   `"q" 'q'`,
		"DOLLAR":   "$HOME ${X}",
		"UNICODE":  "zażółć ✓",
		"EMPTY":    "",
		"MULTI":    "line one\nline two",
	}

	spec := targetSpec(pinImage(t, engine, testImage), freeNetwork(t, engine, serviceRole))
	spec.Spec.Env = want

	got := map[string]string{}

	for _, entry := range containerEnv(t, docker, createContainer(t, engine, spec).ID()) {
		if key, value, ok := strings.Cut(entry, "="); ok {
			got[key] = value
		}
	}

	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s reads %q, want %q", key, got[key], value)
		}
	}
}

// No proxy variable reaches a target, from any of the three places one comes from: the user's
// client configuration, the image, or compose.
func TestNoProxyVariableReachesTheTarget(t *testing.T) {
	t.Parallel()

	gate := dockertest.Require(t)
	docker := gate.Docker(t)

	image := testRef("proxied")
	docker.Import(t, layer(t, "a", "A"), image, append(runnable(),
		"ENV HTTP_PROXY=http://image.invalid:3128", "ENV https_proxy=http://image.invalid:3128"))

	userConfig := t.TempDir()
	clientHost := "client.invalid"
	proxy := "http://" + clientHost + ":3128"

	config, err := json.Marshal(map[string]any{"proxies": map[string]any{"default": map[string]string{
		"httpProxy": proxy, "httpsProxy": proxy, "ftpProxy": proxy, "allProxy": proxy, "noProxy": clientHost,
	}}})
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(userConfig, "config.json"), config, 0o600); err != nil {
		t.Fatal(err)
	}

	kept := runMiniHelper(t, miniSpec{
		Image: image, StateDir: newStateDir(t), TempDir: t.TempDir(),
		Env: map[string]string{"KEEP": "1"}, Unset: proxyNames(),
	}, helperEnv(gate, os.Getenv("PATH"), userConfig))

	env := containerEnv(t, docker, kept.container)
	t.Logf("entries compared=%d", len(env))

	if len(env) == 0 {
		t.Fatal("the kept container has no environment to compare")
	}

	// The client injects a proxy variable the eight do not name, so its value is looked for too.
	for _, entry := range env {
		key, value, set := strings.Cut(entry, "=")
		if set && (slices.Contains(proxyNames(), key) || strings.Contains(value, clientHost)) {
			t.Errorf("the target carries %s", entry)
		}
	}
}

// sentinelHits counts the sentinels' lines found in data.
func sentinelHits(data []byte, sentinels map[string]string) int {
	hits := 0

	for _, value := range sentinels {
		for line := range strings.Lines(value) {
			if bytes.Contains(data, []byte(strings.TrimSuffix(line, "\n"))) {
				hits++
			}
		}
	}

	return hits
}

// dockerCLI finds the docker CLI on PATH the way a shell does, so the shim can hand every call on.
func dockerCLI(t *testing.T) string {
	t.Helper()

	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		path := filepath.Join(dir, "docker")
		//nolint:gosec // PATH is the test's own environment; the search only reads file modes.
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
			return path
		}
	}

	t.Fatal("no docker CLI on PATH")

	return ""
}

// No environment value is ever in the argv of a docker call: every call a check makes passes through
// a shim first on PATH that records its argv, and neither that nor anything the check kept holds a
// sentinel.
func TestNoEnvironmentValueReachesArgv(t *testing.T) {
	t.Parallel()

	gate := dockertest.Require(t)

	dockerPath := dockerCLI(t)
	shimDir := t.TempDir()
	argvLog := filepath.Join(t.TempDir(), "argv")
	writeShim(t, shimDir, "#!/bin/sh\n{ for a in \"$@\"; do printf '%s\\0' \"$a\"; done; printf '\\n'; } >> '"+
		argvLog+"'\nexec '"+dockerPath+"' \"$@\"\n")

	sentinels := map[string]string{
		"SENTINEL_ONE":   randomHex(16),
		"SENTINEL_TWO":   randomHex(16),
		"SENTINEL_MULTI": randomHex(16) + "\n" + randomHex(16),
	}

	stateDir := newStateDir(t)
	kept := runMiniHelper(t, miniSpec{Image: testImage, StateDir: stateDir, TempDir: t.TempDir(), Env: sentinels},
		helperEnv(gate, shimDir+":"+os.Getenv("PATH"), t.TempDir()))

	shimmed, err := os.ReadFile(argvLog)
	if err != nil {
		t.Fatalf("the shim saw no call: %v", err)
	}

	hits := sentinelHits(shimmed, sentinels)
	scannedArgv := bytes.Count(shimmed, []byte("\n"))
	scannedFiles := 1

	root, err := os.OpenRoot(kept.private)
	if err != nil {
		t.Fatal(err)
	}

	defer root.Close()

	files := root.FS()

	walkErr := fs.WalkDir(files, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}

		data, err := fs.ReadFile(files, path)
		if err != nil {
			return err
		}

		scannedFiles++
		hits += sentinelHits(data, sentinels)

		if path == logName {
			scannedArgv += bytes.Count(data, []byte(`"kind":"call"`))
		}

		return nil
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}

	t.Logf("STUTTER-AUDIT secrets check=%s sentinel-hits=%d scanned-files=%d scanned-argv=%d", kept.check, hits,
		scannedFiles, scannedArgv)

	if hits != 0 || scannedFiles < 2 || scannedArgv == 0 {
		t.Errorf("sentinel-hits=%d scanned-files=%d scanned-argv=%d: want no hit over a kept log and the shim",
			hits, scannedFiles, scannedArgv)
	}
}
