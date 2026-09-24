//go:build linux

package enginetest_test

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/testexec"
)

const (
	// helperMode turns this test binary into a helper process when set in its environment. A helper
	// is the process a test kills or signals, standing in for a stutter invocation.
	helperMode = "STUTTER_PROVISION_HELPER"
	// specVar names the file a helper reads its spec from.
	specVar = "STUTTER_HELPER_SPEC"
	// testImage is the image the engine suite runs: it has a shell and declares a VOLUME.
	testImage = "postgres:18-alpine"
	// testService is the compose service every test container serves.
	testService = "worker"
	// serviceRole is the role of a test's first network.
	serviceRole = "service"
	// subnetAttempts bounds how often a test retries a subnet another network took meanwhile.
	subnetAttempts = 8
	// exitLimit bounds how long a test waits for a container to exit by itself.
	exitLimit = time.Minute
	// logName is the invocation log's name in the check-private directory.
	logName = "invocation.log"
	// logsDir is where the check-private directory keeps container logs.
	logsDir = "logs"
)

// testSubnets is the range the engine suite's networks come from: no host interface uses it.
var testSubnets = netip.MustParsePrefix("10.231.0.0/16")

func TestMain(m *testing.M) {
	if mode := os.Getenv(helperMode); mode != "" {
		// The helper exits with its mode's code; m.Run never runs in it.
		os.Exit(runHelper(mode))
	}

	dockertest.Main(m)
}

// writeShim writes an executable `docker` script into dir.
func writeShim(t *testing.T, dir, script string) {
	t.Helper()

	testexec.WriteScript(t, filepath.Join(dir, "docker"), script)
}

// startProcess re-executes this test binary as a helper in mode, with exactly env as its
// environment, and returns it with its stdin and a reader over its stdout. The helper is killed and
// reaped when the test ends.
func startProcess(t *testing.T, mode string, env []string) (*exec.Cmd, io.WriteCloser, *bufio.Reader) {
	t.Helper()

	//nolint:gosec // re-executes this test binary; the only argument is fixed.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")

	cmd.Env = append(append([]string(nil), env...), helperMode+"="+mode)
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("helper stdin: %v", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("helper stdout: %v", err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}

	t.Cleanup(func() {
		if cmd.ProcessState != nil {
			return
		}

		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("kill helper %s: %v", mode, err)
		}

		if err := cmd.Wait(); err != nil {
			t.Logf("helper %s ended: %v", mode, err)
		}
	})

	return cmd, stdin, bufio.NewReader(stdout)
}

// startHelper is startProcess for a helper that reads nothing.
func startHelper(t *testing.T, mode string, env []string) (*exec.Cmd, *bufio.Reader) {
	t.Helper()

	cmd, _, stdout := startProcess(t, mode, env)

	return cmd, stdout
}

// helperSpec is the check a helper opens and what it does there, written by the parent.
type helperSpec struct {
	Env      map[string]string `json:"env"`
	Image    string            `json:"image"`
	StateDir string            `json:"state_dir"`
	TempDir  string            `json:"temp_dir"`
	Dir      string            `json:"dir"`
	FIFO     string            `json:"fifo"`
	Unset    []string          `json:"unset"`
	Keep     bool              `json:"keep"`
}

// helper is a helper process that opened a check: a stutter invocation the test drives, signals
// or kills.
type helper struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	out   *bufio.Reader
	check string
}

// launch writes spec, starts a helper in mode with exactly env, and reads the check ID it opened.
// The check is cleaned — the way a user would, `stutter clean --check` — once the helper is gone.
func launch(t *testing.T, mode string, spec helperSpec, env []string) *helper {
	t.Helper()

	path := filepath.Join(t.TempDir(), "spec.json")

	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	h := &helper{}

	// Registered before the process starts, so it runs after the process is killed: a live
	// helper's ledger is locked, and a clean refuses it.
	t.Cleanup(func() {
		if h.check != "" {
			cleanCheck(t, spec.StateDir, h.check)
		}
	})

	h.cmd, h.stdin, h.out = startProcess(t, mode, append(env, specVar+"="+path))
	h.check = h.await(t, "check")

	return h
}

// await reads the helper's report until a line starting with key, and returns the rest of it.
func (h *helper) await(t *testing.T, key string) string {
	t.Helper()

	for {
		line, err := h.out.ReadString('\n')
		if k, v, _ := strings.Cut(strings.TrimSpace(line), " "); k == key {
			return v
		}

		if err != nil {
			t.Fatalf("the helper ended before reporting %q: %v", key, err)
		}
	}
}

// collect reads the helper's report through the first line starting with last, and returns every
// value it reported, by key.
func (h *helper) collect(t *testing.T, last string) map[string][]string {
	t.Helper()

	out := map[string][]string{}

	for {
		line, err := h.out.ReadString('\n')
		if k, v, _ := strings.Cut(strings.TrimSpace(line), " "); k != "" {
			out[k] = append(out[k], v)

			if k == last {
				return out
			}
		}

		if err != nil {
			t.Fatalf("the helper ended before reporting %q: %v (reported %v)", last, err, out)
		}
	}
}

// kill ends the helper with SIGKILL, as a crash would: nothing it holds is released but its locks.
func (h *helper) kill(t *testing.T) {
	t.Helper()

	if err := h.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill the helper: %v", err)
	}

	if err := h.cmd.Wait(); err == nil {
		t.Fatal("the helper exited cleanly; it was meant to die by SIGKILL")
	}
}

// miniResult is what a mini helper reported.
type miniResult struct {
	check     string
	private   string
	container string
}

// runMiniHelper runs a mini helper on spec with exactly env and returns what it reported: a kept
// check with one created target container.
func runMiniHelper(t *testing.T, spec helperSpec, env []string) miniResult {
	t.Helper()

	spec.Keep = true
	h := launch(t, "mini", spec, env)
	out := miniResult{check: h.check, private: h.await(t, "private"), container: h.await(t, "container")}

	if err := h.cmd.Wait(); err != nil {
		t.Fatalf("the mini helper failed: %v (reported %+v)", err, out)
	}

	return out
}

// cleanCheck removes a kept check the way a user does: `stutter clean --check <id>`.
func cleanCheck(t *testing.T, stateDir, check string) {
	t.Helper()

	result, err := provision.Clean(context.WithoutCancel(t.Context()), provision.CleanOptions{
		StateDir: stateDir, CheckID: check,
	})
	if err != nil || len(result.Failed) > 0 {
		t.Errorf("clean check %s: %v (failed %+v)", check, err, result.Failed)
	}
}

// engineSlots bounds how many engine tests run at once. Each spawns docker CLIs, helper processes
// and containers on the one engine the whole test run shares; unbounded, the suite's load pushed
// timing-bound tests in other packages past their bounds (measured: a 50 ms hold bound failed 6 and
// 14 times beside it, 0 alone).
var engineSlots = make(chan struct{}, 3)

// slot waits for one of the engine slots and gives it back when the test ends, after its cleanups.
func slot(t *testing.T) {
	t.Helper()

	select {
	case engineSlots <- struct{}{}:
	case <-t.Context().Done():
		t.Fatal("the test ended waiting for an engine slot")
	}

	t.Cleanup(func() { <-engineSlots })
}

// requireEngine is the gate every engine test passes first, then its slot.
func requireEngine(t *testing.T) dockertest.Engine {
	t.Helper()

	engine := dockertest.Require(t)
	slot(t)

	return engine
}

// helperEnv is the environment a helper runs a check in: the docker CLI reachable, the engine
// the gate accepted, and dockerConfig as the user's client configuration.
func helperEnv(engine dockertest.Engine, path, dockerConfig string) []string {
	return []string{"PATH=" + path, "DOCKER_HOST=" + engine.Endpoint(), "DOCKER_CONFIG=" + dockerConfig}
}

// openEngine opens a check against the real engine; empty directories in opts become fresh ones.
// The test ends with it closed, and nothing carrying its label left behind.
func openEngine(t *testing.T, opts provision.Options) *provision.Engine {
	t.Helper()

	if opts.StateDir == "" {
		opts.StateDir = newStateDir(t)
	}

	if opts.TempDir == "" {
		opts.TempDir = t.TempDir()
	}

	engine, err := provision.Open(t.Context(), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	t.Cleanup(func() {
		down := engine.Close(t.Context(), provision.DiscardLogs)
		if len(down.Listing) != 0 && !opts.Keep {
			t.Errorf("the engine still holds %d resources of check %s: %+v", len(down.Listing), engine.CheckID(),
				down.Listing)
		}
	})

	return engine
}

// pickSubnet creates a network for role on the lowest /24 of the suite's range that nothing holds,
// moving on when another network takes it first.
func pickSubnet(ctx context.Context, engine *provision.Engine, role string) (*provision.Network, error) {
	var last error

	for range subnetAttempts {
		subnet, err := freeSubnet(ctx, engine)
		if err != nil {
			return nil, err
		}

		network, err := engine.CreateNetwork(ctx, role, true, subnet)
		if !errors.Is(err, provision.ErrSubnetTaken) {
			return network, err
		}

		last = err
	}

	return nil, last
}

// freeSubnet returns a /24 of the suite's range that no interface or network holds, picked at random
// among the free ones: tests running in parallel would all race for the lowest.
func freeSubnet(ctx context.Context, engine *provision.Engine) (netip.Prefix, error) {
	occupied, err := engine.Occupied(ctx)
	if err != nil {
		return netip.Prefix{}, err
	}

	base := testSubnets.Addr().As4()

	var free []netip.Prefix

	for third := range 256 {
		candidate := netip.PrefixFrom(netip.AddrFrom4([4]byte{base[0], base[1], byte(third), 0}), 24)
		if !slices.ContainsFunc(occupied, func(o provision.Occupied) bool { return o.Prefix.Overlaps(candidate) }) {
			free = append(free, candidate)
		}
	}

	if len(free) == 0 {
		return netip.Prefix{}, fmt.Errorf("no free /24 in %s", testSubnets)
	}

	pick, err := rand.Int(rand.Reader, big.NewInt(int64(len(free))))
	if err != nil {
		return netip.Prefix{}, err
	}

	return free[pick.Int64()], nil
}

// freeNetwork is pickSubnet for a test.
func freeNetwork(t *testing.T, engine *provision.Engine, role string) *provision.Network {
	t.Helper()

	network, err := pickSubnet(t.Context(), engine, role)
	if err != nil {
		t.Fatalf("create a network: %v", err)
	}

	return network
}

// pinImage resolves ref.
func pinImage(t *testing.T, engine *provision.Engine, ref string) compose.Image {
	t.Helper()

	image, err := engine.ResolveImage(t.Context(), ref, "")
	if err != nil {
		t.Fatalf("ResolveImage(%s): %v", ref, err)
	}

	return image
}

// targetSpec is a target container of image on network, running the image's own process.
func targetSpec(image compose.Image, network *provision.Network) provision.ContainerSpec {
	return provision.ContainerSpec{
		Kind: rules.KindTarget, Service: testService, Spec: compose.Spec{Image: image.ID, Env: map[string]string{}},
		Networks: []provision.NetworkAttach{{Network: network, Aliases: []string{testService}}},
	}
}

// shell makes a spec run script under /bin/sh as its process.
func shell(spec provision.ContainerSpec, script string) provision.ContainerSpec {
	spec.Spec.Entrypoint, spec.Spec.EntrypointSet = []string{"/bin/sh", "-c"}, true
	spec.Spec.Cmd, spec.Spec.CmdSet = []string{script}, true

	return spec
}

// createContainer creates spec, failing the test on an error.
func createContainer(t *testing.T, engine *provision.Engine, spec provision.ContainerSpec) *provision.Container {
	t.Helper()

	container, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	return container
}

// startContainer starts c, failing the test on an error.
func startContainer(t *testing.T, engine *provision.Engine, c *provision.Container) {
	t.Helper()

	if err := engine.Start(t.Context(), c); err != nil {
		t.Fatalf("Start %s: %v", c.Name(), err)
	}
}

// awaitExit waits for a started container to exit by itself.
func awaitExit(t *testing.T, engine *provision.Engine, c *provision.Container) {
	t.Helper()

	select {
	case <-engine.Exited(c):
	case <-time.After(exitLimit):
		t.Fatalf("%s did not exit within %v", c.Name(), exitLimit)
	}
}

// inspected decodes one object the decoy helper read back from the engine, as the engine names its
// fields.
func inspected(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()

	if raw == nil {
		return nil
	}

	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}

	return out
}

// field walks a decoded inspect by keys.
func field(value any, keys ...string) any {
	for _, key := range keys {
		m, ok := value.(map[string]any)
		if !ok {
			return nil
		}

		value = m[key]
	}

	return value
}

// list is a decoded inspect's array, each element an object.
func list(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}

	out := make([]map[string]any, 0, len(items))

	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}

	return out
}

// texts is a decoded inspect's array of strings.
func texts(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(items))

	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}

	return out
}

// jsonLines decodes every whole JSON object line of the file at path.
func jsonLines(t *testing.T, path string) []map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var out []map[string]any

	for line := range strings.Lines(string(data)) {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err == nil {
			out = append(out, entry)
		}
	}

	return out
}

// newStateDir is a fresh ledger directory: this user's alone, as Open requires.
func newStateDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	//nolint:gosec // a directory needs its search bit; 0700 is Open's own requirement.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	return dir
}

// ledgerPath is a check's ledger in the state directory.
func ledgerPath(stateDir, check string) string {
	return filepath.Join(stateDir, check+".ledger")
}

// lastSeq is the highest sequence number a ledger holds.
func lastSeq(t *testing.T, stateDir, check string) int {
	t.Helper()

	last := 0

	for _, entry := range jsonLines(t, ledgerPath(stateDir, check)) {
		if seq, ok := entry["seq"].(float64); ok && int(seq) > last {
			last = int(seq)
		}
	}

	return last
}

// randomHex returns n random bytes as hex.
func randomHex(n int) string {
	raw := make([]byte, n)
	_, _ = rand.Read(raw) // crypto/rand.Read never returns an error.

	return hex.EncodeToString(raw)
}

// testRef is an image reference no one else uses: under a reserved test domain, random per call.
func testRef(name string) string {
	return "stutter-test.invalid/" + name + ":" + randomHex(8)
}

// layer is a filesystem archive holding one file, for the decoy helper to import as an image.
func layer(t *testing.T, name, content string) io.Reader {
	t.Helper()

	var out bytes.Buffer

	archive := tar.NewWriter(&out)

	if err := archive.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}

	if _, err := archive.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}

	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	return &out
}
