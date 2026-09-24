//go:build linux

package enginetest_test

import (
	"bytes"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// crashed runs a crash helper in stateDir and kills it once it holds a network, a volume and a
// container: a check that died. It returns the helper and its container's ID.
func crashed(t *testing.T, gate dockertest.Engine, stateDir string) (*helper, string) {
	t.Helper()

	h := launch(t, "crash", helperSpec{StateDir: stateDir, TempDir: t.TempDir()},
		helperEnv(gate, os.Getenv("PATH"), t.TempDir()))
	container := h.await(t, "container")
	h.await(t, "ready")
	h.kill(t)

	return h, container
}

// decoyContainer starts a test container carrying another check's labels, and returns its ID and
// when it started.
func decoyContainer(t *testing.T, docker *dockertest.Docker) (string, any) {
	t.Helper()

	decoy := docker.Create(t, dockertest.CreateSpec{
		Image:  testImage,
		Labels: map[string]string{rules.LabelCheck: randomHex(16), rules.LabelKind: string(rules.KindTarget)},
		Cmd:    []string{"sleep", "600"},
	})
	docker.Start(t, decoy)

	return decoy, startedAt(t, docker, decoy)
}

// startedAt reads when a container started, or nil when the engine no longer holds it.
func startedAt(t *testing.T, docker *dockertest.Docker, id string) any {
	t.Helper()

	return field(inspected(t, docker.Inspect(t, dockertest.ObjectContainer, id)), "State", "StartedAt")
}

// checkContainers lists the containers carrying check's label.
func checkContainers(t *testing.T, docker *dockertest.Docker, check string) []string {
	t.Helper()

	return docker.Listing(t, rules.LabelCheck+"="+check).Containers
}

// A ledger edited to name someone else's container never removes it: the sweep reads the container
// before removing it, finds another check's labels, and reports it instead.
func TestAnEditedLedgerNeverRemovesADecoy(t *testing.T) {
	t.Parallel()

	gate := dockertest.Require(t)
	docker := gate.Docker(t)
	stateDir := newStateDir(t)

	dead, own := crashed(t, gate, stateDir)
	decoy, started := decoyContainer(t, docker)

	path := ledgerPath(stateDir, dead.check)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	edited := strings.Replace(string(data), `"id":"`+own+`"`, `"id":"`+decoy+`"`, 1)
	if edited == string(data) {
		t.Fatalf("the ledger names no container %s", own)
	}

	writeLedger(t, path, []byte(edited))

	// Put the real ID back before the dead check is cleaned, so the clean can remove what it made.
	t.Cleanup(func() {
		current, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}

		writeLedger(t, path, []byte(strings.ReplaceAll(string(current), decoy, own)))
	})

	sweep := openEngine(t, provision.Options{StateDir: stateDir}).Sweep()
	t.Logf("swept %v, failed %+v", sweep.Swept, sweep.Failed)

	if now := startedAt(t, docker, decoy); now == nil || now != started {
		t.Errorf("the decoy %s was touched: started %v, now %v", decoy, started, now)
	}

	if !slices.ContainsFunc(sweep.Failed, func(l provision.Listed) bool { return l.ID == decoy }) {
		t.Errorf("the sweep did not report the decoy %s it refused to remove", decoy)
	}
}

// setHeader rewrites one field of a ledger's header line.
func setHeader(t *testing.T, path, key string, value any) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	first, rest, _ := bytes.Cut(data, []byte("\n"))

	decoder := json.NewDecoder(bytes.NewReader(first))
	decoder.UseNumber()

	var header map[string]any
	if err = decoder.Decode(&header); err != nil {
		t.Fatal(err)
	}

	header[key] = value

	line, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}

	writeLedger(t, path, slices.Concat(line, []byte("\n"), rest))
}

// writeLedger replaces a ledger's content, as a user editing it would.
func writeLedger(t *testing.T, path string, data []byte) {
	t.Helper()

	//nolint:gosec // the ledger is in the test's own state directory.
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// Only a check provably dead is swept: not one whose owner still holds its lock, even when its boot
// says otherwise, and not one whose PID belongs to another namespace, where a PID proves nothing.
func TestOnlyADeadCheckIsSwept(t *testing.T) {
	t.Parallel()

	gate := dockertest.Require(t)
	docker := gate.Docker(t)
	stateDir := newStateDir(t)

	live := launch(t, "lock", helperSpec{StateDir: stateDir, TempDir: t.TempDir()},
		helperEnv(gate, os.Getenv("PATH"), t.TempDir()))
	live.await(t, "ready")
	// As a ledger from another boot reads — a directory shared across machines — so its PID says
	// nothing and only the lock its owner holds keeps it.
	setHeader(t, ledgerPath(stateDir, live.check), "boot_id", "another-boot")

	// Each helper's own Open sweeps the ledgers already here, so the dead check comes last: made
	// before the foreign ledger's edit, it would be swept by that helper instead.
	foreign, _ := crashed(t, gate, stateDir)
	setHeader(t, ledgerPath(stateDir, foreign.check), "pid_ns", "pid:[1]")
	dead, _ := crashed(t, gate, stateDir)

	sweep := openEngine(t, provision.Options{StateDir: stateDir}).Sweep()
	t.Logf("eligible=%d skipped=%d", len(sweep.Swept), len(sweep.Skipped))

	for _, c := range []struct {
		name  string
		check string
		left  int
	}{
		{name: "the dead check", check: dead.check},
		{name: "the check whose owner holds its lock", check: live.check, left: 1},
		{name: "the check from another PID namespace", check: foreign.check, left: 1},
	} {
		if left := checkContainers(t, docker, c.check); len(left) != c.left {
			t.Errorf("%s holds %d containers after the sweep, want %d", c.name, len(left), c.left)
		}
	}

	if !slices.Contains(sweep.Swept, dead.check) || len(sweep.Skipped) == 0 {
		t.Errorf("swept %v, skipped %v: want the dead check swept and the others skipped", sweep.Swept,
			sweep.Skipped)
	}
}
