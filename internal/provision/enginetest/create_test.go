//go:build linux

package enginetest_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// runnable is the change that gives an imported image a process, so a container can be created
// from it; nothing here starts one.
func runnable() []string {
	return []string{`CMD ["/run"]`}
}

// A reference that moves after the pin never reaches a create: every container of the check runs
// the image the check pinned, and nothing is pulled or built once the first container exists.
func TestEveryRunContainerUsesThePinnedImage(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)
	moved := testRef("moved")
	pinnedID := docker.Import(t, layer(t, "a", "A"), testRef("kept"), runnable())
	docker.AddTag(t, pinnedID, moved)

	// Opened after the images exist, so it is closed before the helper removes them.
	engine := openEngine(t, provision.Options{})

	pinned := pinImage(t, engine, moved)
	if pinned.ID != pinnedID {
		t.Fatalf("%s resolved to %s, not %s", moved, pinned.ID, pinnedID)
	}

	docker.Remove(t, moved)
	movedID := docker.Import(t, layer(t, "b", "B"), moved, runnable())

	if tags := docker.ImageTags(t); tags[moved] != movedID {
		t.Fatalf("the reference did not move: %s names %s, want %s", moved, tags[moved], movedID)
	}

	network := freeNetwork(t, engine, serviceRole)

	for range 2 {
		spec := targetSpec(pinned, network)
		spec.Kind = rules.KindJob
		c := createContainer(t, engine, spec)

		runs := field(inspected(t, docker.Inspect(t, dockertest.ObjectContainer, c.ID())), "Image")
		t.Logf("container %s image %v", c.ID(), runs)

		if runs != pinnedID {
			t.Errorf("container %s runs %v, not the pinned %s (the moved reference names %s)", c.ID(), runs,
				pinnedID, movedID)
		}
	}

	down := engine.Close(t.Context(), provision.KeepLogs)

	counts, after := map[string]int{}, map[string]int{}
	created := false

	for _, line := range jsonLines(t, filepath.Join(down.PrivateDir, logName)) {
		verb, ok := line["verb"].(string)
		if !ok || line["kind"] != "call" {
			continue
		}

		counts[verb]++

		if created {
			after[verb]++
		}

		created = created || verb == "create"
	}

	t.Logf("calls per verb %v; after the first create %v", counts, after)

	if counts["create"] != 2 {
		t.Errorf("the log holds %d creates, want 2", counts["create"])
	}

	if after["pull"] != 0 || after["composeBuild"] != 0 {
		t.Errorf("%d pulls and %d builds after the first create, want 0", after["pull"], after["composeBuild"])
	}
}

// sentinelArchive is an archive of one file whose content is random, with that content's hash.
func sentinelArchive(t *testing.T) (*bytes.Buffer, string) {
	t.Helper()

	content := []byte(randomHex(32))
	sum := sha256.Sum256(content)

	var out bytes.Buffer

	archive := tar.NewWriter(&out)
	if err := archive.WriteHeader(&tar.Header{Name: "sentinel", Mode: 0o644, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}

	if _, err := archive.Write(content); err != nil {
		t.Fatal(err)
	}

	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	return &out, hex.EncodeToString(sum[:])
}

// sentinelIntact runs a test container over the volume that checks the sentinel's hash, and reports
// whether it matched.
func sentinelIntact(t *testing.T, docker *dockertest.Docker, volume, hash string) bool {
	t.Helper()

	checker := docker.Create(t, dockertest.CreateSpec{
		Image:  testImage,
		Mounts: []string{"type=volume,source=" + volume + ",target=/v"},
		Cmd:    []string{"sh", "-c", `echo "$0  /v/sentinel" | sha256sum -c -`, hash},
	})
	docker.Start(t, checker)

	code := docker.Wait(t, checker)
	docker.Remove(t, checker)

	return code == 0
}

// A volume the check did not create is never used and never removed, even when it carries the very
// name the check's next volume would have: the engine hands back an existing volume unchanged.
func TestAVolumeNotCreatedByThisCheckIsNeverUsed(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)
	stateDir := newStateDir(t)
	engine := openEngine(t, provision.Options{StateDir: stateDir})

	name := "stutter-" + engine.CheckID() + "-" + string(rules.KindTarget) + "-" +
		strconv.Itoa(lastSeq(t, stateDir, engine.CheckID())+1)
	docker.CreateVolume(t, name, map[string]string{"planted": "decoy"})

	archive, hash := sentinelArchive(t)
	writer := docker.Create(t, dockertest.CreateSpec{
		Image: testImage, Mounts: []string{"type=volume,source=" + name + ",target=/v"}, Cmd: []string{"true"},
	})
	docker.CopyIn(t, writer, "/v", archive)
	docker.Remove(t, writer)

	before := field(inspected(t, docker.Inspect(t, dockertest.ObjectVolume, name)), "Labels")

	volume, err := engine.CreateVolume(t.Context(), rules.KindTarget, testService)
	if !errors.Is(err, provision.ErrNotOurs) {
		t.Fatalf("CreateVolume over a planted %s = %v, %v; want ErrNotOurs", name, volume, err)
	}

	down := engine.Close(t.Context(), provision.DiscardLogs)
	t.Logf("teardown after the refusal: %v", down.Err)

	after := inspected(t, docker.Inspect(t, dockertest.ObjectVolume, name))
	if after == nil {
		t.Fatalf("the planted volume %s is gone", name)
	}

	if labels := field(after, "Labels"); !reflect.DeepEqual(before, labels) {
		t.Errorf("the planted volume's labels changed: %v, then %v", before, labels)
	}

	if !sentinelIntact(t, docker, name, hash) {
		t.Error("the planted volume's sentinel changed")
	}

	if used := docker.Listing(t, rules.LabelCheck+"="+engine.CheckID()); len(used.Containers) != 0 {
		t.Errorf("containers of the check remain: %v", used.Containers)
	}
}

// Every bind is read-only however compose asked for it, and recursively: nothing a container does
// through it changes the host directory.
func TestEveryBindIsRecursivelyReadOnly(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)
	engine := openEngine(t, provision.Options{})
	image := pinImage(t, engine, testImage)

	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "a"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}

	spec := shell(targetSpec(image, freeNetwork(t, engine, serviceRole)), "touch /b/x; mkdir /b/y; exit 0")
	spec.Spec.Mounts = []compose.Mount{{Kind: compose.MountBind, Source: source, Target: "/b", ReadOnly: false}}

	c := createContainer(t, engine, spec)
	startContainer(t, engine, c)
	awaitExit(t, engine, c)

	if _, err := engine.Stop(t.Context(), c); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	read := inspected(t, docker.Inspect(t, dockertest.ObjectContainer, c.ID()))
	binds := 0

	for _, m := range list(field(read, "Mounts")) {
		if m["Type"] == "bind" {
			binds++

			if rw, ok := m["RW"].(bool); !ok || rw {
				t.Errorf("bind %v reads RW=%v", m["Destination"], m["RW"])
			}
		}
	}

	for _, m := range list(field(read, "HostConfig", "Mounts")) {
		recursive, ok := field(m, "BindOptions", "ReadOnlyForceRecursive").(bool)
		if m["Type"] == "bind" && (!ok || !recursive) {
			t.Errorf("bind %v is not recursively read-only: %v", m["Target"], m["BindOptions"])
		}
	}

	t.Logf("binds inspected=%d", binds)

	if binds == 0 {
		t.Fatal("no bind mount was read back")
	}

	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}

	if !slices.Equal(names, []string{"a"}) {
		t.Errorf("the bound directory now holds %v, want [a]", names)
	}
}

// anonymousVolumes names every volume the engine mounts in c and every volume the ledger attributes
// to the check.
func anonymousVolumes(
	t *testing.T, docker *dockertest.Docker, stateDir string, engine *provision.Engine, c *provision.Container,
) []string {
	t.Helper()

	var out []string

	for _, m := range list(field(inspected(t, docker.Inspect(t, dockertest.ObjectContainer, c.ID())), "Mounts")) {
		if name, ok := m["Name"].(string); ok && m["Type"] == "volume" {
			out = append(out, name)
		}
	}

	for _, line := range jsonLines(t, ledgerPath(stateDir, engine.CheckID())) {
		if name, ok := line["name"].(string); ok && line["type"] == "volume" && !slices.Contains(out, name) {
			out = append(out, name)
		}
	}

	return out
}

// An image VOLUME gets a labelled anonymous volume that goes with its container: removed with it,
// and removed at Close, never left for the user.
func TestNoAnonymousVolumeSurvives(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)
	stateDir := newStateDir(t)
	engine := openEngine(t, provision.Options{StateDir: stateDir})
	image := pinImage(t, engine, testImage)
	spec := targetSpec(image, freeNetwork(t, engine, serviceRole))

	gone := func(names []string) {
		t.Helper()

		for _, name := range names {
			if docker.Inspect(t, dockertest.ObjectVolume, name) != nil {
				t.Errorf("anonymous volume %s survived", name)
			}
		}
	}

	removed := createContainer(t, engine, spec)
	first := anonymousVolumes(t, docker, stateDir, engine, removed)

	if err := engine.Remove(t.Context(), removed); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	gone(first)

	closed := createContainer(t, engine, spec)
	second := anonymousVolumes(t, docker, stateDir, engine, closed)
	second = slices.DeleteFunc(second, func(name string) bool { return slices.Contains(first, name) })

	if down := engine.Close(t.Context(), provision.KeepLogs); down.Err != nil {
		t.Errorf("Close: %v", down.Err)
	}

	gone(second)

	t.Logf("anonymous volumes=%d", len(first)+len(second))

	if len(first) == 0 || len(second) == 0 {
		t.Fatalf("no anonymous volume was attributed: %v after Remove, %v after Close", first, second)
	}
}
