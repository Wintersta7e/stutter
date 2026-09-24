//go:build linux

package enginetest_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// lastOp is the op of a ledger's last line.
func lastOp(t *testing.T, stateDir, check string) any {
	t.Helper()

	lines := jsonLines(t, ledgerPath(stateDir, check))
	if len(lines) == 0 {
		return nil
	}

	return lines[len(lines)-1]["op"]
}

// held counts what the engine holds carrying check's label, by type.
func held(t *testing.T, docker *dockertest.Docker, check string) dockertest.Listing {
	t.Helper()

	return docker.Listing(t, rules.LabelCheck+"="+check)
}

// gone reports whether nothing is at path.
func gone(path string) bool {
	_, err := os.Lstat(path)

	return errors.Is(err, fs.ErrNotExist)
}

// A check that ends in a setup error keeps its logs and nothing else: every resource goes, the
// staged CA and bus store go, and `stutter clean` removes what was kept.
func TestLogsSurviveASetupExit(t *testing.T) {
	t.Parallel()

	gate := requireEngine(t)
	docker := gate.Docker(t)
	stateDir := newStateDir(t)

	h := launch(t, "setup", helperSpec{StateDir: stateDir, TempDir: t.TempDir()},
		helperEnv(gate, os.Getenv("PATH"), t.TempDir()))
	private := h.await(t, "private")
	reported := h.collect(t, "removed")

	if err := h.cmd.Wait(); err != nil {
		t.Fatalf("the helper failed: %v", err)
	}

	if info, err := os.Stat(private); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("the kept private directory: %v, %v; want mode 0700", info, err)
	}

	kept := 0

	walkErr := filepath.WalkDir(private, func(path string, entry fs.DirEntry, err error) error {
		rel, relErr := filepath.Rel(private, path)

		switch {
		case err != nil || relErr != nil:
			return errors.Join(err, relErr)
		case rel == "." || rel == logsDir && entry.IsDir():
		case rel == logName || filepath.Dir(rel) == logsDir && entry.Type().IsRegular():
			kept++
		default:
			t.Errorf("the kept private directory holds %s", rel)
		}

		return nil
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}

	for _, log := range reported["log"] {
		if gone(log) {
			t.Errorf("the teardown names a log that does not exist: %s", log)
		}
	}

	if listing := held(t, docker, h.check); len(listing.Containers)+len(listing.Networks)+len(listing.Volumes)+
		len(listing.Images) != 0 {
		t.Errorf("the engine holds resources of the check: %+v", listing)
	}

	if op := lastOp(t, stateDir, h.check); op != "retained" {
		t.Errorf("the ledger ends %v, want retained", op)
	}

	_, err := provision.Clean(t.Context(), provision.CleanOptions{StateDir: stateDir, CheckID: h.check})
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}

	if !gone(private) || !gone(ledgerPath(stateDir, h.check)) {
		t.Error("the clean left the private directory or the ledger")
	}

	t.Logf("kept=%d removed=%s", kept, strings.Join(reported["removed"], ""))

	if kept < 2 {
		t.Fatalf("kept %d files, want the invocation log and at least one container log", kept)
	}
}

// present reports whether the engine holds an object.
func present(t *testing.T, docker *dockertest.Docker, what dockertest.Object, id string) bool {
	t.Helper()

	return docker.Inspect(t, what, id) != nil
}

// A kept check is left whole — containers stopped, the rest present — by its own teardown and the
// next check's sweep alike, and `stutter clean --check` removes it; a tag the user gave its snapshot
// survives, and only the check's own reference goes.
func TestAKeptCheckIsLeftForClean(t *testing.T) {
	t.Parallel()

	gate := requireEngine(t)
	docker := gate.Docker(t)
	stateDir := newStateDir(t)

	h := launch(t, "keep", helperSpec{StateDir: stateDir, TempDir: t.TempDir(), Keep: true},
		helperEnv(gate, os.Getenv("PATH"), t.TempDir()))
	reported := h.collect(t, "done")

	if err := h.cmd.Wait(); err != nil {
		t.Fatalf("the helper failed: %v", err)
	}

	container, image := reported["container"][0], reported["image"][0]
	objects := map[dockertest.Object]string{
		dockertest.ObjectContainer: container, dockertest.ObjectNetwork: reported["network"][0],
		dockertest.ObjectVolume: reported["volume"][0], dockertest.ObjectImage: image,
	}

	allPresent := func(when string) {
		t.Helper()

		for what, id := range objects {
			if !present(t, docker, what, id) {
				t.Errorf("%s: the kept %s %s is gone", when, what, id)
			}
		}
	}

	allPresent("after the kept check's teardown")

	state := field(inspected(t, docker.Inspect(t, dockertest.ObjectContainer, container)), "State")
	if running, ok := field(state, "Running").(bool); !ok || running {
		t.Errorf("the kept container reads %v, want stopped", state)
	}

	if op := lastOp(t, stateDir, h.check); op != "kept" {
		t.Errorf("the ledger ends %v, want kept", op)
	}

	retained := reported["retained"]
	for _, want := range []string{"container " + container, "image " + image} {
		if !slices.Contains(retained, want) {
			t.Errorf("the teardown did not name %s as retained: %v", want, retained)
		}
	}

	sweep := openEngine(t, provision.Options{StateDir: stateDir}).Sweep()
	if !slices.Contains(sweep.Skipped, h.check) {
		t.Errorf("the next check's sweep did not skip the kept check: %+v", sweep)
	}

	allPresent("after the next check's sweep")

	userTag := testRef("kept")
	docker.AddTag(t, image, userTag)

	dry, err := provision.Clean(t.Context(), provision.CleanOptions{StateDir: stateDir, CheckID: h.check, DryRun: true})
	if err != nil || len(dry.Removed) == 0 {
		t.Fatalf("a dry-run clean: %v, would remove %d", err, len(dry.Removed))
	}

	allPresent("after a dry-run clean")

	cleaned, err := provision.Clean(t.Context(), provision.CleanOptions{StateDir: stateDir, CheckID: h.check})
	if err != nil || len(cleaned.Failed) != 0 {
		t.Fatalf("Clean: %v, failed %+v", err, cleaned.Failed)
	}

	left := held(t, docker, h.check)
	if len(left.Containers)+len(left.Networks)+len(left.Volumes) != 0 || !slices.Equal(left.Images, []string{image}) {
		t.Errorf("after the clean the engine holds %+v; want only the snapshot, under the user's tag", left)
	}

	for ref, id := range docker.ImageTags(t) {
		if strings.HasPrefix(ref, rules.ImageDomain+"/"+h.check+"/") {
			t.Errorf("the check's reference %s survived the clean (image %s)", ref, id)
		}
	}

	if tags := docker.ImageTags(t); tags[userTag] != image {
		t.Errorf("the user's tag %s names %q after the clean, want the snapshot %s", userTag, tags[userTag], image)
	}

	t.Logf("retained=%d removed=%d", len(retained), len(cleaned.Removed))
}

// createdCount matches AUDIT-4's count line of a type the check created something of.
var createdCount = regexp.MustCompile(`^teardown \S+ created=([1-9][0-9]*) `)

// A resource carrying the check's label that the check never made is listed at teardown, and never
// removed: only the ledger authorises a removal.
func TestAPlantedLabelledResourceIsListedNeverRemoved(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)

	engine, err := provision.Open(t.Context(), provision.Options{StateDir: newStateDir(t), TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	t.Cleanup(func() { engine.Close(context.WithoutCancel(t.Context()), provision.DiscardLogs) })

	freeNetwork(t, engine, serviceRole)

	planted := docker.CreateVolume(t, "stutter-test-planted-"+randomHex(8), map[string]string{
		rules.LabelCheck: engine.CheckID(), rules.LabelKind: string(rules.KindTarget),
	})

	down := engine.Close(t.Context(), provision.DiscardLogs)

	if !slices.ContainsFunc(down.Listing, func(l provision.Listed) bool { return l.ID == planted }) {
		t.Errorf("the teardown's listing does not hold the planted volume %s: %+v", planted, down.Listing)
	}

	if !present(t, docker, dockertest.ObjectVolume, planted) {
		t.Errorf("the planted volume %s was removed", planted)
	}

	var audit bytes.Buffer
	if _, err := down.WriteTo(&audit); err != nil {
		t.Fatal(err)
	}

	created := 0

	for line := range strings.Lines(audit.String()) {
		t.Log(strings.TrimSpace(line))

		if match := createdCount.FindStringSubmatch(line); match != nil {
			n, err := strconv.Atoi(match[1])
			if err != nil {
				t.Fatal(err)
			}

			created += n
		}
	}

	if created == 0 {
		t.Fatal("no count line shows anything created")
	}
}
