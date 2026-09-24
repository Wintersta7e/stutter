//go:build linux

package provision

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// runClean runs Clean against the fake engine and returns what it printed.
func runClean(t *testing.T, state string, fake *fakeEngine, opts CleanOptions) (CleanResult, string, error) {
	t.Helper()

	var out bytes.Buffer

	opts.StateDir, opts.Stdout = state, &out

	result, err := cleanWith(t.Context(), opts, cleanDeps{
		admit: func(context.Context) (Identity, engineCaller, error) { return testIdentity(), fake, nil },
		host:  defaultHostFS(),
	})

	return result, out.String(), err
}

func newState(t *testing.T) string {
	t.Helper()

	state := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}

	return state
}

// Clean acts only on dead ledgers of this engine and host — kept and retained ones included — and
// removes their resources and their whole private directory. Another engine's or host's ledger is
// listed and left; a live one is skipped and named.
func TestCleanActsOnlyOnDeadLedgersOfThisEngineAndHost(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), newState(t)
	foreignEngine := plantLedger(t, state, fake, plant{edit: func(h *header) { h.EngineID = "engine-b" }})
	foreignHost := plantLedger(t, state, fake, plant{edit: func(h *header) { h.Host = "elsewhere" }})
	live := plantLedger(t, state, fake, plant{holdLock: true})
	dead := plantLedger(t, state, fake, plant{kept: true, privateOK: true})
	writeTestFile(t, filepath.Join(dead.private, "invocation.log"))

	result, out, err := runClean(t, state, fake, CleanOptions{})
	if err != nil {
		t.Fatalf("Clean: %v\n%s", err, out)
	}

	for _, p := range []planted{foreignEngine, foreignHost, live} {
		if fake.find(ResourceContainer, p.container) == nil {
			t.Errorf("check %s's container was removed", p.check)
		}

		if _, err := os.Lstat(p.led.path); err != nil {
			t.Errorf("check %s's ledger was removed: %v", p.check, err)
		}
	}

	if !slices.Equal(result.Foreign, sorted(foreignEngine.check, foreignHost.check)) ||
		!slices.Equal(result.Live, []string{live.check}) {
		t.Errorf("Foreign = %v, Live = %v", result.Foreign, result.Live)
	}

	if fake.find(ResourceContainer, dead.container) != nil {
		t.Error("the dead check's container remains")
	}

	for _, gone := range []string{dead.private, dead.led.path} {
		if _, err := os.Lstat(gone); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s remains after clean: %v", gone, err)
		}
	}

	t.Logf("clean output:\n%s", out)
}

func sorted(values ...string) []string {
	slices.Sort(values)

	return values
}

// `clean --check <id>` refuses a check whose owner is alive and removes nothing.
func TestCleanCheckRefusesALiveCheck(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), newState(t)
	live := plantLedger(t, state, fake, plant{holdLock: true})

	_, _, err := runClean(t, state, fake, CleanOptions{CheckID: live.check})
	if !errors.Is(err, ErrLive) || !strings.Contains(err.Error(), live.check) {
		t.Fatalf("Clean = %v, want ErrLive naming the check", err)
	}

	if fake.find(ResourceContainer, live.container) == nil {
		t.Error("the live check's container was removed")
	}

	for _, c := range fake.calls {
		if verbs()[verbOf(c.verb)].mutates {
			t.Errorf("a refused clean issued %s %v", c.verb, c.argv)
		}
	}
}

// The one label-driven removal: the user typed the check ID, so resources carrying exactly that
// check label and a kind of the vocabulary are removed even with no ledger here. A resource with
// another ID, or a kind outside the vocabulary, is not.
func TestCleanCheckRemovesUnledgeredResourcesByExactLabel(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), newState(t)
	id, other := strings.Repeat("d", 32), strings.Repeat("e", 32)

	ours := fake.add(ResourceContainer, &fakeObject{name: "a", labels: labelSet(id, rules.KindTarget, "")})
	oddKind := fake.add(ResourceContainer, &fakeObject{name: "b", labels: map[string]string{
		rules.LabelCheck: id, rules.LabelKind: "their-kind",
	}})
	theirs := fake.add(ResourceContainer, &fakeObject{name: "c", labels: labelSet(other, rules.KindTarget, "")})
	ref := imageRef(id, rules.KindSnapshot, 3)
	image := fake.add(ResourceImage, &fakeObject{tags: []string{ref}, labels: labelSet(id, rules.KindSnapshot, "")})

	result, out, err := runClean(t, state, fake, CleanOptions{CheckID: id})
	if err != nil {
		t.Fatalf("Clean: %v\n%s", err, out)
	}

	if fake.find(ResourceContainer, ours) != nil || fake.find(ResourceImage, image) != nil {
		t.Errorf("a resource carrying exactly the check label remains:\n%s", out)
	}

	if fake.find(ResourceContainer, oddKind) == nil || fake.find(ResourceContainer, theirs) == nil {
		t.Errorf("a resource outside the exact label was removed:\n%s", out)
	}

	if len(result.Removed) != 2 {
		t.Errorf("Removed = %+v, want the container and the image", result.Removed)
	}
}

// A dry run prints what it would remove and removes nothing.
func TestCleanDryRunRemovesNothing(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), newState(t)
	dead := plantLedger(t, state, fake, plant{privateOK: true})

	_, out, err := runClean(t, state, fake, CleanOptions{DryRun: true})
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}

	for _, c := range fake.calls {
		if verbs()[verbOf(c.verb)].mutates {
			t.Errorf("a dry run issued %s %v", c.verb, c.argv)
		}
	}

	if fake.find(ResourceContainer, dead.container) == nil {
		t.Error("a dry run removed the container")
	}

	for _, kept := range []string{dead.private, dead.led.path} {
		if _, err := os.Lstat(kept); err != nil {
			t.Errorf("a dry run removed %s: %v", kept, err)
		}
	}

	lines := 0

	for line := range strings.Lines(out) {
		if strings.Contains(line, dead.check) && !strings.HasPrefix(line, "clean ") {
			lines++

			if !strings.HasPrefix(line, "would remove ") {
				t.Errorf("a dry-run line does not say would remove: %q", line)
			}
		}
	}

	t.Logf("would-remove lines=%d", lines)

	if lines < 2 {
		t.Errorf("the dry run named %d resources, want the container and the directory:\n%s", lines, out)
	}
}

// Clean mints no check, writes no ledger, and points the engine's client at a configuration
// directory it never creates.
func TestCleanWritesNoLedgerAndNoConfig(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), newState(t)
	plantLedger(t, state, fake, plant{})

	before := entriesOf(t, state)

	if _, _, err := runClean(t, state, fake, CleanOptions{}); err != nil {
		t.Fatal(err)
	}

	after := entriesOf(t, state)
	for _, name := range after {
		if !slices.Contains(before, name) {
			t.Errorf("clean wrote %s into the state directory", name)
		}
	}

	base := filepath.Base(fake.configDir)
	if !strings.HasPrefix(base, "stutter-clean-") || !strings.HasSuffix(base, "-absent") {
		t.Errorf("DOCKER_CONFIG = %q, not the never-created clean path", fake.configDir)
	}

	if _, err := os.Lstat(fake.configDir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the DOCKER_CONFIG path exists: %v", err)
	}
}

// A ledger's recorded private directory is re-validated before clean removes it: a symlink, or a
// path not named for its check, is left and reported, and the ledger stays.
func TestCleanRevalidatesTheRecordedPrivateDirectory(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), newState(t)
	target := t.TempDir()
	writeTestFile(t, filepath.Join(target, "keep"))

	misnamed := plantLedger(t, state, fake, plant{noObject: true, entries: noEntries, edit: func(h *header) {
		h.PrivateDir = target
	}})
	symlinked := plantLedger(t, state, fake, plant{noObject: true, entries: noEntries, edit: func(h *header) {
		if err := os.Symlink(target, filepath.Join(filepath.Dir(h.PrivateDir), "stutter-"+h.Check)); err != nil {
			t.Fatal(err)
		}
	}})

	result, out, err := runClean(t, state, fake, CleanOptions{})
	if err == nil {
		t.Errorf("clean of an unverifiable directory reported success:\n%s", out)
	}

	if _, err := os.Lstat(filepath.Join(target, "keep")); err != nil {
		t.Errorf("a directory outside a check's root was removed: %v", err)
	}

	for _, p := range []planted{misnamed, symlinked} {
		if _, err := os.Lstat(p.led.path); err != nil {
			t.Errorf("check %s's ledger was deleted though its directory was refused: %v", p.check, err)
		}
	}

	if len(result.Failed) != 2 {
		t.Errorf("Failed = %+v, want both refused directories", result.Failed)
	}
}
