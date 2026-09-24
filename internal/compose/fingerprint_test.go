package compose_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// tree writes files, keyed by a slash-separated path under a fresh directory, and returns it.
func tree(t *testing.T, files map[string]string) string {
	t.Helper()

	root := t.TempDir()

	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	return root
}

func fingerprint(t *testing.T, sources ...string) compose.Prints {
	t.Helper()

	prints, err := compose.Fingerprint(t.Context(), sources)
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}

	return prints
}

func TestAnEditedBindIsNamedByTheEndWalk(t *testing.T) {
	t.Parallel()

	root := tree(t, map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"})
	single := filepath.Join(tree(t, map[string]string{"c.conf": "charlie"}), "c.conf")
	start := fingerprint(t, root, single)

	edited := filepath.Join(root, "sub", "b.txt")
	if err := os.WriteFile(edited, []byte("BRAVO"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(edited, later, later); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	end := fingerprint(t, root, single)
	changed := start.Changed(end)

	t.Logf("entries=%d changed=%d", start.Entries(), len(changed))

	if start.Entries() == 0 || start.Sources() != 2 {
		t.Fatalf("entries=%d sources=%d, want entries and 2 sources", start.Entries(), start.Sources())
	}

	if !slices.Equal(changed, []string{edited}) {
		t.Errorf("Changed = %v, want exactly %s", changed, edited)
	}

	// A single-file source is hashed: same size, same mtime, different bytes is still a change.
	info, err := os.Stat(single)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if err := os.WriteFile(single, []byte("CHARLIE"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	if err := os.Chtimes(single, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if got := end.Changed(fingerprint(t, root, single)); !slices.Equal(got, []string{single}) {
		t.Errorf("Changed = %v, want exactly %s", got, single)
	}
}

func TestAnUnchangedTreeHasNoChanges(t *testing.T) {
	t.Parallel()

	root := tree(t, map[string]string{"a": "1", "d/b": "2", "d/e/c": "3"})

	if changed := fingerprint(t, root).Changed(fingerprint(t, root)); len(changed) != 0 {
		t.Errorf("Changed = %v, want none", changed)
	}
}

func TestAnAddedOrRemovedEntryIsNamed(t *testing.T) {
	t.Parallel()

	root := tree(t, map[string]string{"keep": "1", "gone": "2"})
	start := fingerprint(t, root)

	if err := os.Remove(filepath.Join(root, "gone")); err != nil {
		t.Fatalf("remove: %v", err)
	}

	if err := os.WriteFile(filepath.Join(root, "new"), []byte("3"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	changed := start.Changed(fingerprint(t, root))
	for _, name := range []string{"gone", "new"} {
		if !slices.Contains(changed, filepath.Join(root, name)) {
			t.Errorf("Changed = %v, want %s named", changed, name)
		}
	}
}

func TestASymlinkIsRecordedNotFollowed(t *testing.T) {
	t.Parallel()

	outside := tree(t, map[string]string{"target.txt": "one", "other.txt": "two"})
	root := t.TempDir()
	link := filepath.Join(root, "link")

	if err := os.Symlink(filepath.Join(outside, "target.txt"), link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	start := fingerprint(t, root)

	if err := os.WriteFile(filepath.Join(outside, "target.txt"), []byte("ONE-changed"), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	if changed := start.Changed(fingerprint(t, root)); len(changed) != 0 {
		t.Errorf("Changed = %v after editing a link's target, want none: the link is not followed", changed)
	}

	if err := os.Remove(link); err != nil {
		t.Fatalf("remove: %v", err)
	}

	if err := os.Symlink(filepath.Join(outside, "other.txt"), link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// Replacing the link also moves its directory's mtime.
	if changed := start.Changed(fingerprint(t, root)); !slices.Contains(changed, link) ||
		slices.ContainsFunc(changed, func(path string) bool { return path != link && path != root }) {
		t.Errorf("Changed = %v after retargeting the link, want %s and at most its directory", changed, link)
	}

	// A source that is itself a symlink is followed, as the mount follows it.
	linkedSource := filepath.Join(t.TempDir(), "source")
	if err := os.Symlink(outside, linkedSource); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if prints := fingerprint(t, linkedSource); prints.Entries() < 3 {
		t.Errorf("a symlinked source yielded %d entries, want its directory's", prints.Entries())
	}
}

func TestAnEmptySourceListYieldsNothing(t *testing.T) {
	t.Parallel()

	prints := fingerprint(t)
	if prints.Entries() != 0 || prints.Sources() != 0 || len(prints.Special()) != 0 {
		t.Errorf("empty source list = %d entries, %d sources, %v special", prints.Entries(), prints.Sources(),
			prints.Special())
	}

	root := t.TempDir()
	if empty := fingerprint(t, root); empty.Entries() < 1 {
		t.Errorf("an empty directory source yielded %d entries, want at least itself", empty.Entries())
	}
}
