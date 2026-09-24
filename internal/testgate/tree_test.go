package testgate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// porcelain renders entries as git status --porcelain=v1 -z does: "XY path", NUL-terminated.
func porcelain(entries ...string) *strings.Reader {
	return strings.NewReader(strings.Join(entries, "\x00") + "\x00")
}

func write(t *testing.T, dir, name, content string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func snapshot(t *testing.T, root string, entries ...string) testgate.Tree {
	t.Helper()

	tree, err := testgate.ReadTree(porcelain(entries...), root)
	if err != nil {
		t.Fatal(err)
	}

	// Through the saved form, as make tree-snapshot and tree-check hand it over.
	var saved bytes.Buffer
	if err := tree.Save(&saved); err != nil {
		t.Fatal(err)
	}

	loaded, loadErr := testgate.LoadTree(&saved)
	if loadErr != nil {
		t.Fatal(loadErr)
	}

	return loaded
}

func TestTheTreeLineCountsWhatTheSuiteChanged(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write(t, root, "edited.go", "before")
	write(t, root, "steady.go", "same")
	write(t, root, "flipped.go", "x")

	before := snapshot(t, root, " M edited.go", " M steady.go", " M flipped.go")

	write(t, root, "edited.go", "after")
	write(t, root, "leftover.txt", "a test wrote me")

	after := snapshot(t, root, " M edited.go", " M steady.go", "MM flipped.go", "?? leftover.txt")

	want := []string{"edited.go", "flipped.go", "leftover.txt"}
	if got := after.Changed(before); !slices.Equal(got, want) {
		t.Fatalf("changed = %q, want %q: new, changed in content, changed in status", got, want)
	}

	if got := snapshot(t, root).Changed(before); !slices.Equal(got, []string{
		"edited.go", "flipped.go",
		"steady.go",
	}) {
		t.Fatalf("changed = %q; an entry that is gone after the suite is a change", got)
	}

	if got := snapshot(t, root).Changed(snapshot(t, root)); len(got) != 0 {
		t.Fatalf("a clean tree before and after: changed = %q, want none", got)
	}
}

func TestACompareWithoutASnapshotFails(t *testing.T) {
	t.Parallel()

	if _, err := testgate.LoadTree(strings.NewReader("")); err == nil {
		t.Fatal("an empty snapshot loaded; a compare without a snapshot must fail")
	}

	if _, err := testgate.LoadTree(strings.NewReader("{not a snapshot")); err == nil {
		t.Fatal("a malformed snapshot loaded")
	}
}
