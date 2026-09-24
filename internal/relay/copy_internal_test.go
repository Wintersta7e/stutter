//go:build linux

package relay

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// everyModeBit is what the manifest compares of a mode, stated apart from the copy's own mask so that
// narrowing that mask cannot narrow the comparison with it.
const everyModeBit = fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

// entry is one path of a tree, as the copy must reproduce it.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type entry struct {
	path    string
	target  string
	content string
	// linkedTo is the first path of the tree sharing this entry's inode, for a hard link.
	linkedTo string
	size     int64
	mtime    int64
	kind     fs.FileMode
	mode     fs.FileMode
	uid      uint32
	gid      uint32
}

// manifest walks a tree without following a link and describes every entry, the root included.
func manifest(t *testing.T, root string) []entry {
	t.Helper()

	var entries []entry

	firstOf := map[[2]uint64]string{}

	err := filepath.WalkDir(root, func(at string, _ fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		info, err := os.Lstat(at)
		if err != nil {
			return err
		}

		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("%s: no stat", at)
		}

		rel, err := filepath.Rel(root, at)
		if err != nil {
			return err
		}

		described := entry{
			path: rel, kind: info.Mode().Type(), mode: info.Mode() & everyModeBit, uid: stat.Uid, gid: stat.Gid,
			mtime: stat.Mtim.Nano(),
		}

		return describeEntry(at, info, stat, &described, firstOf, func() { entries = append(entries, described) })
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	return entries
}

// describeEntry fills what depends on an entry's type: a link's target, a file's size, content and links.
func describeEntry(
	at string, info fs.FileInfo, stat *syscall.Stat_t, into *entry, firstOf map[[2]uint64]string, add func(),
) error {
	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := os.Readlink(at)
		if err != nil {
			return err
		}

		into.target = target
	case info.Mode().IsRegular():
		data, err := os.ReadFile(at)
		if err != nil {
			return err
		}

		sum := sha256.Sum256(data)
		into.size, into.content = info.Size(), hex.EncodeToString(sum[:])

		if stat.Nlink > 1 {
			key := [2]uint64{stat.Dev, stat.Ino}
			if first, seen := firstOf[key]; seen {
				into.linkedTo = first
			} else {
				firstOf[key] = into.path
			}
		}
	default:
	}

	add()

	return nil
}

// owner is a file's owner and group.
type owner struct {
	uid int
	gid int
}

// otherOwner is an owner the copy must carry over that is not simply the copying user's own: another
// uid when the test runs as root, else one of the user's supplementary groups.
func otherOwner(t *testing.T) owner {
	t.Helper()

	if os.Geteuid() == 0 {
		return owner{uid: 1234, gid: 1234}
	}

	groups, err := os.Getgroups()
	if err != nil {
		t.Fatalf("Getgroups() error = %v", err)
	}

	for _, group := range groups {
		if group != os.Getgid() {
			return owner{uid: os.Getuid(), gid: group}
		}
	}

	t.Fatal("the test user has no supplementary group, so a dropped group owner could not be told apart")

	return owner{}
}

// past is the modification time the tree is given, far from anything the copy could stamp by accident.
var past = time.Date(2001, time.February, 3, 4, 5, 6, 789, time.UTC)

// buildTree lays out one of every kind of entry the copy must keep, and sets each directory's time
// after its children, as a real tree ends up.
func buildTree(t *testing.T, root string) {
	t.Helper()

	other := otherOwner(t)

	must := func(err error) {
		t.Helper()

		if err != nil {
			t.Fatal(err)
		}
	}

	// The modes below are the ones a copy must keep, so they are not the tightest a file could have.
	must(os.MkdirAll(filepath.Join(root, "shared"), 0o755)) //nolint:gosec // a mode the copy must keep.
	must(os.Mkdir(filepath.Join(root, "sticky"), 0o755))    //nolint:gosec // as above.
	must(os.Mkdir(filepath.Join(root, "empty"), 0o700))
	must(os.WriteFile(filepath.Join(root, "secret"), []byte("only mine\n"), 0o600))
	must(os.Lchown(filepath.Join(root, "secret"), other.uid, other.gid))
	must(os.WriteFile(filepath.Join(root, "tool"), []byte("#!/bin/sh\n"), 0o755)) //nolint:gosec // as above.
	must(os.Chmod(filepath.Join(root, "tool"), 0o755|fs.ModeSetuid))
	must(os.Lchown(filepath.Join(root, "shared"), other.uid, other.gid))
	must(os.WriteFile(filepath.Join(root, "shared", "inner"), []byte("inner\n"), 0o640)) //nolint:gosec // as above.
	must(os.Chmod(filepath.Join(root, "shared"), 0o775|fs.ModeSetgid))
	must(os.Chmod(filepath.Join(root, "sticky"), 0o777|fs.ModeSticky))

	linked := []byte("one inode, two names\n")

	must(os.WriteFile(filepath.Join(root, "first"), linked, 0o644)) //nolint:gosec // as above.
	must(os.Link(filepath.Join(root, "first"), filepath.Join(root, "second")))
	must(os.Symlink("/etc/absolute-target", filepath.Join(root, "absolute")))
	must(os.Symlink("secret", filepath.Join(root, "relative")))

	for _, file := range []string{"secret", "tool", "first", "shared/inner"} {
		must(os.Chtimes(filepath.Join(root, file), past, past))
	}

	stamp := syscall.NsecToTimespec(past.UnixNano())

	for _, link := range []string{"absolute", "relative"} {
		must(setTimes(filepath.Join(root, link), [2]syscall.Timespec{stamp, stamp}, atSymlinkNoFollow))
	}

	for _, dir := range []string{"shared", "sticky", "empty", "."} {
		must(os.Chtimes(filepath.Join(root, dir), past, past))
	}

	must(os.Chmod(root, 0o751)) //nolint:gosec // as above.
	must(os.Chtimes(root, past, past))
}

// copyOf runs the copy mode over src into a fresh destination root and returns the root, its exit
// code and its stderr.
func copyOf(t *testing.T, src string) (string, int, string) {
	t.Helper()

	dst := filepath.Join(t.TempDir(), "dst")
	if err := os.Mkdir(dst, 0o700); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer

	code := hostSystem().run(t.Context(), Copy{Src: src, Dst: dst}.Args(), &stdout, &stderr)

	return dst, code, stderr.String()
}

// TestTheCopyKeepsEveryPropertyOfTheTree copies a tree holding a file of another owner, setuid and
// setgid and sticky modes, both kinds of symlink, a hard-link pair, an empty directory and past times,
// and compares both trees entry by entry.
func TestTheCopyKeepsEveryPropertyOfTheTree(t *testing.T) {
	t.Parallel()

	src := filepath.Join(t.TempDir(), "src")
	if err := os.Mkdir(src, 0o700); err != nil {
		t.Fatal(err)
	}

	buildTree(t, src)

	dst, code, stderr := copyOf(t, src)
	if code != 0 {
		t.Fatalf("copy exit = %d, want 0 (stderr %q)", code, stderr)
	}

	want, got := manifest(t, src), manifest(t, dst)

	t.Logf("entries compared=%d", len(want))

	if len(want) == 0 {
		t.Fatal("entries compared=0: the source tree is empty, so the comparison proves nothing")
	}

	if !slices.Equal(got, want) {
		for index := range max(len(got), len(want)) {
			if index >= len(got) || index >= len(want) || got[index] != want[index] {
				t.Errorf("entry %d differs:\n got %+v\nwant %+v", index, at(got, index), at(want, index))
			}
		}
	}
}

func at(entries []entry, index int) entry {
	if index < len(entries) {
		return entries[index]
	}

	return entry{}
}

// TestTheCopyRefusesASpecialFile fails the copy on an entry no volume should hold, naming it, rather
// than copying something that is not data.
func TestTheCopyRefusesASpecialFile(t *testing.T) {
	t.Parallel()

	for _, special := range []struct {
		make func(t *testing.T, at string) error
		name string
	}{
		{name: "fifo", make: func(_ *testing.T, at string) error { return syscall.Mkfifo(at, 0o600) }},
		{name: "socket", make: func(t *testing.T, at string) error {
			t.Helper()

			var config net.ListenConfig

			listener, err := config.Listen(t.Context(), "unix", at)
			if err == nil {
				t.Cleanup(func() { _ = listener.Close() })
			}

			return err
		}},
	} {
		t.Run(special.name, func(t *testing.T) {
			t.Parallel()

			src := filepath.Join(t.TempDir(), "src")
			if err := os.MkdirAll(filepath.Join(src, "dir"), 0o700); err != nil {
				t.Fatal(err)
			}

			if err := special.make(t, filepath.Join(src, "dir", "pipe")); err != nil {
				t.Fatalf("make the %s: %v", special.name, err)
			}

			_, code, stderr := copyOf(t, src)
			if code != exitFailure {
				t.Fatalf("copy exit = %d, want %d (stderr %q)", code, exitFailure, stderr)
			}

			if !strings.Contains(stderr, "dir/pipe") || !strings.Contains(stderr, "cannot be copied") {
				t.Errorf("stderr = %q, want it to name dir/pipe as an entry that cannot be copied", stderr)
			}
		})
	}
}

// TestTheCopyNeedsTwoDirectories refuses a source or destination that is not a directory before
// anything is written.
func TestTheCopyNeedsTwoDirectories(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer

	code := hostSystem().run(t.Context(), Copy{Src: file, Dst: t.TempDir()}.Args(), &stdout, &stderr)
	if code != exitFailure || !strings.Contains(stderr.String(), "not a directory") {
		t.Errorf("copy from a file: exit = %d, stderr %q; want %d naming a non-directory", code, stderr.String(),
			exitFailure)
	}

	err := copyTree(t.TempDir(), filepath.Join(t.TempDir(), "absent"))
	if !errors.Is(err, errCopy) {
		t.Errorf("copy into an absent destination: err = %v, want errCopy", err)
	}
}
