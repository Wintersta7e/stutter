package dependencytest_test

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// treeServer is the script a fixture service runs to let a test read one of its directories: a tar of
// it on every connection to port 7000. Tests reach the engine only through the test helper, which
// copies nothing out, so a container hands its own files over.
const treeServer = "while :; do tar -c -C \"$$0\" . | nc -l -p 7000; done"

// treePort is the port treeServer serves on.
const treePort = 7000

// kindFile is a regular file's kind in a tree description.
const kindFile = "file"

// treeEntry is one path of a tree as a copy must reproduce it.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type treeEntry struct {
	kind    string
	target  string
	content string
	// links are every name of the entry's inode, sorted and joined, when it has more than one.
	links string
	size  int64
	mtime int64
	mode  int64
	uid   int
	gid   int
}

func (e treeEntry) String() string {
	return fmt.Sprintf("%s mode=%04o uid=%d gid=%d size=%d mtime=%d target=%q content=%.12s links=%q", e.kind,
		e.mode, e.uid, e.gid, e.size, e.mtime, e.target, e.content, e.links)
}

// hostTree describes a tree on the host without following a link.
func hostTree(t *testing.T, root string) map[string]treeEntry {
	t.Helper()

	entries := map[string]treeEntry{}
	names := map[[2]uint64][]string{}

	err := filepath.WalkDir(root, func(at string, _ fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(root, at)
		if err != nil {
			return err
		}

		entry, inode, err := describeHost(at)
		if err != nil {
			return err
		}

		entries[rel] = entry

		if entry.kind == kindFile {
			names[inode] = append(names[inode], rel)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	for _, group := range names {
		linkGroup(entries, group)
	}

	return entries
}

// describeHost describes one host path, and names its inode.
func describeHost(at string) (treeEntry, [2]uint64, error) {
	info, err := os.Lstat(at)
	if err != nil {
		return treeEntry{}, [2]uint64{}, err
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return treeEntry{}, [2]uint64{}, fmt.Errorf("%s: no stat", at)
	}

	entry := treeEntry{
		mode: int64(stat.Mode & 0o7777), uid: int(stat.Uid), gid: int(stat.Gid), mtime: stat.Mtim.Sec, kind: "dir",
	}

	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		entry.kind, entry.mode = "symlink", 0

		entry.target, err = os.Readlink(at)
	case info.Mode().IsRegular():
		var data []byte

		data, err = os.ReadFile(at)
		sum := sha256.Sum256(data)
		entry.kind, entry.size, entry.content = kindFile, info.Size(), hex.EncodeToString(sum[:])
	default:
	}

	return entry, [2]uint64{stat.Dev, stat.Ino}, err
}

// linkGroup marks every name of one inode with all of its names.
func linkGroup(entries map[string]treeEntry, group []string) {
	if len(group) < 2 {
		return
	}

	slices.Sort(group)

	for _, name := range group {
		entry := entries[name]
		entry.links = strings.Join(group, ",")
		entries[name] = entry
	}
}

// servedTree reads the tar a container's treeServer serves at addr, retrying while nothing listens
// yet: the engine's forwarder accepts before the server does.
func servedTree(t *testing.T, addr netip.AddrPort) map[string]treeEntry {
	t.Helper()

	deadline := time.Now().Add(time.Minute)

	for {
		entries, err := readServedTree(t, addr)
		if err == nil {
			return entries
		}

		if time.Now().After(deadline) {
			t.Fatalf("read the tree served at %s: %v", addr, err)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// readServedTree makes one attempt at reading a served tree.
func readServedTree(t *testing.T, addr netip.AddrPort) (map[string]treeEntry, error) {
	t.Helper()

	var dialer net.Dialer

	conn, err := dialer.DialContext(t.Context(), "tcp4", addr.String())
	if err != nil {
		return nil, err
	}

	defer func() { _ = conn.Close() }()

	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, err
	}

	archive := tar.NewReader(conn)
	entries := map[string]treeEntry{}
	linked := map[string]string{}

	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) && len(entries) > 0 {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("the tar ended after %d entries: %w", len(entries), err)
		}

		name := path.Clean(strings.TrimPrefix(header.Name, "./"))

		entry, err := describeServed(header, archive)
		if err != nil {
			return nil, err
		}

		if header.Typeflag == tar.TypeLink {
			linked[name] = path.Clean(strings.TrimPrefix(header.Linkname, "./"))
		}

		entries[name] = entry
	}

	resolveHardLinks(entries, linked)

	return entries, nil
}

// describeServed describes one tar entry.
func describeServed(header *tar.Header, body io.Reader) (treeEntry, error) {
	entry := treeEntry{
		mode: header.Mode & 0o7777, uid: header.Uid, gid: header.Gid, mtime: header.ModTime.Unix(), kind: "dir",
	}

	switch header.Typeflag {
	case tar.TypeSymlink:
		entry.kind, entry.target, entry.mode = "symlink", header.Linkname, 0
	case tar.TypeReg, tar.TypeLink:
		data, err := io.ReadAll(body)
		if err != nil {
			return treeEntry{}, err
		}

		sum := sha256.Sum256(data)
		entry.kind, entry.size, entry.content = kindFile, header.Size, hex.EncodeToString(sum[:])
	default:
	}

	return entry, nil
}

// resolveHardLinks gives each hard-link entry its target's content and marks every name of an inode.
func resolveHardLinks(entries map[string]treeEntry, linked map[string]string) {
	groups := map[string][]string{}

	for name, target := range linked {
		entry, first := entries[name], entries[target]
		entry.size, entry.content = first.size, first.content
		entries[name] = entry

		if !slices.Contains(groups[target], target) {
			groups[target] = append(groups[target], target)
		}

		groups[target] = append(groups[target], name)
	}

	for _, group := range groups {
		linkGroup(entries, group)
	}
}

// compareTrees fails on any entry that differs, and returns how many were compared.
func compareTrees(t *testing.T, want, got map[string]treeEntry) int {
	t.Helper()

	for _, name := range slices.Sorted(func(yield func(string) bool) {
		seen := map[string]bool{}

		for _, entries := range []map[string]treeEntry{want, got} {
			for name := range entries {
				if !seen[name] && !yield(name) {
					return
				}

				seen[name] = true
			}
		}
	}) {
		if want[name] != got[name] {
			t.Errorf("%s differs:\n got %v\nwant %v", name, got[name], want[name])
		}
	}

	return len(want)
}
