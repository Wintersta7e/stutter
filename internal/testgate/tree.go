package testgate

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	// minStatusEntry is "XY p": two status letters, a space and at least one byte of path.
	minStatusEntry = 4
	// maxStatusEntry bounds one entry of git status output: a path, however deep.
	maxStatusEntry = 1 << 16
)

var (
	// errNoSnapshot means a compare was asked for with no snapshot taken before the suite.
	errNoSnapshot = errors.New("no tree snapshot")
	// errPorcelain means git status output was not the porcelain v1 -z form.
	errPorcelain = errors.New("git status porcelain")
)

// treeEntry is one path git status lists: its two status letters and a hash of what the path holds.
type treeEntry struct {
	Status string `json:"status"`
	Hash   string `json:"hash"`
}

// Tree is what git status listed at one moment, each path with a hash of its content, so a path
// whose status is unchanged but whose content changed still reads as changed.
type Tree struct {
	entries map[string]treeEntry
}

// savedTree is a Tree's saved form.
type savedTree struct {
	Entries map[string]treeEntry `json:"entries"`
}

// ReadTree reads git status --porcelain=v1 -z from porcelain and hashes each listed path under root.
func ReadTree(porcelain io.Reader, root string) (Tree, error) {
	tree := Tree{entries: map[string]treeEntry{}}

	fields := bufio.NewScanner(porcelain)
	fields.Buffer(nil, maxStatusEntry)
	fields.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if i := bytes.IndexByte(data, 0); i >= 0 {
			return i + 1, data[:i], nil
		}

		if atEOF && len(data) > 0 {
			return len(data), data, nil
		}

		return 0, nil, nil
	})

	for fields.Scan() {
		entry := fields.Text()
		if entry == "" {
			continue
		}

		if len(entry) < minStatusEntry || entry[2] != ' ' {
			return Tree{}, fmt.Errorf("%w: %q is not an entry", errPorcelain, entry)
		}

		status, path := entry[:2], entry[3:]

		// A rename or copy is followed by the path it came from, which is not an entry of its own.
		if strings.ContainsAny(status, "RC") {
			fields.Scan()
		}

		hash, err := hashPath(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return Tree{}, err
		}

		tree.entries[path] = treeEntry{Status: status, Hash: hash}
	}

	if err := fields.Err(); err != nil {
		return Tree{}, fmt.Errorf("%w: %w", errPorcelain, err)
	}

	return tree, nil
}

// LoadTree reads a saved snapshot. An empty input is no snapshot at all, and fails.
func LoadTree(r io.Reader) (Tree, error) {
	var saved savedTree

	if err := json.NewDecoder(r).Decode(&saved); err != nil {
		if errors.Is(err, io.EOF) {
			return Tree{}, fmt.Errorf("%w: the snapshot is absent or empty", errNoSnapshot)
		}

		return Tree{}, fmt.Errorf("%w: unreadable: %w", errNoSnapshot, err)
	}

	if saved.Entries == nil {
		return Tree{}, fmt.Errorf("%w: the snapshot holds no entry list", errNoSnapshot)
	}

	return Tree{entries: saved.Entries}, nil
}

// Save writes the tree in the form LoadTree reads.
func (t Tree) Save(w io.Writer) error {
	entries := t.entries
	if entries == nil {
		entries = map[string]treeEntry{}
	}

	if err := json.NewEncoder(w).Encode(savedTree{Entries: entries}); err != nil {
		return fmt.Errorf("saving the tree: %w", err)
	}

	return nil
}

// Changed returns, sorted, every path whose entry differs from before: new, gone, or changed in
// status or content.
func (t Tree) Changed(before Tree) []string {
	var changed []string

	for path, entry := range t.entries {
		if was, ok := before.entries[path]; !ok || was != entry {
			changed = append(changed, path)
		}
	}

	for path := range before.entries {
		if _, ok := t.entries[path]; !ok {
			changed = append(changed, path)
		}
	}

	slices.Sort(changed)

	return changed
}

// hashPath hashes what a path holds: a file's bytes, a link's target; "absent" and "directory"
// stand for themselves.
func hashPath(path string) (string, error) {
	info, err := os.Lstat(path)

	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "absent", nil
	case err != nil:
		return "", fmt.Errorf("reading %s: %w", path, err)
	case info.Mode()&fs.ModeSymlink != 0:
		target, linkErr := os.Readlink(path)
		if linkErr != nil {
			return "", fmt.Errorf("reading the link %s: %w", path, linkErr)
		}

		return "link:" + target, nil
	case info.IsDir():
		return "directory", nil
	default:
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", path, err)
	}

	sum := sha256.Sum256(content)

	return hex.EncodeToString(sum[:]), nil
}
