package compose

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
)

// contextCheckEvery is how many entries a walk visits between two looks at its context.
const contextCheckEvery = 1024

// specialModes are the file types a bind source must never hold: a mount would hand the container
// a socket, a device or a pipe of the host.
const specialModes = fs.ModeSocket | fs.ModeDevice | fs.ModeCharDevice | fs.ModeNamedPipe

// Prints is a fingerprint of bind sources, taken at check start and again after the last run: any
// difference means later runs executed different input, and every finding is withheld.
type Prints struct {
	entries map[string]fingerprintEntry
	special []string
	sources int
}

// fingerprintEntry is what is recorded of one path. mtime is data compared for equality, never a
// clock a duration is measured from.
type fingerprintEntry struct {
	link  string
	sum   [sha256.Size]byte
	size  int64
	mtime int64
	mode  fs.FileMode
}

// Fingerprint records every bind source. A source is resolved as the mount resolves it; a file
// records its size, mtime and SHA-256, a directory an lstat walk of every entry beneath it — a
// symlink inside is recorded by its target string, never followed. A source that does not resolve
// is refused (K10).
func Fingerprint(ctx context.Context, sources []string) (Prints, error) {
	prints := Prints{entries: map[string]fingerprintEntry{}, sources: len(sources)}

	for _, source := range sources {
		if err := addSource(ctx, &prints, source); err != nil {
			return Prints{}, err
		}
	}

	if len(sources) > 0 && len(prints.entries) == 0 {
		return Prints{}, fmt.Errorf("%w: %d bind sources yielded no entry", ErrModel, len(sources))
	}

	slices.Sort(prints.special)
	prints.special = slices.Compact(prints.special)

	return prints, nil
}

func addSource(ctx context.Context, prints *Prints, source string) error {
	resolved, err := filepath.EvalSymlinks(source)
	if err != nil {
		return &Refusal{Key: source, Class: K10}
	}

	info, err := os.Lstat(resolved)
	if err != nil {
		return &Refusal{Key: source, Class: K10}
	}

	if !info.IsDir() {
		return record(prints, source, resolved, info, true)
	}

	visited := 0

	walkErr := filepath.WalkDir(resolved, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if visited++; visited%contextCheckEvery == 0 && ctx.Err() != nil {
			return fmt.Errorf("walk stopped: %w", ctx.Err())
		}

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("lstat %s: %w", path, err)
		}

		rel, err := filepath.Rel(resolved, path)
		if err != nil {
			return fmt.Errorf("relative path of %s: %w", path, err)
		}

		return record(prints, filepath.Join(source, rel), path, info, false)
	})
	if walkErr != nil {
		return fmt.Errorf("%w: fingerprint bind source %s: %w", ErrModel, source, walkErr)
	}

	return nil
}

// record keeps one entry, found at path and reported as key. Only a regular file is read, and only
// when hash says it is a source of its own; a special file is never opened.
func record(prints *Prints, key, path string, info fs.FileInfo, hash bool) error {
	entry := fingerprintEntry{size: info.Size(), mtime: info.ModTime().UnixNano(), mode: info.Mode()}

	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		link, err := os.Readlink(path)
		if err != nil {
			return fmt.Errorf("%w: read link %s: %w", ErrModel, key, err)
		}

		entry.link = link
	case info.Mode()&specialModes != 0:
		prints.special = append(prints.special, key)
	case hash && info.Mode().IsRegular():
		sum, err := hashFile(path)
		if err != nil {
			return fmt.Errorf("%w: fingerprint bind source %s: %w", ErrModel, key, err)
		}

		entry.sum = sum
	default:
	}

	prints.entries[key] = entry

	return nil
}

func hashFile(path string) ([sha256.Size]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("open: %w", err)
	}

	defer func() { _ = file.Close() }()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("read: %w", err)
	}

	return [sha256.Size]byte(hash.Sum(nil)), nil
}

// Changed names every path that differs between two fingerprints, appeared or vanished: sorted.
func (p Prints) Changed(later Prints) []string {
	var changed []string

	for key, entry := range p.entries {
		if other, ok := later.entries[key]; !ok || other != entry {
			changed = append(changed, key)
		}
	}

	for key := range later.entries {
		if _, ok := p.entries[key]; !ok {
			changed = append(changed, key)
		}
	}

	slices.Sort(changed)

	return changed
}

// Sources counts the sources fingerprinted.
func (p Prints) Sources() int {
	return p.sources
}

// Entries counts the entries recorded.
func (p Prints) Entries() int {
	return len(p.entries)
}

// Special names every socket, device or FIFO seen: sorted.
func (p Prints) Special() []string {
	return slices.Clone(p.special)
}
