package provision

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// HostName is an entry another part of Stutter stages in the check-private directory. The
// directory's layout has one owner, this file; each entry's content has its own.
type HostName string

// The staged entries. The checkpoints are siblings of the store, never inside it: a restore replaces
// the store, and a checkpoint beneath it would go with it.
const (
	// HostCA is the per-check CA certificate file.
	HostCA HostName = "ca.pem"
	// HostStore is the bus store directory.
	HostStore HostName = "bus/store"
	// HostB0 is the first bus checkpoint directory.
	HostB0 HostName = "bus/B0"
	// HostB1 is the second bus checkpoint directory.
	HostB1 HostName = "bus/B1"
)

// errUnknownHostName means a caller asked for an entry the layout does not define.
var errUnknownHostName = errors.New("not an entry of the check-private directory")

// hostNames maps each staged entry to what it is: a file or a directory.
func hostNames() map[HostName]string {
	return map[HostName]string{HostCA: hostFile, HostStore: hostDir, HostB0: hostDir, HostB1: hostDir}
}

// makePrivate creates the check-private directory at path, ledgered as a host path: never reused
// (Mkdir, not MkdirAll), and verified a local directory this user alone owns, mode 0700. A
// directory it made and then refused is removed again. It returns the entry's seq.
func makePrivate(path string, led *ledger, host hostFS) (int, error) {
	seq := led.next()

	if err := led.append(entry{Seq: seq, Op: opIntent, Type: ResourceHostPath, Name: path}); err != nil {
		return seq, fmt.Errorf("%w: %w", ErrStateDir, err)
	}

	if err := os.Mkdir(path, privateMode); err != nil {
		return seq, fmt.Errorf("%w: %s: %w", ErrPrivateDir, path, err)
	}

	if err := led.append(entry{Seq: seq, Op: opCreated, Type: ResourceHostPath, Name: path}); err != nil {
		return seq, errors.Join(fmt.Errorf("%w: %w", ErrStateDir, err), removeMade(path))
	}

	if err := host.private(path); err != nil {
		return seq, errors.Join(fmt.Errorf("%w: %s: %w", ErrPrivateDir, path, err), removeMade(path))
	}

	if err := led.append(entry{Seq: seq, Op: opVerified, Type: ResourceHostPath, Name: path}); err != nil {
		return seq, errors.Join(fmt.Errorf("%w: %w", ErrStateDir, err), removeMade(path))
	}

	for _, dir := range []string{dockerConfigDir, logsDir} {
		if err := os.Mkdir(filepath.Join(path, dir), privateMode); err != nil {
			return seq, errors.Join(fmt.Errorf("%w: %s: %w", ErrPrivateDir, path, err), removeMade(path))
		}
	}

	return seq, nil
}

// removeMade removes a directory Stutter made, never following a symlink: a symlink found in its
// place is removed as a link.
func removeMade(path string) error {
	info, err := os.Lstat(path)

	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("remove %s: %w", path, err)
	case info.IsDir():
		return removeTree(path)
	default:
		return removeFile(path)
	}
}

func removeTree(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove %s: %w", path, err)
	}

	return nil
}

func removeFile(path string) error {
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove %s: %w", path, err)
	}

	return nil
}

// HostPath returns where the entry name lives, creating its parent directory (0700) on first use
// but never the entry itself: its content owner creates that. The path is recorded in the
// invocation log.
func (e *Engine) HostPath(name HostName) (string, error) {
	kind, ok := hostNames()[name]
	if !ok {
		return "", fmt.Errorf("host path %q: %w", name, errUnknownHostName)
	}

	path := filepath.Join(e.private, string(name))

	e.mu.Lock()
	defer e.mu.Unlock()

	if parent := filepath.Dir(path); parent != e.private {
		if err := e.ensureDir(parent); err != nil {
			return "", err
		}
	}

	if err := e.recordHostPath(kind, path); err != nil {
		return "", err
	}

	return path, nil
}

// ensureDir makes a private directory under the check-private one, or verifies the one there.
func (e *Engine) ensureDir(path string) error {
	err := os.Mkdir(path, privateMode)

	switch {
	case err == nil:
		return e.recordHostPath(hostDir, path)
	case errors.Is(err, fs.ErrExist):
		if refused := e.host.private(path); refused != nil {
			return fmt.Errorf("%w: %s: %w", ErrPrivateDir, path, refused)
		}

		return nil
	default:
		return fmt.Errorf("%w: %s: %w", ErrPrivateDir, path, err)
	}
}
