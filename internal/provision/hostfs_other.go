//go:build !linux

package provision

import (
	"fmt"
	"io/fs"
	"os"
)

// defaultHostFS refuses every fact: the compose path runs only on Linux, and Preconditions refuses
// every other host first.
func defaultHostFS() hostFS {
	return hostFS{
		statfs:  func(string) (int64, error) { return 0, notLinux() },
		tryLock: func(*os.File) (bool, error) { return false, notLinux() },
		sync:    func(*os.File) error { return notLinux() },
		lstat:   func(string) (fs.FileInfo, error) { return nil, notLinux() },
		owner:   func(fs.FileInfo) (int, bool) { return -1, false },
		euid:    -1,
	}
}

func bootID() (string, error) { return "", notLinux() }

func pidNamespace() (string, error) { return "", notLinux() }

func startTime(int) (uint64, error) { return 0, notLinux() }

func notLinux() error {
	return fmt.Errorf("%w: host facts are read only on Linux", ErrPrecondition)
}
