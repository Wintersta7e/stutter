package compose

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// CheckSource refuses a host path no container Stutter creates may mount (K10): one that is
// missing, that is not a regular file or a directory (or a symlink to one), that lies on a kernel
// pseudo-filesystem, or under which prints saw a socket, device or FIFO. It only ever reads the
// path: a missing source is never created. This is the one declaration of these conditions; every
// caller that validates a bind source calls it.
func CheckSource(path string, prints Prints) error {
	refuse := &Refusal{Key: path, Class: K10}

	info, err := os.Lstat(path)
	if err != nil {
		return refuse
	}

	if info.Mode()&fs.ModeSymlink != 0 {
		if info, err = os.Stat(path); err != nil {
			return refuse
		}
	}

	if !info.Mode().IsRegular() && !info.IsDir() {
		return refuse
	}

	if pseudoFilesystem(path) {
		return refuse
	}

	for _, special := range prints.special {
		if special == path || strings.HasPrefix(special, path+string(filepath.Separator)) {
			return refuse
		}
	}

	return nil
}
