//go:build !linux

package relay

import (
	"fmt"
	"runtime"
)

// copyTree needs Linux's ownership and utimensat. A copy helper only ever runs in a Linux container,
// so this is reached by nothing but a build for another system.
func copyTree(string, string) error {
	return fmt.Errorf("%w: the copy runs only on linux, not %s", errCopy, runtime.GOOS)
}
