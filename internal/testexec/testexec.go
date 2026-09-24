// Package testexec writes the executables tests run in place of real programs.
package testexec

import (
	"os"
	"syscall"
	"testing"
)

// scriptMode lets the test's own user run the script.
const scriptMode = 0o755

// WriteScript writes an executable script at path.
//
// It holds the fork lock while the file is open for writing. A child that another parallel test forks
// meanwhile inherits the write descriptor until it execs, and running the script then fails with
// "text file busy": measured, 34 such failures in 150 runs of a package whose tests start scripts in
// parallel, and none once the write held the lock.
func WriteScript(tb testing.TB, path, script string) {
	tb.Helper()

	syscall.ForkLock.Lock()
	err := os.WriteFile(path, []byte(script), scriptMode)
	syscall.ForkLock.Unlock()

	if err != nil {
		tb.Fatalf("write %s: %v", path, err)
	}
}
