package testexec_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testexec"
)

// TestAWrittenScriptIsExecutable: the script is written as given, and its owner may run it.
func TestAWrittenScriptIsExecutable(t *testing.T) {
	t.Parallel()

	const script = "#!/bin/sh\nprintf ran\n"

	path := filepath.Join(t.TempDir(), "shim")
	testexec.WriteScript(t, path, script)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}

	if info.Mode().Perm()&0o100 == 0 {
		t.Errorf("mode = %s, want its owner able to run it", info.Mode())
	}

	written, err := os.ReadFile(path)
	if err != nil || string(written) != script {
		t.Errorf("ReadFile() = %q, %v, want the script as given", written, err)
	}
}
