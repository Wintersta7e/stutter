package harness_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/harness"
)

// TestTheStartupLimitHasOneOwner: the startup limit is stated once, as a constant the CLI's usage can
// read. A second statement of it — a comment naming a number, an unexported copy — drifted from the
// value in force the moment the value changed.
func TestTheStartupLimitHasOneOwner(t *testing.T) {
	t.Parallel()

	if harness.DefaultStartup != 60*time.Second {
		t.Errorf("DefaultStartup = %s, want 1m0s", harness.DefaultStartup)
	}

	scanned := 0

	for _, name := range []string{"harness.go", "observed.go"} {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}

		scanned++

		for _, restated := range []string{"Zero is ten seconds.", "defaultStartup"} {
			if strings.Contains(string(source), restated) {
				t.Errorf("%s restates the startup limit: %q", name, restated)
			}
		}
	}

	t.Logf("scanned %d files", scanned)

	if scanned == 0 {
		t.Fatal("scanned no files, so the scan proves nothing")
	}
}
