package topology

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func kernel(t *testing.T, release string) prerequisites {
	t.Helper()

	path := filepath.Join(t.TempDir(), "osrelease")
	if err := os.WriteFile(path, []byte(release), 0o600); err != nil {
		t.Fatalf("write the kernel release: %v", err)
	}

	return prerequisites{osRelease: path}
}

func answering(mode string, err error) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return mode, err }
}

// TestMirroredWSLIsRefused refuses a host whose containers could not reach its listeners, before
// anything exists to clean up.
func TestMirroredWSLIsRefused(t *testing.T) {
	t.Parallel()

	wsl := kernel(t, "6.18.33.2-microsoft-standard-WSL2\n")

	if err := wsl.check(t.Context(), answering("mirrored\n", nil)); !errors.Is(err, ErrPrerequisite) ||
		!strings.Contains(err.Error(), "mirrored") {
		t.Errorf("mirrored: err = %v, want the mirrored refusal", err)
	}

	if err := wsl.check(t.Context(), answering("", errors.New("not found"))); !errors.Is(err, ErrPrerequisite) ||
		!strings.Contains(err.Error(), "wslinfo") {
		t.Errorf("an unreadable mode: err = %v, want a refusal naming wslinfo", err)
	}

	if err := wsl.check(t.Context(), answering("nat\n", nil)); err != nil {
		t.Errorf("nat: err = %v, want nil", err)
	}

	asked := false

	native := kernel(t, "6.8.0-45-generic\n")
	if err := native.check(t.Context(), func(context.Context) (string, error) {
		asked = true

		return "mirrored", nil
	}); err != nil || asked {
		t.Errorf("a native kernel: err = %v, asked for the mode = %v; want nil and not asked", err, asked)
	}
}
