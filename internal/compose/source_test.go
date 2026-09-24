package compose_test

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

func TestCheckSourceRefusesMissingAndSpecial(t *testing.T) {
	t.Parallel()

	root := tree(t, map[string]string{"conf/app.yaml": "a: 1", "file.txt": "x"})
	missing := filepath.Join(root, "absent", "dir")
	dangling := filepath.Join(root, "dangling")
	socket := filepath.Join(root, "s.sock")
	linkedDir := filepath.Join(root, "linked")

	if err := os.Symlink(filepath.Join(root, "nowhere"), dangling); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if err := os.Symlink(filepath.Join(root, "conf"), linkedDir); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	t.Cleanup(func() { _ = listener.Close() })

	refused := []string{missing, dangling, socket}
	for _, path := range refused {
		prints, _ := compose.Fingerprint(t.Context(), []string{path})

		var refusal *compose.Refusal

		err := compose.CheckSource(path, prints)
		if !errors.As(err, &refusal) || refusal.Class != compose.K10 || refusal.Key != path ||
			!errors.Is(err, compose.ErrRefused) || !strings.Contains(err.Error(), path) {
			t.Errorf("CheckSource(%s) = %v, want a K10 refusal naming it", path, err)
		}
	}

	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the missing source exists after the check: %v", err)
	}

	accepted := []string{filepath.Join(root, "conf"), filepath.Join(root, "file.txt"), linkedDir}
	for _, path := range accepted {
		if err := compose.CheckSource(path, fingerprint(t, path)); err != nil {
			t.Errorf("CheckSource(%s) = %v, want accepted", path, err)
		}
	}
}
