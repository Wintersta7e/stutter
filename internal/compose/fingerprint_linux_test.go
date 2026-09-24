//go:build linux

package compose_test

import (
	"errors"
	"net"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// unixSocket listens on a unix socket at path until the test ends.
func unixSocket(t *testing.T, path string) {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	t.Cleanup(func() { _ = listener.Close() })
}

func TestSpecialFilesAreListed(t *testing.T) {
	t.Parallel()

	root := tree(t, map[string]string{"plain.txt": "x"})
	socket := filepath.Join(root, "s.sock")
	fifo := filepath.Join(root, "nested", "pipe")

	unixSocket(t, socket)

	if err := syscall.Mkdir(filepath.Dir(fifo), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	special := fingerprint(t, root).Special()
	if !slices.Equal(special, []string{fifo, socket}) {
		t.Errorf("Special = %v, want %s and %s", special, fifo, socket)
	}
}

func TestCheckSourceRefusesLinuxSpecials(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	fifo := filepath.Join(root, "pipe")

	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	cases := []struct{ name, path string }{
		{name: "a directory holding a FIFO", path: root},
		{name: "a FIFO", path: fifo},
		{name: "a kernel pseudo-filesystem", path: "/proc"},
	}

	for _, tc := range cases {
		// /proc is refused by its filesystem alone; walking it proves nothing.
		prints := compose.Prints{}
		if tc.path != "/proc" {
			prints = fingerprint(t, tc.path)
		}

		var refusal *compose.Refusal
		if err := compose.CheckSource(tc.path, prints); !errors.As(err, &refusal) || refusal.Class != compose.K10 ||
			refusal.Key != tc.path {
			t.Errorf("%s: CheckSource = %v, want a K10 refusal naming %s", tc.name, err, tc.path)
		}
	}
}
