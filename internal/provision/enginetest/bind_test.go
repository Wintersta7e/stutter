//go:build linux

package enginetest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision"
)

// TestABindAfterAnotherMountIsCreated: Docker Desktop's WSL integration reports a bind made after
// another mount at a path of its own, not at its source; the created container is still recognised
// as holding the source the check validated. An engine that reports the source as given creates it
// the same way.
func TestABindAfterAnotherMountIsCreated(t *testing.T) {
	t.Parallel()

	requireEngine(t)
	engine := openEngine(t, provision.Options{})
	image := pinImage(t, engine, testImage)

	source := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(source, []byte("ca"), 0o600); err != nil {
		t.Fatal(err)
	}

	spec := shell(targetSpec(image, freeNetwork(t, engine, serviceRole)), "exit 0")
	spec.Spec.Mounts = []compose.Mount{
		{Kind: compose.MountTmpfs, Target: "/scratch"},
		{Kind: compose.MountBind, Source: source, Target: "/etc/stutter/ca.pem", ReadOnly: true},
	}

	createContainer(t, engine, spec)
}
