//go:build linux

package enginetest_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision"
)

// absentImage is a reference no registry or engine holds.
const absentImage = "stutter-test-absent-image:never"

// TestALocalImageReadTellsAbsentFromPresent: the engine's own answer for a reference it does not hold
// reads as absent, not as a failure, and a present image reads as the one resolving it then pins.
func TestALocalImageReadTellsAbsentFromPresent(t *testing.T) {
	t.Parallel()

	requireEngine(t)
	engine := openEngine(t, provision.Options{})

	_, found, err := engine.LocalImage(t.Context(), absentImage)
	if err != nil || found {
		t.Fatalf("LocalImage(%s) = (%v, %v), want absent", absentImage, found, err)
	}

	pinned := pinImage(t, engine, testImage)

	local, present, err := engine.LocalImage(t.Context(), testImage)
	if err != nil || !present || local.ID != pinned.ID {
		t.Errorf("LocalImage(%s) = (%s, %v, %v), want %s", testImage, local.ID, present, err, pinned.ID)
	}
}
