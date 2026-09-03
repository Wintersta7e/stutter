package version_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/version"
)

func TestStringIsNeverEmpty(t *testing.T) {
	t.Parallel()

	if got := version.String(); got == "" {
		t.Error("String() returned an empty string; want a build identity")
	}
}
