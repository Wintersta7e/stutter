package compose_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

func TestComposeBelowTheFloorIsRefused(t *testing.T) {
	t.Parallel()

	cases := []struct {
		found string
		names []string
		ok    bool
	}{
		{found: "v2.29.6", names: []string{"2.29.6", compose.MinVersion}},
		{found: "", names: []string{"no compose plugin", compose.MinVersion}},
		{found: "garbage", names: []string{"garbage", compose.MinVersion}},
		{found: "2.9.10", names: []string{"2.9.10", compose.MinVersion}},
		{found: "v1.29.2", names: []string{"1.29.2", compose.MinVersion}},
		{found: "v2.29.7", ok: true},
		{found: "2.40.3", ok: true},
		{found: "v5.3.1", ok: true},
		{found: "v5.5.1-desktop.1", ok: true},
		{found: "2.30.0", ok: true},
	}

	for _, tc := range cases {
		err := compose.CheckVersion(tc.found)
		if tc.ok {
			if err != nil {
				t.Errorf("CheckVersion(%q) = %v, want accepted", tc.found, err)
			}

			continue
		}

		if !errors.Is(err, compose.ErrUnsupportedCompose) {
			t.Errorf("CheckVersion(%q) = %v, want ErrUnsupportedCompose", tc.found, err)

			continue
		}

		for _, name := range tc.names {
			if !strings.Contains(err.Error(), name) {
				t.Errorf("CheckVersion(%q) = %q, want it to name %q", tc.found, err, name)
			}
		}
	}
}
