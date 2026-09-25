//go:build linux

package provision

import (
	"errors"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// What Docker Desktop's WSL integration reported, on a real engine, for a bind made after another mount:
// the source, and the digest it named the source's place in its own directory by.
const (
	measuredSource = "/tmp/stutter-7641e0a324424edda220a3a0488ca418/ca.pem"
	measuredDigest = "a4dd0a67ad771bf2fa7af9eaa08c74f6569fc27fbd177eded0ab812d60339447"
	measuredRoot   = "/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/"
)

// A bind Docker Desktop reports at its own path for a validated source is that source; one it reports
// for another source, under another distribution, or on a host outside WSL is refused.
func TestADesktopBindPathStandsForItsValidatedSourceOnly(t *testing.T) {
	t.Parallel()

	const (
		target = "/etc/stutter/ca.pem"
		distro = "Distro"
	)

	spec := ContainerSpec{Spec: compose.Spec{Mounts: []compose.Mount{
		{Kind: compose.MountBind, Source: measuredSource, Target: target, ReadOnly: true},
	}}}

	report := func(source string) containerReport {
		return containerReport{
			Mounts:     []mountReport{{Type: mountBind, Source: source, Destination: target}},
			HostMounts: []hostMountReport{{Target: target, Recursive: true}},
		}
	}

	other := strings.Repeat("0", len(measuredDigest))

	cases := []struct {
		name, distro, source string
		want                 bool
	}{
		{"the source as given", "", measuredSource, true},
		{"the source as given, in WSL", distro, measuredSource, true},
		{"Docker Desktop's path for it", distro, measuredRoot + distro + "/" + measuredDigest, true},
		{"Docker Desktop's path outside WSL", "", measuredRoot + distro + "/" + measuredDigest, false},
		{"another distribution's path", distro, measuredRoot + "Other/" + measuredDigest, false},
		{"Docker Desktop's path for another source", distro, measuredRoot + distro + "/" + other, false},
	}

	engine, _ := openFakeEngine(t)

	for _, tc := range cases {
		engine.wslDistro = tc.distro

		err := verifyMounts(report(tc.source), engine.expect(spec, nil, nil))
		if accepted := err == nil; accepted != tc.want {
			t.Errorf("%s: verifyMounts = %v, want accepted %v", tc.name, err, tc.want)
		}

		if err != nil && !errors.Is(err, errNotAsAsked) {
			t.Errorf("%s: verifyMounts = %v, want errNotAsAsked", tc.name, err)
		}
	}

	t.Logf("cases=%d", len(cases))
}
