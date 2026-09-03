// Package version reports the build identity of the stutter binary.
package version

import "runtime/debug"

// String returns the build identity of the running binary: the module version, suffixed with the
// VCS revision when the binary was built from a checkout. It returns "unknown" when the Go runtime
// carries no build information, which happens for binaries built without module support.
func String() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return info.Main.Version + "+" + setting.Value
		}
	}

	return info.Main.Version
}
