package compose

import "strings"

// RefusedCapability reports a capability no container Stutter creates may be given: one that
// reaches the host, the engine or the network outside Stutter's control. Names compare as the
// engine compares them — case-insensitive, with any `CAP_` prefix stripped.
func RefusedCapability(name string) bool {
	switch strings.TrimPrefix(strings.ToUpper(name), "CAP_") {
	case "ALL", "SYS_ADMIN", "DAC_READ_SEARCH", "SYS_MODULE", "SYS_RAWIO", "NET_ADMIN":
		return true
	default:
		return false
	}
}

// SharedNamespace reports the first namespace setting that joins the host's or another container's
// namespace: its compose key, and true. Any `pid`, `uts` or `userns` value is shared — the private
// default is the empty value; `ipc` may be empty, `private` or `shareable`; `cgroup` empty or
// `private`.
func SharedNamespace(pid, ipc, uts, userns, cgroup string) (string, bool) {
	switch {
	case pid != "":
		return "pid", true
	case ipc != "" && ipc != "private" && ipc != "shareable":
		return "ipc", true
	case uts != "":
		return "uts", true
	case userns != "":
		return "userns_mode", true
	case cgroup != "" && cgroup != "private":
		return "cgroup", true
	default:
		return "", false
	}
}
