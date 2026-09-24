package compose_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

func TestARefusedCapabilityMatchesAsDockerDoes(t *testing.T) {
	t.Parallel()

	refused := []string{
		"cap_net_admin", "CAP_SYS_ADMIN", "all", "ALL", "SYS_ADMIN", "DAC_READ_SEARCH",
		"sys_module", "CAP_SYS_RAWIO", "NET_ADMIN",
	}
	for _, name := range refused {
		if !compose.RefusedCapability(name) {
			t.Errorf("RefusedCapability(%q) = false, want refused", name)
		}
	}

	accepted := []string{"NET_BIND_SERVICE", "CHOWN", "cap_net_raw", "SYS_PTRACE", ""}
	for _, name := range accepted {
		if compose.RefusedCapability(name) {
			t.Errorf("RefusedCapability(%q) = true, want accepted", name)
		}
	}
}

func TestASharedNamespaceIsNamed(t *testing.T) {
	t.Parallel()

	const host = "host"

	cases := []struct {
		name                          string
		pid, ipc, uts, userns, cgroup string
		key                           string
		shared                        bool
	}{
		{name: "defaults"},
		{name: "private ipc and cgroup", ipc: "private", cgroup: "private"},
		{name: "shareable ipc", ipc: "shareable"},
		{name: "pid host", pid: host, key: pidKey, shared: true},
		{name: "pid of a service", pid: "service:db", key: pidKey, shared: true},
		{name: "ipc host", ipc: host, key: ipcKey, shared: true},
		{name: "ipc of a container", ipc: "container:x", key: ipcKey, shared: true},
		{name: "uts host", uts: host, key: utsKey, shared: true},
		{name: "userns host", userns: host, key: "userns_mode", shared: true},
		{name: "cgroup host", cgroup: host, key: "cgroup", shared: true},
		{name: "first shared key wins", uts: host, cgroup: host, key: utsKey, shared: true},
	}

	for _, tc := range cases {
		key, shared := compose.SharedNamespace(tc.pid, tc.ipc, tc.uts, tc.userns, tc.cgroup)
		if key != tc.key || shared != tc.shared {
			t.Errorf("%s: SharedNamespace = (%q, %v), want (%q, %v)", tc.name, key, shared, tc.key, tc.shared)
		}
	}
}
