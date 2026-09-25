package compose

import "strings"

// Verdict is what Stutter does with one compose key.
type Verdict uint8

const (
	// VerdictHonour keeps compose's semantics.
	VerdictHonour Verdict = iota + 1
	// VerdictReplace substitutes Stutter's value, and the report names the key.
	VerdictReplace
	// VerdictRefuse stops the check naming the key and its class.
	VerdictRefuse
	// VerdictIgnore changes neither the code that runs nor what it reaches.
	VerdictIgnore
	// VerdictModel is read to understand the project and changes nothing on the container.
	VerdictModel
	// VerdictConditional depends on the value: honoured, replaced or refused by what the key says.
	VerdictConditional
)

// Compose keys named in more than one table.
const (
	keyDeploy      = "deploy"
	keyDevices     = "devices"
	keyModels      = "models"
	keyNetworks    = "networks"
	keyNetworkMode = "network_mode"
)

// keyRule is one row of the verdict table. A path is dotted; list elements share their parent's
// path, and `*` stands for a user-named map key. A subtree rule classifies every path beneath it.
type keyRule struct {
	path    string
	verdict Verdict
	class   Class
	subtree bool
}

// serviceKeys is the verdict of every key a compose service may carry. It is closed: a key it does
// not list, at any depth, is refused.
//
//nolint:gochecknoglobals // a fixed table, not mutable state.
var serviceKeys = []keyRule{
	// Process and identity.
	{path: "image", verdict: VerdictHonour},
	{path: "build", verdict: VerdictHonour, subtree: true},
	{path: "platform", verdict: VerdictHonour},
	{path: "command", verdict: VerdictHonour},
	{path: "entrypoint", verdict: VerdictHonour},
	{path: "environment", verdict: VerdictHonour, subtree: true},
	{path: "working_dir", verdict: VerdictHonour},
	{path: "user", verdict: VerdictHonour},
	{path: "group_add", verdict: VerdictHonour},
	{path: "init", verdict: VerdictHonour},
	{path: "read_only", verdict: VerdictHonour},
	{path: "tmpfs", verdict: VerdictHonour},
	{path: "sysctls", verdict: VerdictHonour, subtree: true},
	{path: "security_opt", verdict: VerdictHonour},
	{path: "cap_drop", verdict: VerdictHonour},
	{path: "cap_add", verdict: VerdictConditional, class: K2},
	{path: "domainname", verdict: VerdictHonour},
	{path: "mac_address", verdict: VerdictHonour},
	{path: "stdin_open", verdict: VerdictHonour},
	{path: "tty", verdict: VerdictHonour},
	{path: "hostname", verdict: VerdictHonour},
	{path: "privileged", verdict: VerdictConditional, class: K1},
	{path: "use_api_socket", verdict: VerdictConditional, class: K7},
	{path: "pull_policy", verdict: VerdictConditional},
	{path: "pull_refresh_after", verdict: VerdictReplace},
	// Resources.
	{path: "shm_size", verdict: VerdictHonour},
	{path: "ulimits", verdict: VerdictHonour, subtree: true},
	{path: "cpu_count", verdict: VerdictHonour},
	{path: "cpu_percent", verdict: VerdictHonour},
	{path: "cpu_period", verdict: VerdictHonour},
	{path: "cpu_quota", verdict: VerdictHonour},
	{path: "cpu_rt_period", verdict: VerdictHonour},
	{path: "cpu_rt_runtime", verdict: VerdictHonour},
	{path: "cpu_shares", verdict: VerdictHonour},
	{path: "cpus", verdict: VerdictHonour},
	{path: "cpuset", verdict: VerdictHonour},
	{path: "mem_limit", verdict: VerdictHonour},
	{path: "mem_reservation", verdict: VerdictHonour},
	{path: "mem_swappiness", verdict: VerdictHonour},
	{path: "memswap_limit", verdict: VerdictHonour},
	{path: "oom_kill_disable", verdict: VerdictHonour},
	{path: "oom_score_adj", verdict: VerdictHonour},
	{path: "pids_limit", verdict: VerdictHonour},
	{path: "blkio_config", verdict: VerdictHonour, subtree: true},
	{path: "storage_opt", verdict: VerdictHonour, subtree: true},
	{path: "cgroup_parent", verdict: VerdictHonour},
	{path: "deploy.resources.limits", verdict: VerdictHonour, subtree: true},
	{path: "deploy.resources.reservations.cpus", verdict: VerdictHonour},
	{path: "deploy.resources.reservations.memory", verdict: VerdictHonour},
	// Namespaces.
	{path: "ipc", verdict: VerdictConditional, class: K4},
	{path: "cgroup", verdict: VerdictConditional, class: K4},
	{path: "pid", verdict: VerdictRefuse, class: K4},
	{path: "uts", verdict: VerdictRefuse, class: K4},
	{path: "userns_mode", verdict: VerdictRefuse, class: K4},
	// Mounts.
	{path: "volumes", verdict: VerdictHonour},
	{path: "volumes.type", verdict: VerdictConditional, class: K9},
	{path: "volumes.source", verdict: VerdictHonour},
	{path: "volumes.target", verdict: VerdictHonour},
	{path: "volumes.read_only", verdict: VerdictHonour},
	{path: "volumes.consistency", verdict: VerdictIgnore},
	{path: "volumes.bind.propagation", verdict: VerdictIgnore},
	{path: "volumes.bind.create_host_path", verdict: VerdictIgnore},
	{path: "volumes.bind.recursive", verdict: VerdictReplace},
	{path: "volumes.bind.selinux", verdict: VerdictRefuse, class: K9},
	{path: "volumes.volume.nocopy", verdict: VerdictHonour},
	{path: "volumes.volume.subpath", verdict: VerdictRefuse, class: K9},
	{path: "volumes.volume.labels", verdict: VerdictIgnore, subtree: true},
	{path: "volumes.tmpfs", verdict: VerdictHonour, subtree: true},
	{path: "volumes.image", verdict: VerdictRefuse, class: K9, subtree: true},
	// A config or secret's uid, gid and mode apply to a copied-in file and are moot on a bind.
	{path: "configs", verdict: VerdictHonour},
	{path: "configs.source", verdict: VerdictHonour},
	{path: "configs.target", verdict: VerdictHonour},
	{path: "configs.uid", verdict: VerdictHonour},
	{path: "configs.gid", verdict: VerdictHonour},
	{path: "configs.mode", verdict: VerdictHonour},
	{path: "secrets", verdict: VerdictHonour},
	{path: "secrets.source", verdict: VerdictHonour},
	{path: "secrets.target", verdict: VerdictHonour},
	{path: "secrets.uid", verdict: VerdictHonour},
	{path: "secrets.gid", verdict: VerdictHonour},
	{path: "secrets.mode", verdict: VerdictHonour},
	// Replaced by Stutter's own value.
	{path: "restart", verdict: VerdictReplace},
	{path: "deploy.restart_policy", verdict: VerdictReplace, subtree: true},
	{path: "healthcheck", verdict: VerdictReplace, subtree: true},
	{path: "logging", verdict: VerdictReplace, subtree: true},
	{path: keyNetworks, verdict: VerdictReplace},
	{path: "networks.*", verdict: VerdictReplace},
	{path: "networks.*.aliases", verdict: VerdictModel},
	{path: "networks.*.ipv4_address", verdict: VerdictReplace},
	{path: "networks.*.ipv6_address", verdict: VerdictReplace},
	{path: "networks.*.link_local_ips", verdict: VerdictReplace},
	{path: "networks.*.mac_address", verdict: VerdictReplace},
	{path: "networks.*.driver_opts", verdict: VerdictReplace, subtree: true},
	{path: "networks.*.priority", verdict: VerdictReplace},
	{path: "networks.*.gw_priority", verdict: VerdictReplace},
	{path: "networks.*.interface_name", verdict: VerdictReplace},
	{path: keyNetworkMode, verdict: VerdictConditional, class: K3},
	{path: "dns", verdict: VerdictReplace},
	{path: "dns_search", verdict: VerdictReplace},
	{path: "dns_opt", verdict: VerdictReplace},
	{path: "extra_hosts", verdict: VerdictReplace, subtree: true},
	{path: "scale", verdict: VerdictReplace},
	{path: "deploy.replicas", verdict: VerdictReplace},
	{path: "deploy.mode", verdict: VerdictReplace},
	{path: "stop_signal", verdict: VerdictReplace},
	{path: "stop_grace_period", verdict: VerdictReplace},
	// Refused whatever the value.
	{path: keyDevices, verdict: VerdictRefuse, class: K5, subtree: true},
	{path: "device_cgroup_rules", verdict: VerdictRefuse, class: K5},
	{path: "gpus", verdict: VerdictRefuse, class: K5, subtree: true},
	{path: "deploy.resources.reservations.devices", verdict: VerdictRefuse, class: K5, subtree: true},
	{path: "deploy.resources.reservations.generic_resources", verdict: VerdictRefuse, class: K5, subtree: true},
	{path: "runtime", verdict: VerdictRefuse, class: K6},
	{path: "isolation", verdict: VerdictConditional, class: K6},
	{path: "credential_spec", verdict: VerdictRefuse, class: K6, subtree: true},
	{path: "volumes_from", verdict: VerdictRefuse, class: K7},
	{path: "provider", verdict: VerdictRefuse, class: K7, subtree: true},
	{path: keyModels, verdict: VerdictRefuse, class: K7, subtree: true},
	{path: "pre_start", verdict: VerdictRefuse, class: K8, subtree: true},
	{path: "post_start", verdict: VerdictRefuse, class: K8, subtree: true},
	{path: "pre_stop", verdict: VerdictRefuse, class: K8, subtree: true},
	// Ignored: they change neither the code that runs nor what it reaches.
	{path: "container_name", verdict: VerdictIgnore},
	{path: "ports", verdict: VerdictIgnore, subtree: true},
	{path: "expose", verdict: VerdictIgnore},
	{path: "labels", verdict: VerdictIgnore, subtree: true},
	{path: "label_file", verdict: VerdictIgnore},
	{path: "annotations", verdict: VerdictIgnore, subtree: true},
	{path: "attach", verdict: VerdictIgnore},
	{path: "develop", verdict: VerdictIgnore, subtree: true},
	{path: "deploy.placement", verdict: VerdictIgnore, subtree: true},
	{path: "deploy.update_config", verdict: VerdictIgnore, subtree: true},
	{path: "deploy.rollback_config", verdict: VerdictIgnore, subtree: true},
	{path: "deploy.endpoint_mode", verdict: VerdictIgnore},
	{path: "deploy.labels", verdict: VerdictIgnore, subtree: true},
	// Read to understand the project.
	{path: "depends_on", verdict: VerdictModel, subtree: true},
	{path: "links", verdict: VerdictModel},
	{path: "external_links", verdict: VerdictModel},
	{path: "profiles", verdict: VerdictModel},
	{path: "env_file", verdict: VerdictModel, subtree: true},
	{path: "extends", verdict: VerdictModel, subtree: true},
}

// projectKeys is the verdict of every top-level key. `services` walks serviceKeys; a top-level
// `x-*` key never reaches the table — `x-stutter` is read on its own and every other is ignored.
//
//nolint:gochecknoglobals // a fixed table, not mutable state.
var projectKeys = []keyRule{
	{path: "services", verdict: VerdictHonour},
	{path: "name", verdict: VerdictModel},
	{path: "include", verdict: VerdictModel, subtree: true},
	{path: "version", verdict: VerdictIgnore},
	{path: "networks", verdict: VerdictIgnore, subtree: true},
	// Top-level `models` defines models; attaching one to a service is the service key, refused.
	{path: "models", verdict: VerdictIgnore, subtree: true},
	// A named volume is replaced by a fresh one per container, whatever its driver.
	{path: "volumes.*.driver", verdict: VerdictReplace},
	{path: "volumes.*.driver_opts", verdict: VerdictIgnore, subtree: true},
	{path: "volumes.*.external", verdict: VerdictRefuse, class: K9, subtree: true},
	{path: "volumes.*.labels", verdict: VerdictIgnore, subtree: true},
	{path: "volumes.*.name", verdict: VerdictIgnore},
	{path: "configs.*.content", verdict: VerdictHonour},
	{path: "configs.*.environment", verdict: VerdictHonour},
	{path: "configs.*.file", verdict: VerdictHonour},
	{path: "configs.*.external", verdict: VerdictRefuse, class: K11, subtree: true},
	{path: "configs.*.template_driver", verdict: VerdictRefuse, class: K11},
	{path: "configs.*.labels", verdict: VerdictIgnore, subtree: true},
	{path: "configs.*.name", verdict: VerdictIgnore},
	{path: "secrets.*.environment", verdict: VerdictHonour},
	{path: "secrets.*.file", verdict: VerdictHonour},
	{path: "secrets.*.external", verdict: VerdictRefuse, class: K11, subtree: true},
	{path: "secrets.*.driver", verdict: VerdictRefuse, class: K11},
	{path: "secrets.*.template_driver", verdict: VerdictRefuse, class: K11},
	{path: "secrets.*.driver_opts", verdict: VerdictIgnore, subtree: true},
	{path: "secrets.*.labels", verdict: VerdictIgnore, subtree: true},
	{path: "secrets.*.name", verdict: VerdictIgnore},
}

// keyTable indexes a verdict table for the walk.
type keyTable struct {
	rules map[string]keyRule
	// inner holds every proper prefix of a rule path: a structural node, classified by its children.
	inner map[string]bool
}

func newKeyTable(rules []keyRule) keyTable {
	table := keyTable{rules: make(map[string]keyRule, len(rules)), inner: make(map[string]bool)}

	for _, rule := range rules {
		table.rules[rule.path] = rule

		parts := strings.Split(rule.path, ".")
		for n := 1; n < len(parts); n++ {
			table.inner[strings.Join(parts[:n], ".")] = true
		}
	}

	return table
}

// known reports a path the table has a rule for, or a structural node above one.
func (k keyTable) known(path string) bool {
	_, ok := k.rules[path]

	return ok || k.inner[path]
}
