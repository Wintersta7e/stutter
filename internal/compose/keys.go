package compose

import "strings"

// Verdict is what Stutter does with one compose key.
type Verdict uint8

const (
	// Honour keeps compose's semantics.
	Honour Verdict = iota + 1
	// Replace substitutes Stutter's value, and the report names the key.
	Replace
	// Refuse stops the check naming the key and its class.
	Refuse
	// Ignore changes neither the code that runs nor what it reaches.
	Ignore
	// Model is read to understand the project and changes nothing on the container.
	Model
	// Conditional depends on the value: honoured, replaced or refused by what the key says.
	Conditional
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
	{path: "image", verdict: Honour},
	{path: "build", verdict: Honour, subtree: true},
	{path: "platform", verdict: Honour},
	{path: "command", verdict: Honour},
	{path: "entrypoint", verdict: Honour},
	{path: "environment", verdict: Honour, subtree: true},
	{path: "working_dir", verdict: Honour},
	{path: "user", verdict: Honour},
	{path: "group_add", verdict: Honour},
	{path: "init", verdict: Honour},
	{path: "read_only", verdict: Honour},
	{path: "tmpfs", verdict: Honour},
	{path: "sysctls", verdict: Honour, subtree: true},
	{path: "security_opt", verdict: Honour},
	{path: "cap_drop", verdict: Honour},
	{path: "cap_add", verdict: Conditional, class: K2},
	{path: "domainname", verdict: Honour},
	{path: "mac_address", verdict: Honour},
	{path: "stdin_open", verdict: Honour},
	{path: "tty", verdict: Honour},
	{path: "hostname", verdict: Honour},
	{path: "privileged", verdict: Conditional, class: K1},
	{path: "use_api_socket", verdict: Conditional, class: K7},
	{path: "pull_policy", verdict: Conditional},
	{path: "pull_refresh_after", verdict: Replace},
	// Resources.
	{path: "shm_size", verdict: Honour},
	{path: "ulimits", verdict: Honour, subtree: true},
	{path: "cpu_count", verdict: Honour},
	{path: "cpu_percent", verdict: Honour},
	{path: "cpu_period", verdict: Honour},
	{path: "cpu_quota", verdict: Honour},
	{path: "cpu_rt_period", verdict: Honour},
	{path: "cpu_rt_runtime", verdict: Honour},
	{path: "cpu_shares", verdict: Honour},
	{path: "cpus", verdict: Honour},
	{path: "cpuset", verdict: Honour},
	{path: "mem_limit", verdict: Honour},
	{path: "mem_reservation", verdict: Honour},
	{path: "mem_swappiness", verdict: Honour},
	{path: "memswap_limit", verdict: Honour},
	{path: "oom_kill_disable", verdict: Honour},
	{path: "oom_score_adj", verdict: Honour},
	{path: "pids_limit", verdict: Honour},
	{path: "blkio_config", verdict: Honour, subtree: true},
	{path: "storage_opt", verdict: Honour, subtree: true},
	{path: "cgroup_parent", verdict: Honour},
	{path: "deploy.resources.limits", verdict: Honour, subtree: true},
	{path: "deploy.resources.reservations.cpus", verdict: Honour},
	{path: "deploy.resources.reservations.memory", verdict: Honour},
	// Namespaces.
	{path: "ipc", verdict: Conditional, class: K4},
	{path: "cgroup", verdict: Conditional, class: K4},
	{path: "pid", verdict: Refuse, class: K4},
	{path: "uts", verdict: Refuse, class: K4},
	{path: "userns_mode", verdict: Refuse, class: K4},
	// Mounts.
	{path: "volumes", verdict: Honour},
	{path: "volumes.type", verdict: Conditional, class: K9},
	{path: "volumes.source", verdict: Honour},
	{path: "volumes.target", verdict: Honour},
	{path: "volumes.read_only", verdict: Honour},
	{path: "volumes.consistency", verdict: Ignore},
	{path: "volumes.bind.propagation", verdict: Ignore},
	{path: "volumes.bind.create_host_path", verdict: Ignore},
	{path: "volumes.bind.recursive", verdict: Replace},
	{path: "volumes.bind.selinux", verdict: Refuse, class: K9},
	{path: "volumes.volume.nocopy", verdict: Honour},
	{path: "volumes.volume.subpath", verdict: Refuse, class: K9},
	{path: "volumes.volume.labels", verdict: Ignore, subtree: true},
	{path: "volumes.tmpfs", verdict: Honour, subtree: true},
	{path: "volumes.image", verdict: Refuse, class: K9, subtree: true},
	// A config or secret's uid, gid and mode apply to a copied-in file and are moot on a bind.
	{path: "configs", verdict: Honour},
	{path: "configs.source", verdict: Honour},
	{path: "configs.target", verdict: Honour},
	{path: "configs.uid", verdict: Honour},
	{path: "configs.gid", verdict: Honour},
	{path: "configs.mode", verdict: Honour},
	{path: "secrets", verdict: Honour},
	{path: "secrets.source", verdict: Honour},
	{path: "secrets.target", verdict: Honour},
	{path: "secrets.uid", verdict: Honour},
	{path: "secrets.gid", verdict: Honour},
	{path: "secrets.mode", verdict: Honour},
	// Replaced by Stutter's own value.
	{path: "restart", verdict: Replace},
	{path: "deploy.restart_policy", verdict: Replace, subtree: true},
	{path: "healthcheck", verdict: Replace, subtree: true},
	{path: "logging", verdict: Replace, subtree: true},
	{path: "networks", verdict: Replace},
	{path: "networks.*", verdict: Replace},
	{path: "networks.*.aliases", verdict: Model},
	{path: "networks.*.ipv4_address", verdict: Replace},
	{path: "networks.*.ipv6_address", verdict: Replace},
	{path: "networks.*.link_local_ips", verdict: Replace},
	{path: "networks.*.mac_address", verdict: Replace},
	{path: "networks.*.driver_opts", verdict: Replace, subtree: true},
	{path: "networks.*.priority", verdict: Replace},
	{path: "networks.*.gw_priority", verdict: Replace},
	{path: "networks.*.interface_name", verdict: Replace},
	{path: "network_mode", verdict: Conditional, class: K3},
	{path: "dns", verdict: Replace},
	{path: "dns_search", verdict: Replace},
	{path: "dns_opt", verdict: Replace},
	{path: "extra_hosts", verdict: Replace, subtree: true},
	{path: "scale", verdict: Replace},
	{path: "deploy.replicas", verdict: Replace},
	{path: "deploy.mode", verdict: Replace},
	{path: "stop_signal", verdict: Replace},
	{path: "stop_grace_period", verdict: Replace},
	// Refused whatever the value.
	{path: "devices", verdict: Refuse, class: K5, subtree: true},
	{path: "device_cgroup_rules", verdict: Refuse, class: K5},
	{path: "gpus", verdict: Refuse, class: K5, subtree: true},
	{path: "deploy.resources.reservations.devices", verdict: Refuse, class: K5, subtree: true},
	{path: "deploy.resources.reservations.generic_resources", verdict: Refuse, class: K5, subtree: true},
	{path: "runtime", verdict: Refuse, class: K6},
	{path: "isolation", verdict: Conditional, class: K6},
	{path: "credential_spec", verdict: Refuse, class: K6, subtree: true},
	{path: "volumes_from", verdict: Refuse, class: K7},
	{path: "provider", verdict: Refuse, class: K7, subtree: true},
	{path: "models", verdict: Refuse, class: K7, subtree: true},
	{path: "pre_start", verdict: Refuse, class: K8, subtree: true},
	{path: "post_start", verdict: Refuse, class: K8, subtree: true},
	{path: "pre_stop", verdict: Refuse, class: K8, subtree: true},
	// Ignored: they change neither the code that runs nor what it reaches.
	{path: "container_name", verdict: Ignore},
	{path: "ports", verdict: Ignore, subtree: true},
	{path: "expose", verdict: Ignore},
	{path: "labels", verdict: Ignore, subtree: true},
	{path: "label_file", verdict: Ignore},
	{path: "annotations", verdict: Ignore, subtree: true},
	{path: "attach", verdict: Ignore},
	{path: "develop", verdict: Ignore, subtree: true},
	{path: "deploy.placement", verdict: Ignore, subtree: true},
	{path: "deploy.update_config", verdict: Ignore, subtree: true},
	{path: "deploy.rollback_config", verdict: Ignore, subtree: true},
	{path: "deploy.endpoint_mode", verdict: Ignore},
	{path: "deploy.labels", verdict: Ignore, subtree: true},
	// Read to understand the project.
	{path: "depends_on", verdict: Model, subtree: true},
	{path: "links", verdict: Model},
	{path: "external_links", verdict: Model},
	{path: "profiles", verdict: Model},
	{path: "env_file", verdict: Model, subtree: true},
	{path: "extends", verdict: Model, subtree: true},
}

// projectKeys is the verdict of every top-level key. `services` walks serviceKeys; a top-level
// `x-*` key never reaches the table — `x-stutter` is read on its own and every other is ignored.
//
//nolint:gochecknoglobals // a fixed table, not mutable state.
var projectKeys = []keyRule{
	{path: "services", verdict: Honour},
	{path: "name", verdict: Model},
	{path: "include", verdict: Model, subtree: true},
	{path: "version", verdict: Ignore},
	{path: "networks", verdict: Ignore, subtree: true},
	// Top-level `models` defines models; attaching one to a service is the service key, refused.
	{path: "models", verdict: Ignore, subtree: true},
	// A named volume is replaced by a fresh one per container, whatever its driver.
	{path: "volumes.*.driver", verdict: Replace},
	{path: "volumes.*.driver_opts", verdict: Ignore, subtree: true},
	{path: "volumes.*.external", verdict: Refuse, class: K9, subtree: true},
	{path: "volumes.*.labels", verdict: Ignore, subtree: true},
	{path: "volumes.*.name", verdict: Ignore},
	{path: "configs.*.content", verdict: Honour},
	{path: "configs.*.environment", verdict: Honour},
	{path: "configs.*.file", verdict: Honour},
	{path: "configs.*.external", verdict: Refuse, class: K11, subtree: true},
	{path: "configs.*.template_driver", verdict: Refuse, class: K11},
	{path: "configs.*.labels", verdict: Ignore, subtree: true},
	{path: "configs.*.name", verdict: Ignore},
	{path: "secrets.*.environment", verdict: Honour},
	{path: "secrets.*.file", verdict: Honour},
	{path: "secrets.*.external", verdict: Refuse, class: K11, subtree: true},
	{path: "secrets.*.driver", verdict: Refuse, class: K11},
	{path: "secrets.*.template_driver", verdict: Refuse, class: K11},
	{path: "secrets.*.driver_opts", verdict: Ignore, subtree: true},
	{path: "secrets.*.labels", verdict: Ignore, subtree: true},
	{path: "secrets.*.name", verdict: Ignore},
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

// covered reports a path under a subtree rule, which classifies everything beneath it.
func (k keyTable) covered(path string) bool {
	parts := strings.Split(path, ".")
	for n := len(parts) - 1; n >= 1; n-- {
		if rule, ok := k.rules[strings.Join(parts[:n], ".")]; ok && rule.subtree {
			return true
		}
	}

	return false
}

// classifies reports whether the table has a verdict for path: its own rule, a subtree above it,
// or — for a structural node — rules beneath it.
func (k keyTable) classifies(path string) bool {
	return k.known(path) || k.covered(path)
}
