package provision

import (
	"context"
	"fmt"
	"net/netip"
	"strconv"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// containerTemplate reads everything the post-create verification and the compose model's audit
// read of a container, as one snake_case object. A container's inspect fails outright on a key its
// JSON lacks, so every key that can be absent is read with index; every nested object is rebuilt
// here rather than decoded in the engine's own naming.
const containerTemplate = `{"id":{{json .Id}},"image":{{json .Image}},` +
	`"labels":{{json (index .Config "Labels")}},"user":{{json (index .Config "User")}},` +
	`"working_dir":{{json (index .Config "WorkingDir")}},"hostname":{{json (index .Config "Hostname")}},` +
	`"domainname":{{json (index .Config "Domainname")}},"env":{{json (index .Config "Env")}},` +
	`"entrypoint":{{json (index .Config "Entrypoint")}},"cmd":{{json (index .Config "Cmd")}},` +
	`"healthcheck":{{with index .Config "Healthcheck"}}{{json (index . "Test")}}{{else}}null{{end}},` +
	`"restart":{{json (index .HostConfig.RestartPolicy "Name")}},` +
	`"auto_remove":{{json (index .HostConfig "AutoRemove")}},` +
	`"log_driver":{{json (index .HostConfig.LogConfig "Type")}},` +
	`"privileged":{{json (index .HostConfig "Privileged")}},` +
	`"devices":[{{range $i, $d := (index .HostConfig "Devices")}}{{if $i}},{{end}}` +
	`{{json (index $d "PathOnHost")}}{{end}}],` +
	`"device_cgroup_rules":{{json (index .HostConfig "DeviceCgroupRules")}},` +
	`"volumes_from":{{json (index .HostConfig "VolumesFrom")}},"cap_add":{{json (index .HostConfig "CapAdd")}},` +
	`"pid_mode":{{json (index .HostConfig "PidMode")}},"ipc_mode":{{json (index .HostConfig "IpcMode")}},` +
	`"uts_mode":{{json (index .HostConfig "UTSMode")}},"userns_mode":{{json (index .HostConfig "UsernsMode")}},` +
	`"cgroupns_mode":{{json (index .HostConfig "CgroupnsMode")}},"runtime":{{json (index .HostConfig "Runtime")}},` +
	`"extra_hosts":{{json (index .HostConfig "ExtraHosts")}},"dns":{{json (index .HostConfig "Dns")}},` +
	`"publish_all":{{json (index .HostConfig "PublishAllPorts")}},` +
	`"device_requests":[{{range $i, $r := (index .HostConfig "DeviceRequests")}}{{if $i}},{{end}}0{{end}}],` +
	`"network_mode":{{json (index .HostConfig "NetworkMode")}},` +
	`"bindings":[{{$first := true}}{{range $port, $list := (index .HostConfig "PortBindings")}}` +
	`{{range $b := $list}}{{if not $first}},{{end}}{{$first = false}}{"port":{{json $port}},` +
	`"host_ip":{{json (index $b "HostIp")}},"host_port":{{json (index $b "HostPort")}}}{{end}}{{end}}],` +
	`"host_mounts":[{{range $i, $m := (index .HostConfig "Mounts")}}{{if $i}},{{end}}` +
	`{"target":{{json (index $m "Target")}},"recursive":{{with index $m "BindOptions"}}` +
	`{{json (index . "ReadOnlyForceRecursive")}}{{else}}false{{end}}}{{end}}],` +
	`"mounts":[{{range $i, $m := .Mounts}}{{if $i}},{{end}}{"type":{{json (index $m "Type")}},` +
	`"name":{{json (index $m "Name")}},"source":{{json (index $m "Source")}},` +
	`"destination":{{json (index $m "Destination")}},"rw":{{json (index $m "RW")}}}{{end}}],` +
	`"networks":[{{$first := true}}{{range $name, $n := .NetworkSettings.Networks}}{{if not $first}},{{end}}` +
	`{{$first = false}}{"name":{{json $name}},"ip":{{json (index $n "IPAddress")}},` +
	`"aliases":{{json (index $n "Aliases")}}}{{end}}],` +
	`"ports":[{{$first := true}}{{range $port, $list := (index .NetworkSettings "Ports")}}` +
	`{{range $b := $list}}{{if not $first}},{{end}}{{$first = false}}{"port":{{json $port}},` +
	`"host_ip":{{json (index $b "HostIp")}},"host_port":{{json (index $b "HostPort")}}}{{end}}{{end}}],` +
	`"running":{{json .State.Running}},"exit_code":{{json .State.ExitCode}},` +
	`"oom_killed":{{json .State.OOMKilled}},` +
	`"health":{{with index .State "Health"}}{{json (index . "Status")}}{{else}}""{{end}},` +
	`"restart_count":{{json .RestartCount}}}`

// The mount types the engine reports.
const (
	mountBind   = "bind"
	mountVolume = "volume"
	mountTmpfs  = "tmpfs"
)

// portReport is one published or bound port.
type portReport struct {
	Port     string `json:"port"`
	HostIP   string `json:"host_ip"`
	HostPort string `json:"host_port"`
}

// mountReport is one entry of a container's mounts.
type mountReport struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	RW          bool   `json:"rw"`
}

// hostMountReport is one mount as the create asked for it.
type hostMountReport struct {
	Target    string `json:"target"`
	Recursive bool   `json:"recursive"`
}

// endpointReport is one network a container is attached to.
type endpointReport struct {
	Name    string   `json:"name"`
	IP      string   `json:"ip"`
	Aliases []string `json:"aliases"`
}

// containerReport is what containerTemplate prints.
type containerReport struct {
	Labels            map[string]string `json:"labels"`
	ID                string            `json:"id"`
	Image             string            `json:"image"`
	User              string            `json:"user"`
	WorkingDir        string            `json:"working_dir"`
	Hostname          string            `json:"hostname"`
	Domainname        string            `json:"domainname"`
	Restart           string            `json:"restart"`
	LogDriver         string            `json:"log_driver"`
	PidMode           string            `json:"pid_mode"`
	IpcMode           string            `json:"ipc_mode"`
	UTSMode           string            `json:"uts_mode"`
	UsernsMode        string            `json:"userns_mode"`
	CgroupnsMode      string            `json:"cgroupns_mode"`
	Runtime           string            `json:"runtime"`
	NetworkMode       string            `json:"network_mode"`
	Health            string            `json:"health"`
	Env               []string          `json:"env"`
	Entrypoint        []string          `json:"entrypoint"`
	Cmd               []string          `json:"cmd"`
	Healthcheck       []string          `json:"healthcheck"`
	Devices           []string          `json:"devices"`
	DeviceCgroupRules []string          `json:"device_cgroup_rules"`
	VolumesFrom       []string          `json:"volumes_from"`
	CapAdd            []string          `json:"cap_add"`
	ExtraHosts        []string          `json:"extra_hosts"`
	DNS               []string          `json:"dns"`
	DeviceRequests    []int             `json:"device_requests"`
	Bindings          []portReport      `json:"bindings"`
	HostMounts        []hostMountReport `json:"host_mounts"`
	Mounts            []mountReport     `json:"mounts"`
	Networks          []endpointReport  `json:"networks"`
	Ports             []portReport      `json:"ports"`
	ExitCode          int               `json:"exit_code"`
	RestartCount      int               `json:"restart_count"`
	AutoRemove        bool              `json:"auto_remove"`
	Privileged        bool              `json:"privileged"`
	PublishAll        bool              `json:"publish_all"`
	Running           bool              `json:"running"`
	OOMKilled         bool              `json:"oom_killed"`
}

// recursive reports whether the mount at target was made recursively read-only.
func (r containerReport) recursive(target string) bool {
	for _, m := range r.HostMounts {
		if m.Target == target {
			return m.Recursive
		}
	}

	return false
}

// inspected converts a report to what the compose model's audit reads.
func (r containerReport) inspected(defaultRuntime string) compose.Inspected {
	out := compose.Inspected{
		Labels: r.Labels, Image: r.Image, User: r.User, WorkingDir: r.WorkingDir, Hostname: r.Hostname,
		Domainname: r.Domainname, RestartPolicy: r.Restart, PidMode: r.PidMode, IpcMode: r.IpcMode,
		UTSMode: r.UTSMode, UsernsMode: r.UsernsMode, CgroupnsMode: r.CgroupnsMode, Runtime: r.Runtime,
		DefaultRuntime: defaultRuntime, LogDriver: r.LogDriver, Env: r.Env, Entrypoint: r.Entrypoint, Cmd: r.Cmd,
		Healthcheck: r.Healthcheck, ExtraHosts: r.ExtraHosts, CapAdd: r.CapAdd, Devices: r.Devices,
		DeviceCgroupRules: r.DeviceCgroupRules, VolumesFrom: r.VolumesFrom, DNS: r.DNS,
		PortBindings: len(r.Bindings), DeviceRequests: len(r.DeviceRequests), PublishAllPorts: r.PublishAll,
		Privileged: r.Privileged,
	}

	for _, n := range r.Networks {
		out.Networks = append(out.Networks, n.Name)
	}

	for _, m := range r.Mounts {
		out.Mounts = append(out.Mounts, compose.InspectedMount{
			Type: m.Type, Source: m.Source, Destination: m.Destination, Name: m.Name, RW: m.RW,
			ReadOnlyRecursive: r.recursive(m.Destination),
		})
	}

	return out
}

// State is a container's state, read before anything stops it: afterwards its exit code is always
// the kill's.
type State struct {
	// Log is the path of the container's copied log, or "".
	Log string
	// ExitCode is the exit code the container had when read.
	ExitCode int
	// RestartCount is how often the engine restarted it.
	RestartCount int
	// Running reports a container running when read.
	Running bool
	// OOMKilled reports a container the kernel killed for memory.
	OOMKilled bool
	// Killed reports a graceful stop that ended in SIGKILL after its grace period.
	Killed bool
}

// inspectContainer reads a container of this check.
func (e *Engine) inspectContainer(ctx context.Context, c *Container) (containerReport, error) {
	if err := e.owns(c); err != nil {
		return containerReport{}, err
	}

	var report containerReport

	found, err := e.read(ctx, request{verb: verbInspect, args: []arg{{val: containerTemplate}, {val: c.id}}},
		&report)
	if err != nil {
		return containerReport{}, err
	}

	if !found || report.ID != c.id {
		return containerReport{}, fmt.Errorf("%w: container %s is gone", ErrEngine, c.name)
	}

	return report, nil
}

// Inspect reads what the compose model's audit compares a created container against.
func (e *Engine) Inspect(ctx context.Context, c *Container) (compose.Inspected, error) {
	report, err := e.inspectContainer(ctx, c)
	if err != nil {
		return compose.Inspected{}, err
	}

	return report.inspected(e.identity.DefaultRuntime), nil
}

// Address returns a container's IPv4 address on one of the check's networks.
func (e *Engine) Address(ctx context.Context, c *Container, n *Network) (netip.Addr, error) {
	if err := e.owns(n); err != nil {
		return netip.Addr{}, err
	}

	report, err := e.inspectContainer(ctx, c)
	if err != nil {
		return netip.Addr{}, err
	}

	for _, endpoint := range report.Networks {
		if endpoint.Name == n.name {
			addr, err := netip.ParseAddr(endpoint.IP)
			if err != nil {
				return netip.Addr{}, fmt.Errorf("%w: container %s on %s: %w", ErrEngine, c.name, n.name, err)
			}

			return addr, nil
		}
	}

	return netip.Addr{}, fmt.Errorf("%w: container %s has no address on %s", ErrEngine, c.name, n.name)
}

// Published returns where a container's port is published on the host's loopback. It is read after
// Start and never before: a start refused for a taken host port moves the container to another.
func (e *Engine) Published(ctx context.Context, c *Container, port uint16) (netip.AddrPort, error) {
	report, err := e.inspectContainer(ctx, c)
	if err != nil {
		return netip.AddrPort{}, err
	}

	want := strconv.Itoa(int(port)) + "/tcp"

	for _, p := range report.Ports {
		if p.Port != want {
			continue
		}

		published, err := netip.ParseAddrPort(p.HostIP + ":" + p.HostPort)
		if err == nil && published.Addr().IsLoopback() {
			return published, nil
		}
	}

	return netip.AddrPort{}, fmt.Errorf("%w: port %s of container %s is not published on loopback", ErrEngine,
		want, c.name)
}

// Status reads a container's state.
func (e *Engine) Status(ctx context.Context, c *Container) (State, error) {
	report, err := e.inspectContainer(ctx, c)
	if err != nil {
		return State{}, err
	}

	return State{
		ExitCode: report.ExitCode, RestartCount: report.RestartCount, Running: report.Running,
		OOMKilled: report.OOMKilled,
	}, nil
}
