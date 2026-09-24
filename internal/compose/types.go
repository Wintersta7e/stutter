// Package compose reads a user's compose project and derives everything Stutter needs from it: the
// container spec of the service under test and of its jobs, the key verdicts that refuse what cannot
// be run safely, the audit of each created container, and the role of every other service.
//
// The package never runs a process. Every `docker compose … config` read goes through a ConfigFunc
// the caller supplies, and the parsed model is held only in memory: it carries interpolated
// secrets, so no error, log line or formatted value this package produces ever carries a value
// from it — only service names, keys, variable names, hosts and paths.
package compose

import (
	"context"
	"io/fs"

	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// ConfigRead selects which of the three `docker compose … config` reads a ConfigFunc performs.
//
// The zero value reads the whole project as JSON. Service reads only that one service, which Parse
// uses solely to learn the profiles that activate a service absent from the whole-project read.
// Environment reads the resolved variables, because compose never resolves an `environment:`-sourced
// config or secret in its JSON output and a copy-in needs the value.
type ConfigRead struct {
	// Service, when set, appends that service name to `config --format json`.
	Service string
	// Environment asks for `config --environment` instead of the JSON model.
	Environment bool
}

// ConfigFunc runs one `docker compose <args> config …` read in the project directory dir.
//
// It returns compose's stdout and the number of lines compose printed on stderr. Compose's stderr
// itself is never returned, quoted or logged: it echoes interpolated values. A failure's error
// unwraps to something with an `ExitCode() int` method when compose ran and exited non-zero.
type ConfigFunc func(ctx context.Context, dir string, args []string, read ConfigRead) ([]byte, int, error)

// Inputs names what the user pointed Stutter at: the compose files, the extra profiles, and the
// service under test.
type Inputs struct {
	// Service is the compose service under test. Required.
	Service string
	// Files are the `--compose` files in the order given, each absolute. At least one is required.
	Files []string
	// Profiles are the `--profile` values in the order given.
	Profiles []string
}

// Image is what the engine reports about one image Stutter pinned. The driver fills it on resolve,
// build, import and commit; this package only reads it.
type Image struct {
	// Labels are the image's labels. Compose stamps its own on a built image, and a container
	// created from the image inherits them.
	Labels map[string]string
	// ID is the pinned `sha256:` image ID every container is created from.
	ID string
	// OS is the image's operating system.
	OS string
	// Arch is the image's architecture.
	Arch string
	// Variant is the image's architecture variant, recorded with the platform.
	Variant string
	// Env is the image's `ENV`, as `NAME=value` entries.
	Env []string
	// Entrypoint is the image's `ENTRYPOINT`.
	Entrypoint []string
	// Cmd is the image's `CMD`.
	Cmd []string
	// ExposedPorts are the image's `EXPOSE` entries as the engine prints them, e.g. `5432/tcp`.
	ExposedPorts []string
	// Volumes are the image's `VOLUME` paths.
	Volumes []string
}

// MountKind is what a container mount is made from.
type MountKind string

const (
	// MountBind is a host path, always mounted read-only.
	MountBind MountKind = "bind"
	// MountFresh is a new labelled volume created for the container and removed with it.
	MountFresh MountKind = "fresh"
	// MountTmpfs is a tmpfs mount.
	MountTmpfs MountKind = "tmpfs"
)

// Mount is one mount of a container Stutter creates from a compose service.
type Mount struct {
	// Kind is what the mount is made from.
	Kind MountKind
	// Source is the host path of a bind; empty otherwise.
	Source string
	// Target is the path inside the container.
	Target string
	// Volume is the model volume name a fresh mount replaces; empty for an anonymous volume and
	// for the other kinds. A job's mount is matched to a template volume by it.
	Volume string
	// Size is a tmpfs size in bytes; zero is unset.
	Size int64
	// Mode is a tmpfs mode as compose wrote it, sticky bit included as the octal 01000; zero is unset.
	Mode fs.FileMode
	// ReadOnly mounts it read-only. Every bind is read-only.
	ReadOnly bool
	// NoCopy skips copying the image's content into a fresh volume.
	NoCopy bool
}

// CopyIn is one file copied into a container before it starts: a `content:` or `environment:`
// config or secret. Its value is unexported and never printed; Data hands it to the driver's tar.
type CopyIn struct {
	// Target is the file's absolute path inside the container.
	Target string
	data   []byte
	// UID is the file's owner.
	UID int
	// GID is the file's group.
	GID int
	// Mode is the file's permission bits.
	Mode fs.FileMode
}

// Data returns the file's content. It is the one way the value leaves this package.
func (c CopyIn) Data() []byte {
	return c.data
}

// Replaced records one compose key whose value Stutter replaced, and what it did instead.
type Replaced struct {
	// Key is the compose key path.
	Key string
	// What says what Stutter did in its place.
	What string
}

// Named records a variable Stutter removed or overrode, and where the value came from.
type Named struct {
	// Name is the variable's name.
	Name string
	// Source is `compose` or `image`.
	Source string
}

// Host is one host a service's configuration names, located by the key that names it.
type Host struct {
	// Service is the service whose configuration names the host.
	Service string
	// Key is the environment key, or `command[i]`/`entrypoint[i]`, holding the reference.
	Key string
	// Host is the host alone — never the rest of the value.
	Host string
}

// Blkio is a service's `blkio_config`.
type Blkio struct {
	// WeightDevice is `blkio_config.weight_device`.
	WeightDevice []BlkioWeight
	// DeviceReadBps is `blkio_config.device_read_bps`.
	DeviceReadBps []BlkioRate
	// DeviceReadIOps is `blkio_config.device_read_iops`.
	DeviceReadIOps []BlkioRate
	// DeviceWriteBps is `blkio_config.device_write_bps`.
	DeviceWriteBps []BlkioRate
	// DeviceWriteIOps is `blkio_config.device_write_iops`.
	DeviceWriteIOps []BlkioRate
	// Weight is `blkio_config.weight`; zero is unset.
	Weight uint16
}

// BlkioWeight is one `weight_device` entry.
type BlkioWeight struct {
	// Path is the device path.
	Path string
	// Weight is the device's relative weight.
	Weight uint16
}

// BlkioRate is one throttle entry: a device and its limit.
type BlkioRate struct {
	// Path is the device path.
	Path string
	// Rate is bytes or operations per second.
	Rate int64
}

// Resources are the resource keys compose honours, as parsed. A zero value is unset.
type Resources struct {
	// Ulimits is `ulimits`: name → [soft, hard]. A single-value limit sets both.
	Ulimits map[string][2]int64
	// StorageOpt is `storage_opt`.
	StorageOpt map[string]string
	// Blkio is `blkio_config`; nil is unset.
	Blkio *Blkio
	// MemSwappiness is `mem_swappiness`; nil is unset, because zero is a valid setting.
	MemSwappiness *int64
	// Cpuset is `cpuset`.
	Cpuset string
	// CgroupParent is `cgroup_parent`.
	CgroupParent string
	// CPUs is `cpus`.
	CPUs float64
	// LimitCPUs is `deploy.resources.limits.cpus`.
	LimitCPUs float64
	// ReserveCPUs is `deploy.resources.reservations.cpus`.
	ReserveCPUs float64
	// CPUCount is `cpu_count`.
	CPUCount int64
	// CPUPercent is `cpu_percent`.
	CPUPercent int64
	// CPUPeriod is `cpu_period`, in microseconds.
	CPUPeriod int64
	// CPUQuota is `cpu_quota`, in microseconds.
	CPUQuota int64
	// CPURTPeriod is `cpu_rt_period`, in microseconds.
	CPURTPeriod int64
	// CPURTRuntime is `cpu_rt_runtime`, in microseconds.
	CPURTRuntime int64
	// CPUShares is `cpu_shares`.
	CPUShares int64
	// MemLimit is `mem_limit`, in bytes.
	MemLimit int64
	// MemReservation is `mem_reservation`, in bytes.
	MemReservation int64
	// MemSwap is `memswap_limit`, in bytes.
	MemSwap int64
	// ShmSize is `shm_size`, in bytes.
	ShmSize int64
	// LimitMemory is `deploy.resources.limits.memory`, in bytes.
	LimitMemory int64
	// ReserveMemory is `deploy.resources.reservations.memory`, in bytes.
	ReserveMemory int64
	// OOMScoreAdj is `oom_score_adj`.
	OOMScoreAdj int64
	// PidsLimit is `pids_limit`.
	PidsLimit int64
	// LimitPids is `deploy.resources.limits.pids`.
	LimitPids int64
	// OOMKillDisable is `oom_kill_disable`.
	OOMKillDisable bool
}

// Spec is the container Stutter creates from one compose service: what the driver passes to
// create, and what Audit compares the created container against.
type Spec struct {
	// Env is the environment passed to the container: compose's, unescaped, without null keys or
	// proxy variables, with the CA variables set on target containers.
	Env map[string]string
	// Sysctls is `sysctls`.
	Sysctls map[string]string
	// raw is compose's environment as parsed — still escaped, null keys absent. Only Audit reads
	// it, so the audit's expectation never comes from the values Spec computed.
	raw map[string]string
	// Service is the compose service name.
	Service string
	// Image is the pinned image ID.
	Image string
	// Platform is `platform`.
	Platform string
	// User is `user`.
	User string
	// WorkingDir is `working_dir`.
	WorkingDir string
	// Hostname is the container's hostname: compose's, else a per-check constant.
	Hostname string
	// Domainname is `domainname`.
	Domainname string
	// MacAddress is `mac_address`.
	MacAddress string
	// IPC is `ipc`.
	IPC string
	// Cgroup is `cgroup`.
	Cgroup string
	// Entrypoint is `entrypoint`, when EntrypointSet.
	Entrypoint []string
	// Cmd is `command`, when CmdSet.
	Cmd []string
	// Unset names each proxy variable the image sets, to be unset in the container.
	Unset []string
	// SecurityOpt is `security_opt`.
	SecurityOpt []string
	// CapAdd is `cap_add`.
	CapAdd []string
	// CapDrop is `cap_drop`.
	CapDrop []string
	// GroupAdd is `group_add`.
	GroupAdd []string
	// Mounts are every mount of the container.
	Mounts []Mount
	// CopyIn are the files copied in before start.
	CopyIn []CopyIn
	// Replaced lists each compose key whose value Stutter replaced.
	Replaced []Replaced
	// ProxyRemoved names each proxy variable removed, with its source.
	ProxyRemoved []Named
	// CAOverridden names each CA variable whose compose or image value was overridden.
	CAOverridden []Named
	// Resources are the resource keys compose honours.
	Resources Resources
	// EntrypointSet reports that compose sets `entrypoint`; otherwise the image's applies.
	EntrypointSet bool
	// CmdSet reports that compose sets `command`; otherwise the image's applies, unless
	// EntrypointSet cleared it.
	CmdSet bool
	// Init is `init`.
	Init bool
	// ReadOnly is `read_only`.
	ReadOnly bool
	// StdinOpen is `stdin_open`.
	StdinOpen bool
	// TTY is `tty`.
	TTY bool
	// target reports a container created from the service under test: the CA is mounted and its
	// variables set.
	target bool
}

// InspectedMount is one entry of a created container's `.Mounts`.
type InspectedMount struct {
	// Type is `.Mounts[].Type`: bind, volume or tmpfs.
	Type string
	// Source is `.Mounts[].Source`.
	Source string
	// Destination is `.Mounts[].Destination`.
	Destination string
	// Name is `.Mounts[].Name`, a volume's name.
	Name string
	// RW is `.Mounts[].RW`.
	RW bool
	// ReadOnlyRecursive is `.HostConfig.Mounts[].BindOptions.ReadOnlyForceRecursive`.
	ReadOnlyRecursive bool
}

// Inspected is what the created-container audit reads from `docker inspect`. The driver decodes it.
type Inspected struct {
	// Labels is `.Config.Labels`.
	Labels map[string]string
	// Image is `.Image`, the image ID.
	Image string
	// User is `.Config.User`.
	User string
	// WorkingDir is `.Config.WorkingDir`.
	WorkingDir string
	// Hostname is `.Config.Hostname`.
	Hostname string
	// Domainname is `.Config.Domainname`.
	Domainname string
	// RestartPolicy is `.HostConfig.RestartPolicy.Name`.
	RestartPolicy string
	// PidMode is `.HostConfig.PidMode`.
	PidMode string
	// IpcMode is `.HostConfig.IpcMode`.
	IpcMode string
	// UTSMode is `.HostConfig.UTSMode`.
	UTSMode string
	// UsernsMode is `.HostConfig.UsernsMode`.
	UsernsMode string
	// CgroupnsMode is `.HostConfig.CgroupnsMode`.
	CgroupnsMode string
	// Runtime is `.HostConfig.Runtime`.
	Runtime string
	// DefaultRuntime is the engine's default runtime, from the engine's identity rather than the
	// container.
	DefaultRuntime string
	// LogDriver is `.HostConfig.LogConfig.Type`.
	LogDriver string
	// Env is `.Config.Env`, as `NAME=value` entries or a bare `NAME`.
	Env []string
	// Entrypoint is `.Config.Entrypoint`.
	Entrypoint []string
	// Cmd is `.Config.Cmd`.
	Cmd []string
	// Healthcheck is `.Config.Healthcheck.Test`.
	Healthcheck []string
	// ExtraHosts is `.HostConfig.ExtraHosts`.
	ExtraHosts []string
	// CapAdd is `.HostConfig.CapAdd`.
	CapAdd []string
	// Devices are `.HostConfig.Devices`, one entry per device.
	Devices []string
	// DeviceCgroupRules is `.HostConfig.DeviceCgroupRules`.
	DeviceCgroupRules []string
	// VolumesFrom is `.HostConfig.VolumesFrom`.
	VolumesFrom []string
	// DNS is `.HostConfig.Dns`.
	DNS []string
	// Networks are the keys of `.NetworkSettings.Networks`.
	Networks []string
	// Mounts is `.Mounts`, each joined with its `.HostConfig.Mounts` bind options.
	Mounts []InspectedMount
	// PortBindings counts the entries of `.HostConfig.PortBindings`.
	PortBindings int
	// DeviceRequests counts the entries of `.HostConfig.DeviceRequests`.
	DeviceRequests int
	// PublishAllPorts is `.HostConfig.PublishAllPorts`.
	PublishAllPorts bool
	// Privileged is `.HostConfig.Privileged`.
	Privileged bool
}

// Role is what a compose service is to the service under test.
type Role string

const (
	// RoleAttached shares the target's network namespace. It is refused.
	RoleAttached Role = "attached"
	// RoleJob is a service some other service waits on to complete successfully.
	RoleJob Role = "job"
	// RoleBus serves NATS; Stutter's own bus stands in for it.
	RoleBus Role = "bus"
	// RoleDatastore serves Postgres.
	RoleDatastore Role = "datastore"
	// RoleSibling is another consumer of the same bus, never started.
	RoleSibling Role = "sibling"
	// RoleOther is any other dependency the target can reach.
	RoleOther Role = "other"
	// RoleUnused is a service the target cannot reach.
	RoleUnused Role = "unused"
)

// Protocol is how Stutter serves one dependency endpoint.
type Protocol string

const (
	// ProtocolPG is the Postgres wire protocol.
	ProtocolPG Protocol = "pg"
	// ProtocolNATS is the NATS client protocol.
	ProtocolNATS Protocol = "nats"
	// ProtocolHTTP is HTTP.
	ProtocolHTTP Protocol = "http"
	// ProtocolOpaque is any other TCP protocol, compared as normalised bytes.
	ProtocolOpaque Protocol = "opaque"
	// ProtocolUDP is a UDP port: listed, never served.
	ProtocolUDP Protocol = "udp"
)

// Endpoint is one port of a dependency, with the protocol Stutter serves it as.
type Endpoint struct {
	// Evidence says what decided the protocol: a key and scheme and port, never a value.
	Evidence string
	// Protocol is how the port is served.
	Protocol Protocol
	// Port is the container port.
	Port uint16
	// TLS reports a reference that encrypts the connection.
	TLS bool
}

// Dependency is one compose service other than the target, classified.
type Dependency struct {
	// Answers are the handshake answers its opaque ports gave, by container port.
	Answers map[uint16]pg.Answer
	// Service is the compose service name.
	Service string
	// Lineage names the image evidence that classified it, if any.
	Lineage string
	// Role is the service's role.
	Role Role
	// Names are every name the target can dial it by: sorted, unique.
	Names []string
	// Evidence lists what decided the role.
	Evidence []string
	// Endpoints are its ports, by port.
	Endpoints []Endpoint
	// Declared reports a role set by `x-stutter`.
	Declared bool
	// Reachable reports that it shares a model network with the target.
	Reachable bool
	// InClosure reports that it is in the target's `depends_on` closure.
	InClosure bool
}

// Shared is one mount the target shares with a started dependency or job.
type Shared struct {
	// Service is the other service.
	Service string
	// Path is the shared host path, or the model volume name when Volume.
	Path string
	// Volume reports a shared model volume rather than a host path.
	Volume bool
}

// Declaration is one `x-stutter` entry applied, with the file that declared it.
type Declaration struct {
	// File is the compose file carrying the declaration.
	File string
	// Service is the service it applies to.
	Service string
	// Key is the declared key path.
	Key string
	// Value is the declared role or protocol.
	Value string
}

// Classification is every compose service classified, with the name sets and disclosures the
// topology and the report need.
type Classification struct {
	// DependencyNames maps each started dependency to the names jobs and other dependencies dial it
	// by.
	DependencyNames map[string][]string
	// target is the service under test, which Started includes.
	target string
	// Deps are every service other than the target, by service name.
	Deps []Dependency
	// SelfAliases are the names the target answers to itself.
	SelfAliases []string
	// BusNames are the names the target dials the bus by.
	BusNames []string
	// SetupBusNames are the names jobs and started dependencies dial the bus by.
	SetupBusNames []string
	// Dangling are names the target references that nothing in the model answers.
	Dangling []string
	// Disclosed are the target's IP-literal, loopback and socket hosts, which bypass DNS.
	Disclosed []Host
	// SetupEgress are hosts jobs and started dependencies reach outside the model.
	SetupEgress []Host
	// Shared are the mounts the target shares with a started service.
	Shared []Shared
	// Declarations are every `x-stutter` entry applied.
	Declarations []Declaration
}

// Profile is one compose profile and the services it gates.
type Profile struct {
	// Name is the profile name.
	Name string
	// Services are the model services carrying it, sorted.
	Services []string
}

// Overrides is the parsed `x-stutter` declaration.
type Overrides struct {
	// Roles maps a service to its declared role.
	Roles map[string]Role
	// Endpoints maps a service and a container port to its declared protocol.
	Endpoints map[string]map[uint16]Protocol
	// File is the compose file that declares it; empty when nothing is declared.
	File string
}

// PullPolicy is how an image is resolved.
type PullPolicy string

const (
	// PullMissing pulls the image only when it is absent locally.
	PullMissing PullPolicy = "missing"
	// PullNever refuses an absent image.
	PullNever PullPolicy = "never"
	// PullBuild builds the image.
	PullBuild PullPolicy = "build"
)

// ImageRef is how one service's image is resolved.
type ImageRef struct {
	// Service is the compose service name.
	Service string
	// Ref is the service's `image`, if any.
	Ref string
	// Platform is the service's `platform`, if any.
	Platform string
	// Policy is the resolution policy.
	Policy PullPolicy
	// Build reports that the service has a `build` key.
	Build bool
}
