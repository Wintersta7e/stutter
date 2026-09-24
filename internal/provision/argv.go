package provision

import (
	"bytes"
	"cmp"
	"encoding/csv"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// Pure argv builders: typed arguments that follow a verb's fixed tokens. Nothing here spawns or
// decides; the owner issues the calls.

// labelArgs renders labels as `--label key=value`, in key order so an argv is reproducible.
func labelArgs(labels map[string]string) []arg {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	out := make([]arg, 0, len(keys)+len(keys))
	for _, key := range keys {
		out = append(out, arg{val: "--label"}, arg{val: key + "=" + labels[key]})
	}

	return out
}

// networkCreateArgs is `network create`'s arguments. The subnet is always the caller's, never the
// engine's pick: an engine-allocated pool can land on a local interface's prefix.
//
//nolint:revive // internal is the network's own property, rendered as its flag, not a control flag.
func networkCreateArgs(name string, labels map[string]string, internal bool, subnet netip.Prefix) []arg {
	out := labelArgs(labels)
	if internal {
		out = append(out, arg{val: "--internal"})
	}

	return append(out, arg{val: "--ipv6=false"}, arg{val: "--subnet"}, arg{val: subnet.String()}, arg{val: name})
}

// volumeCreateArgs is `volume create`'s arguments.
func volumeCreateArgs(name string, labels map[string]string) []arg {
	return append(labelArgs(labels), arg{val: name})
}

// createPlan is everything a container create's argv is built from.
type createPlan struct {
	labels map[string]string
	name   string
	image  string
	// covers are the pinned image's VOLUME paths no mount covers: each gets a labelled anonymous
	// volume, so no Stutter container holds an unlabelled one.
	covers []string
	// volumeLabels label every anonymous volume the create makes as this check's.
	volumeLabels []string
	spec         ContainerSpec
}

// createCall is a create's arguments after its fixed tokens, the env-file lines it sends on stdin,
// and the multi-line variables the child's environment carries.
type createCall struct {
	stdin []byte
	args  []arg
	extra []string
}

// createArgs builds a create: every flag, then the pinned image and the command. No environment
// value is ever an argument.
func createArgs(p createPlan) createCall {
	env, stdin, extra := envArgs(p.spec.Spec)

	args := append([]arg{{val: "--name"}, {val: p.name}}, labelArgs(p.labels)...)
	args = append(args, arg{val: "--restart"}, arg{val: "no"}, arg{val: "--log-driver"}, arg{val: "local"})
	args = append(args, networkArgs(p.spec)...)
	args = append(args, env...)
	args = append(args, processArgs(p.spec.Spec)...)
	args = append(args, resourceArgs(p.spec.Spec.Resources)...)
	args = append(args, mountArgs(p)...)
	args = append(args, healthArgs(p.spec.Healthcheck)...)

	process := commandArgs(p.spec.Spec)
	args = append(args, process.entry...)
	args = append(args, arg{val: p.image})

	return createCall{args: append(args, process.command...), stdin: stdin, extra: extra}
}

// networkArgs attaches the container at create — every network, each with its aliases — or to no
// network at all; publishes each port on loopback with an engine-assigned host port.
func networkArgs(spec ContainerSpec) []arg {
	var out []arg

	if spec.NoNetwork {
		out = append(out, arg{val: "--network"}, arg{val: "none"})
	}

	for _, attach := range spec.Networks {
		fields := []string{"name=" + attach.Network.name}
		for _, alias := range attach.Aliases {
			fields = append(fields, "alias="+alias)
		}

		out = append(out, arg{val: "--network"}, arg{val: csvJoin(fields...)})
	}

	if spec.DNS.IsValid() {
		out = append(out, arg{val: "--dns"}, arg{val: spec.DNS.String()})
	}

	for _, port := range spec.Publish {
		out = append(out, arg{val: "-p"}, arg{val: "127.0.0.1::" + strconv.Itoa(int(port)) + "/tcp"})
	}

	return out
}

// envArgs sends the environment as an env file on stdin, never on disk or in argv. A value holding a
// newline or carriage return goes as `-e KEY` with the value in the child's environment; an unset
// name goes as a bare `-e NAME`, absent from that environment — the only form that unsets.
func envArgs(spec compose.Spec) ([]arg, []byte, []string) {
	out := []arg{{val: "--env-file"}, {val: "/dev/stdin"}}

	var (
		stdin bytes.Buffer
		extra []string
	)

	for _, key := range slices.Sorted(maps.Keys(spec.Env)) {
		value := spec.Env[key]
		if strings.ContainsAny(value, "\r\n") {
			out = append(out, arg{val: "-e"}, arg{val: key})
			extra = append(extra, key+"="+value)

			continue
		}

		stdin.WriteString(key + "=" + value + "\n")
	}

	for _, name := range spec.Unset {
		out = append(out, arg{val: "-e"}, arg{val: name})
	}

	return out, stdin.Bytes(), extra
}

// processArgs renders the compose model's process settings. Every value from the model's process
// fields is a model token, logged as a placeholder.
func processArgs(spec compose.Spec) []arg {
	var out []arg

	model := func(flag, value string) {
		if value != "" {
			out = append(out, arg{val: flag}, arg{val: value, model: true})
		}
	}

	plain := func(flag, value string) {
		if value != "" {
			out = append(out, arg{val: flag}, arg{val: value})
		}
	}

	switchOn := func(flag string, on bool) {
		if on {
			out = append(out, arg{val: flag})
		}
	}

	each := func(flag string, values []string) {
		for _, value := range values {
			out = append(out, arg{val: flag}, arg{val: value})
		}
	}

	model("-u", spec.User)
	model("-w", spec.WorkingDir)
	model("--hostname", spec.Hostname)
	model("--domainname", spec.Domainname)
	model("--mac-address", spec.MacAddress)
	plain("--ipc", spec.IPC)
	plain("--cgroupns", spec.Cgroup)
	switchOn("--init", spec.Init)
	switchOn("--read-only", spec.ReadOnly)
	switchOn("-i", spec.StdinOpen)
	switchOn("-t", spec.TTY)
	each("--group-add", spec.GroupAdd)
	each("--security-opt", spec.SecurityOpt)
	each("--cap-add", spec.CapAdd)
	each("--cap-drop", spec.CapDrop)

	for _, key := range slices.Sorted(maps.Keys(spec.Sysctls)) {
		out = append(out, arg{val: "--sysctl"}, arg{val: key + "=" + spec.Sysctls[key], model: true})
	}

	return out
}

// resourceFlag names the create flag that carries a compose.Resources field. "" is a field compose
// itself passes to no container: a CPU reservation is a swarm option.
func resourceFlag(field string) (string, bool) {
	flag, ok := map[string]string{
		"Ulimits": "--ulimit", "StorageOpt": "--storage-opt", "Blkio": "--blkio-weight",
		"MemSwappiness": "--memory-swappiness", "Cpuset": "--cpuset-cpus", "CgroupParent": "--cgroup-parent",
		"CPUs": flagCPUs, "LimitCPUs": flagCPUs, "ReserveCPUs": "", "CPUCount": "--cpu-count",
		"CPUPercent": "--cpu-percent", "CPUPeriod": "--cpu-period", "CPUQuota": "--cpu-quota",
		"CPURTPeriod": "--cpu-rt-period", "CPURTRuntime": "--cpu-rt-runtime", "CPUShares": "--cpu-shares",
		"MemLimit": flagMemory, "MemReservation": flagReservation, "MemSwap": "--memory-swap",
		"ShmSize": "--shm-size", "LimitMemory": flagMemory, "ReserveMemory": flagReservation,
		"OOMScoreAdj": "--oom-score-adj", "PidsLimit": flagPids, "LimitPids": flagPids,
		"OOMKillDisable": "--oom-kill-disable",
	}[field]

	return flag, ok
}

// The flags two resource keys share: a deploy limit or reservation and the plain key.
const (
	flagCPUs        = "--cpus"
	flagMemory      = "--memory"
	flagReservation = "--memory-reservation"
	flagPids        = "--pids-limit"
)

// resourceArgs renders every resource key compose honours. A deploy limit or reservation overrides
// the plain key, as compose applies it after.
func resourceArgs(r compose.Resources) []arg {
	var out []arg

	number := func(flag string, value int64) {
		if value != 0 {
			out = append(out, arg{val: flag}, arg{val: strconv.FormatInt(value, 10)})
		}
	}

	number("--cpu-count", r.CPUCount)
	number("--cpu-percent", r.CPUPercent)
	number("--cpu-period", r.CPUPeriod)
	number("--cpu-quota", r.CPUQuota)
	number("--cpu-rt-period", r.CPURTPeriod)
	number("--cpu-rt-runtime", r.CPURTRuntime)
	number("--cpu-shares", r.CPUShares)
	number(flagMemory, cmp.Or(r.LimitMemory, r.MemLimit))
	number(flagReservation, cmp.Or(r.ReserveMemory, r.MemReservation))
	number("--memory-swap", r.MemSwap)
	number("--shm-size", r.ShmSize)
	number("--oom-score-adj", r.OOMScoreAdj)
	number(flagPids, cmp.Or(r.LimitPids, r.PidsLimit))

	if cpus := cmp.Or(r.LimitCPUs, r.CPUs); cpus != 0 {
		out = append(out, arg{val: flagCPUs}, arg{val: strconv.FormatFloat(cpus, 'f', -1, 64)})
	}

	return append(append(out, resourceExtras(r)...), blkioArgs(r.Blkio)...)
}

// resourceExtras renders the resource keys that are not plain numbers.
func resourceExtras(r compose.Resources) []arg {
	var out []arg

	if r.MemSwappiness != nil {
		out = append(out, arg{val: "--memory-swappiness"}, arg{val: strconv.FormatInt(*r.MemSwappiness, 10)})
	}

	if r.Cpuset != "" {
		out = append(out, arg{val: "--cpuset-cpus"}, arg{val: r.Cpuset})
	}

	if r.CgroupParent != "" {
		out = append(out, arg{val: "--cgroup-parent"}, arg{val: r.CgroupParent})
	}

	if r.OOMKillDisable {
		out = append(out, arg{val: "--oom-kill-disable"})
	}

	for _, name := range slices.Sorted(maps.Keys(r.Ulimits)) {
		limit := r.Ulimits[name]
		out = append(out, arg{val: "--ulimit"}, arg{val: fmt.Sprintf("%s=%d:%d", name, limit[0], limit[1])})
	}

	for _, key := range slices.Sorted(maps.Keys(r.StorageOpt)) {
		out = append(out, arg{val: "--storage-opt"}, arg{val: key + "=" + r.StorageOpt[key]})
	}

	return out
}

// blkioArgs renders `blkio_config`.
func blkioArgs(b *compose.Blkio) []arg {
	if b == nil {
		return nil
	}

	var out []arg

	if b.Weight != 0 {
		out = append(out, arg{val: "--blkio-weight"}, arg{val: strconv.Itoa(int(b.Weight))})
	}

	for _, w := range b.WeightDevice {
		out = append(out, arg{val: "--blkio-weight-device"}, arg{val: w.Path + ":" + strconv.Itoa(int(w.Weight))})
	}

	rates := func(flag string, list []compose.BlkioRate) {
		for _, rate := range list {
			out = append(out, arg{val: flag}, arg{val: rate.Path + ":" + strconv.FormatInt(rate.Rate, 10)})
		}
	}

	rates("--device-read-bps", b.DeviceReadBps)
	rates("--device-read-iops", b.DeviceReadIOps)
	rates("--device-write-bps", b.DeviceWriteBps)
	rates("--device-write-iops", b.DeviceWriteIOps)

	return out
}

// mountArgs renders every mount. A bind is always recursively read-only; a fresh or covering volume
// is anonymous and labelled as this check's; a named volume is one the check created.
func mountArgs(p createPlan) []arg {
	var out []arg

	mount := func(fields ...string) {
		out = append(out, arg{val: "--mount"}, arg{val: csvJoin(fields...)})
	}

	labels := make([]string, 0, len(p.volumeLabels))
	for _, label := range p.volumeLabels {
		labels = append(labels, "volume-label="+label)
	}

	fresh := func(target string, noCopy, readOnly bool) {
		mount(volumeOptions(slices.Concat([]string{"type=volume", "dst=" + target}, labels), noCopy, readOnly)...)
	}

	for _, m := range p.spec.Spec.Mounts {
		switch m.Kind {
		case compose.MountBind:
			mount("type=bind", "src="+m.Source, "dst="+m.Target, "readonly", "bind-propagation=rprivate",
				"bind-recursive=readonly")
		case compose.MountFresh:
			fresh(m.Target, m.NoCopy, m.ReadOnly)
		case compose.MountTmpfs:
			mount(tmpfsFields(m)...)
		default:
			// The gate refused every other kind before the create was built.
		}
	}

	for _, v := range p.spec.Volumes {
		mount(volumeOptions([]string{"type=volume", "src=" + v.Volume.name, "dst=" + v.Target}, v.NoCopy,
			v.ReadOnly)...)
	}

	for _, path := range p.covers {
		fresh(path, false, false)
	}

	return out
}

//nolint:revive // noCopy and readOnly are the mount's own options, rendered as its fields.
func volumeOptions(fields []string, noCopy, readOnly bool) []string {
	if noCopy {
		fields = append(fields, "volume-nocopy")
	}

	if readOnly {
		fields = append(fields, "readonly")
	}

	return fields
}

func tmpfsFields(m compose.Mount) []string {
	fields := []string{"type=tmpfs", "dst=" + m.Target}
	if m.Size != 0 {
		fields = append(fields, "tmpfs-size="+strconv.FormatInt(m.Size, 10))
	}

	if m.Mode != 0 {
		fields = append(fields, "tmpfs-mode="+strconv.FormatUint(uint64(m.Mode.Perm()), 8))
	}

	return fields
}

// healthArgs renders the healthcheck: nil disables the image's, Inherit keeps it. The CLI takes a
// test only as a shell command, so an exec-form test is shell-quoted into one: the same argv.
func healthArgs(h *Healthcheck) []arg {
	if h == nil || len(h.Test) > 0 && h.Test[0] == "NONE" {
		return []arg{{val: "--no-healthcheck"}}
	}

	if h.Inherit {
		return nil
	}

	var out []arg

	switch {
	case len(h.Test) > 1 && h.Test[0] == "CMD":
		out = append(out, arg{val: "--health-cmd"}, arg{val: shellQuote(h.Test[1:]), model: true})
	case len(h.Test) > 1 && h.Test[0] == "CMD-SHELL":
		out = append(out, arg{val: "--health-cmd"}, arg{val: h.Test[1], model: true})
	default:
	}

	duration := func(flag string, d time.Duration) {
		if d > 0 {
			out = append(out, arg{val: flag}, arg{val: d.String()})
		}
	}

	duration("--health-interval", h.Interval)
	duration("--health-timeout", h.Timeout)
	duration("--health-start-period", h.StartPeriod)
	duration("--health-start-interval", h.StartInterval)

	if h.Retries > 0 {
		out = append(out, arg{val: "--health-retries"}, arg{val: strconv.Itoa(h.Retries)})
	}

	return out
}

// shellQuote joins words as POSIX shell single-quoted words.
func shellQuote(words []string) string {
	quoted := make([]string, 0, len(words))
	for _, word := range words {
		quoted = append(quoted, "'"+strings.ReplaceAll(word, "'", `'\''`)+"'")
	}

	return strings.Join(quoted, " ")
}

// processSplit is the process argv split around the image: the --entrypoint flag before it, the
// arguments after it.
type processSplit struct {
	entry   []arg
	command []arg
}

// commandArgs splits the process argv around the image. The CLI cannot set a multi-element
// entrypoint: its first element is --entrypoint and the rest precede the command, the same argv.
func commandArgs(spec compose.Spec) processSplit {
	var out processSplit

	if spec.EntrypointSet {
		first := ""
		if len(spec.Entrypoint) > 0 {
			first = spec.Entrypoint[0]
		}

		out.entry = []arg{{val: "--entrypoint"}, {val: first, model: true}}

		for _, word := range spec.Entrypoint[min(1, len(spec.Entrypoint)):] {
			out.command = append(out.command, arg{val: word, model: true})
		}
	}

	if spec.CmdSet {
		for _, word := range spec.Cmd {
			out.command = append(out.command, arg{val: word, model: true})
		}
	}

	return out
}

// csvJoin joins fields as the engine's --mount and --network parsers read them: CSV, quoting a
// field that holds a comma or a quote.
func csvJoin(fields ...string) string {
	var out strings.Builder

	writer := csv.NewWriter(&out)
	if err := writer.Write(fields); err != nil {
		return strings.Join(fields, ",")
	}

	writer.Flush()

	return strings.TrimSuffix(out.String(), "\n")
}
