package compose

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// errCAEnvironment means a spec was asked for with a CA environment other than Stutter's own.
var errCAEnvironment = errors.New("compose: a spec's CA environment is CAEnvironment() or nil")

// hostLabel is a name usable as a hostname as it stands.
var hostLabel = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

// hostnameDigits is how much of the SHA-256 of a service name an unset hostname keeps: the shape of
// the engine's own default, a container ID prefix.
const hostnameDigits = 12

// Spec builds the container service runs as, on the pinned image img. ca is CAEnvironment() for a
// container created from the service under test and nil for a job; anything else is refused. The
// result depends on nothing but the model, img and ca, so every start of a check gets the same
// spec.
func (m *Model) Spec(service string, img Image, ca map[string]string) (Spec, error) {
	svc, err := m.serviceOf(service)
	if err != nil {
		return Spec{}, err
	}

	if ca != nil && !maps.Equal(ca, CAEnvironment()) {
		return Spec{}, errCAEnvironment
	}

	spec := identity(service, svc, img)
	spec.target = ca != nil

	if svc.Entrypoint != nil {
		spec.Entrypoint, spec.EntrypointSet = unescapeAll(*svc.Entrypoint), true
	}

	if svc.Command != nil {
		spec.Cmd, spec.CmdSet = unescapeAll(*svc.Command), true
	}

	applyEnvironment(&spec, svc, img, ca)

	spec.Replaced = m.replaced(service, svc)

	if err := m.applyMounts(&spec, svc, img); err != nil {
		return Spec{}, err
	}

	var constant bool
	if spec.Hostname, constant = m.hostname(service, svc); constant {
		spec.Replaced = append(spec.Replaced, Replaced{Key: "hostname", What: "a per-check constant"})
	}

	return spec, nil
}

// identity copies what a spec takes from the model as it stands: the process's identity, its
// privileges and its resources.
func identity(service string, svc *composeService, img Image) Spec {
	return Spec{
		Service:     service,
		Image:       img.ID,
		Platform:    svc.Platform,
		User:        svc.User,
		WorkingDir:  unescape(svc.WorkingDir),
		Domainname:  svc.Domainname,
		MacAddress:  svc.MacAddress,
		IPC:         svc.IPC,
		Cgroup:      svc.Cgroup,
		Init:        svc.Init,
		ReadOnly:    svc.ReadOnly,
		StdinOpen:   svc.StdinOpen,
		TTY:         svc.TTY,
		Sysctls:     sysctls(svc),
		SecurityOpt: slices.Clone(svc.SecurityOpt),
		CapAdd:      slices.Clone(svc.CapAdd),
		CapDrop:     slices.Clone(svc.CapDrop),
		GroupAdd:    slices.Clone(svc.GroupAdd),
		Resources:   resources(svc),
	}
}

func sysctls(svc *composeService) map[string]string {
	if svc.Sysctls == nil {
		return nil
	}

	out := make(map[string]string, len(svc.Sysctls))
	for key, value := range svc.Sysctls {
		out[key] = string(value)
	}

	return out
}

// hostname returns the container's hostname, and whether it is Stutter's constant rather than
// compose's. Unset, the engine would use the random container ID, which differs every run and
// reaches HOSTNAME, client names and consumer names; the service name is used instead, unless the
// target can dial some service by that name or names a bus host that way — the hosts file would
// shadow it — in which case a digest of the service name is.
func (m *Model) hostname(service string, svc *composeService) (string, bool) {
	if svc.Hostname != "" {
		return unescape(svc.Hostname), false
	}

	if hostLabel.MatchString(service) && !m.nameTaken(service, svc) {
		return service, true
	}

	sum := sha256.Sum256([]byte(service))

	return hex.EncodeToString(sum[:])[:hostnameDigits], true
}

// nameTaken reports a service name the service can dial something else by.
func (m *Model) nameTaken(service string, svc *composeService) bool {
	for _, other := range m.Services() {
		if other != service && slices.Contains(NamesFrom(m, other, service), service) {
			return true
		}
	}

	return slices.ContainsFunc(serviceReferences(svc), func(found ref) bool {
		return found.Scheme == "nats" && found.Host == service
	})
}

// replacedKeys are the keys whose value Stutter replaces whenever the model sets them, grouped by
// what it does instead.
//
//nolint:gochecknoglobals // a fixed table, not mutable state.
var replacedKeys = []struct {
	what  string
	paths [][]string
}{
	{what: "no restart", paths: [][]string{{"restart"}, {keyDeploy, "restart_policy"}}},
	{what: "healthcheck disabled", paths: [][]string{{"healthcheck"}}},
	{what: "Stutter's log driver", paths: [][]string{{"logging"}}},
	{what: "Stutter's network", paths: [][]string{{keyNetworks}, {keyNetworkMode}}},
	{what: "Stutter's resolver", paths: [][]string{{"dns"}, {"dns_search"}, {"dns_opt"}}},
	{what: "dropped", paths: [][]string{{"extra_hosts"}}},
	{what: "one container", paths: [][]string{{"scale"}, {keyDeploy, "replicas"}, {keyDeploy, "mode"}}},
	{what: "stopped with SIGKILL", paths: [][]string{{"stop_signal"}, {"stop_grace_period"}}},
	{what: pulledWhenMissing, paths: [][]string{{"pull_refresh_after"}}},
}

// pulledWhenMissing is what replaces every pull policy but `never` and `build`.
const pulledWhenMissing = "pulled only when missing"

// replaced lists each key of service whose value Stutter replaces.
func (m *Model) replaced(service string, svc *composeService) []Replaced {
	raw := m.rawService(service)

	var out []Replaced

	for _, group := range replacedKeys {
		for _, path := range group.paths {
			if present(raw, path...) {
				out = append(out, Replaced{Key: strings.Join(path, "."), What: group.what})
			}
		}
	}

	if policy := svc.PullPolicy; policy != "" && policy != "never" && policy != "build" {
		out = append(out, Replaced{Key: "pull_policy", What: pulledWhenMissing})
	}

	return out
}

// resources copies the resource keys compose honours.
func resources(svc *composeService) Resources {
	out := Resources{
		Ulimits:        nil,
		StorageOpt:     maps.Clone(svc.StorageOpt),
		Cpuset:         svc.Cpuset,
		CgroupParent:   svc.CgroupParent,
		CPUs:           float64(svc.CPUs),
		CPUCount:       int64(svc.CPUCount),
		CPUPercent:     int64(svc.CPUPercent),
		CPUPeriod:      int64(svc.CPUPeriod),
		CPUQuota:       int64(svc.CPUQuota),
		CPURTPeriod:    int64(svc.CPURTPeriod),
		CPURTRuntime:   int64(svc.CPURTRuntime),
		CPUShares:      int64(svc.CPUShares),
		MemLimit:       int64(svc.MemLimit),
		MemReservation: int64(svc.MemReservation),
		MemSwap:        int64(svc.MemSwapLimit),
		ShmSize:        int64(svc.ShmSize),
		OOMScoreAdj:    int64(svc.OomScoreAdj),
		PidsLimit:      int64(svc.PidsLimit),
		OOMKillDisable: svc.OomKillDisable,
	}

	if svc.Ulimits != nil {
		out.Ulimits = make(map[string][2]int64, len(svc.Ulimits))
		for name, limit := range svc.Ulimits {
			out.Ulimits[name] = limit
		}
	}

	if svc.MemSwappiness != nil {
		swappiness := int64(*svc.MemSwappiness)
		out.MemSwappiness = &swappiness
	}

	if svc.Deploy != nil {
		limits, reservations := svc.Deploy.Resources.Limits, svc.Deploy.Resources.Reservations
		out.LimitCPUs, out.LimitMemory, out.LimitPids = float64(limits.CPUs), int64(limits.Memory), int64(limits.Pids)
		out.ReserveCPUs, out.ReserveMemory = float64(reservations.CPUs), int64(reservations.Memory)
	}

	out.Blkio = blkio(svc.BlkioConfig)

	return out
}

func blkio(config *serviceBlkio) *Blkio {
	if config == nil {
		return nil
	}

	rates := func(entries []blkioRate) []BlkioRate {
		out := make([]BlkioRate, 0, len(entries))
		for _, entry := range entries {
			out = append(out, BlkioRate{Path: entry.Path, Rate: int64(entry.Rate)})
		}

		return out
	}

	out := &Blkio{
		Weight:          uint16(config.Weight), //nolint:gosec // compose bounds a weight to 10-1000.
		DeviceReadBps:   rates(config.DeviceReadBps),
		DeviceReadIOps:  rates(config.DeviceReadIOps),
		DeviceWriteBps:  rates(config.DeviceWriteBps),
		DeviceWriteIOps: rates(config.DeviceWriteIOps),
	}

	for _, entry := range config.WeightDevice {
		//nolint:gosec // compose bounds a weight to 10-1000.
		out.WeightDevice = append(out.WeightDevice, BlkioWeight{Path: entry.Path, Weight: uint16(entry.Weight)})
	}

	return out
}
