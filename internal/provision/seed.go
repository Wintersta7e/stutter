package provision

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// Where a Postgres seed keeps its cluster, and where the image finds its init scripts.
const (
	// pgdataPath lies under no image VOLUME and no compose mount, so the container layer holds the
	// cluster and a commit takes it whole.
	pgdataPath = "/stutter/pgdata"
	// initScriptsDir is where the Postgres image finds the scripts it runs on an empty cluster.
	initScriptsDir = "/docker-entrypoint-initdb.d"
	// healthyCondition is the depends_on condition that waits for a healthcheck.
	healthyCondition = "service_healthy"
	// healthyStatus is the engine's health status of a container whose healthcheck passes.
	healthyStatus = "healthy"
)

// errExited means a dependency's container exited before it was ready.
var errExited = errors.New("the container exited before it was ready")

// Seed starts one seed container per started dependency, level by level of depends_on and in parallel
// within a level, each on its own storage and ready before anything that depends on it starts.
// Classification, seeding and jobs connect directly and are never recorded.
func (d *Dependencies) Seed(ctx context.Context) error {
	if err := d.at("Seed", stepNew); err != nil {
		return err
	}

	if err := d.planSeeds(); err != nil {
		return err
	}

	for _, level := range d.inOrder() {
		if err := eachIn(ctx, level, d.seedOne); err != nil {
			return err
		}
	}

	d.reach(stepSeeded)

	return nil
}

// planSeeds decides every started dependency's storage and healthcheck before anything is created.
func (d *Dependencies) planSeeds() error {
	shared := d.sharedVolumes()

	for _, name := range slices.Sorted(maps.Keys(d.started)) {
		dep := d.started[name]

		spec, err := composeMounts(d.cfg.Model, name, dep.image)
		if err != nil {
			return err
		}

		plan, err := seedPlan(storageInput{
			spec: spec, imageVolumes: dep.image.Volumes, isDir: isHostDir, shared: shared,
		}, dep.record.Path)
		if err != nil {
			return fmt.Errorf("service %s: %w", name, err)
		}

		check, set, err := d.cfg.Model.Healthcheck(name)
		if err != nil {
			return fmt.Errorf("service %s: %w", name, err)
		}

		dep.spec, dep.plan, dep.health = spec, plan, healthcheckOf(check, set)
		dep.healthy = d.conditionOn(name) == healthyCondition
		dep.record.Mounts = slices.Clone(plan.verdicts)
	}

	return nil
}

// sharedVolumes are the model volumes two or more started dependencies mount.
func (d *Dependencies) sharedVolumes() map[string]bool {
	users := map[string]int{}

	for _, name := range slices.Sorted(maps.Keys(d.started)) {
		spec, err := composeMounts(d.cfg.Model, name, d.started[name].image)
		if err != nil {
			continue
		}

		seen := map[string]bool{}

		for _, m := range spec.Mounts {
			if m.Kind == compose.MountFresh && m.Volume != "" && !seen[m.Volume] {
				seen[m.Volume] = true
				users[m.Volume]++
			}
		}
	}

	shared := map[string]bool{}

	for volume, count := range users {
		if count > 1 {
			shared[volume] = true
		}
	}

	return shared
}

// conditionOn is the strongest depends_on condition any service the check starts declares on service:
// service_healthy outranks every other.
func (d *Dependencies) conditionOn(service string) string {
	strongest := ""

	for _, declarer := range d.cfg.Classification.Started() {
		switch condition := d.cfg.Model.DependsOn(declarer)[service]; {
		case condition == healthyCondition:
			return condition
		case condition != "":
			strongest = condition
		default:
		}
	}

	return strongest
}

// seedOne creates a dependency's template volumes, copies its writable directory binds into theirs,
// and starts its seed container, ready within the dependency wait from its start.
func (d *Dependencies) seedOne(ctx context.Context, dep *dependency) error {
	eng, service := d.cfg.Engine, dep.dep.Service

	for _, tmpl := range dep.plan.templates {
		volume, err := eng.CreateVolume(ctx, rules.KindTemplateVolume, service)
		if err != nil {
			return setupFailure(ErrSeed, "create a template volume of "+service, err)
		}

		dep.templates = append(dep.templates, volume)

		if tmpl.copyFrom == "" {
			continue
		}

		if err := d.copyTree(ctx, service, copySource{dir: tmpl.copyFrom}, volume); err != nil {
			return setupFailure(ErrSnapshot, "copy the bind at "+tmpl.target+" of "+service, err)
		}
	}

	seed, err := eng.CreateContainer(ctx, d.seedSpec(dep))
	if err != nil {
		return setupFailure(ErrSeed, "create the seed container of "+service, err)
	}

	dep.seed = seed

	if dep.record.Path == PathPostgres {
		dep.record.InitScripts = d.initScripts(ctx, dep)
	}

	if err := copyIn(ctx, eng, seed, dep.spec.CopyIn); err != nil {
		return setupFailure(ErrSeed, "copy into the seed container of "+service, err)
	}

	if err := eng.Start(ctx, seed); err != nil {
		return setupFailure(ErrSeed, "start the seed container of "+service, err)
	}

	dep.seedStarted = time.Now()

	return d.seedReady(ctx, dep)
}

// seedSpec is a dependency's seed container: the compose service on its planned storage, its
// healthcheck honoured, on the dependency network under every name jobs and other dependencies dial
// it by, every TCP port published.
func (d *Dependencies) seedSpec(dep *dependency) ContainerSpec {
	spec := dep.spec
	spec.Mounts = slices.Clone(dep.plan.mounts)

	if dep.plan.pgdata {
		spec.Env = maps.Clone(spec.Env)
		if spec.Env == nil {
			spec.Env = map[string]string{}
		}

		spec.Env["PGDATA"] = pgdataPath
	}

	return ContainerSpec{
		Kind: rules.KindSeed, Service: dep.dep.Service, Spec: spec, Healthcheck: dep.health,
		Volumes: templateMounts(dep.plan.templates, dep.templates), Publish: tcpPorts(dep.dep),
		Networks: []NetworkAttach{{
			Network: d.cfg.Network, Aliases: slices.Clone(d.cfg.Classification.DependencyNames[dep.dep.Service]),
		}},
	}
}

// templateMounts mounts each planned template at its target.
func templateMounts(planned []templateMount, volumes []*Volume) []VolumeMount {
	mounts := make([]VolumeMount, 0, len(planned))

	for index, tmpl := range planned {
		mounts = append(mounts, VolumeMount{Volume: volumes[index], Target: tmpl.target, NoCopy: tmpl.noCopy})
	}

	return mounts
}

// tcpPorts are a dependency's TCP ports, sorted.
func tcpPorts(dep compose.Dependency) []uint16 {
	var ports []uint16

	for _, found := range dep.Endpoints {
		if found.Protocol != compose.ProtocolUDP {
			ports = append(ports, found.Port)
		}
	}

	slices.Sort(ports)

	return slices.Compact(ports)
}

// pgPorts are a dependency's Postgres ports, sorted.
func pgPorts(dep compose.Dependency) []uint16 {
	var ports []uint16

	for _, found := range dep.Endpoints {
		if found.Protocol == compose.ProtocolPG {
			ports = append(ports, found.Port)
		}
	}

	slices.Sort(ports)

	return ports
}

// initScripts reports whether a Postgres seed will run init scripts: a mount at the init directory, or
// an entry below it in the created container. A copy that fails reads as none.
func (d *Dependencies) initScripts(ctx context.Context, dep *dependency) bool {
	for _, m := range dep.plan.mounts {
		if path.Clean(m.Target) == initScriptsDir {
			return true
		}
	}

	for _, tmpl := range dep.plan.templates {
		if path.Clean(tmpl.target) == initScriptsDir {
			return true
		}
	}

	stream, err := d.cfg.Engine.CopyOut(ctx, dep.seed, initScriptsDir)
	if err != nil {
		return false
	}

	defer func() { _ = stream.Close() }()

	return holdsEntries(stream)
}

// holdsEntries reports a tar of one directory holding at least one entry below its root.
func holdsEntries(stream io.Reader) bool {
	archive := tar.NewReader(stream)

	for {
		header, err := archive.Next()
		if err != nil {
			return false
		}

		if strings.Contains(strings.Trim(path.Clean(header.Name), "/"), "/") {
			return true
		}
	}
}

// seedReady waits for a seed: its condition, and every Postgres port answering the handshake.
func (d *Dependencies) seedReady(ctx context.Context, dep *dependency) error {
	wait := d.cfg.Waits.DependencyReady

	probes, err := d.probes(ctx, dep.seed, pgPorts(dep.dep), nil)
	if err != nil {
		return setupFailure(ErrSeed, "read the published ports of "+dep.dep.Service, err)
	}

	got, err := d.awaitReady(ctx, dep.seed, readyWait{
		deadline: dep.seedStarted.Add(wait), probes: probes, healthy: dep.healthy,
	})

	switch {
	case errors.Is(err, compose.ErrHandshakeContradiction):
		return fmt.Errorf("service %s: %w", dep.dep.Service, err)
	case err != nil:
		return fmt.Errorf("%w: seed %s: %w", ErrSeed, dep.dep.Service, err)
	case !got.ready():
		return fmt.Errorf("%w: seed %s was not ready within %v: %s", ErrSeed, dep.dep.Service, wait, got)
	default:
		return nil
	}
}

// readyWait is one readiness wait: until when, which ports, and whether the healthcheck must pass.
type readyWait struct {
	deadline time.Time
	probes   []portProbe
	healthy  bool
}

// readiness is what a readiness wait found.
type readiness struct {
	ports   awaited
	healthy bool
}

// String says what a dependency still lacked when its wait ended.
func (r readiness) String() string {
	var parts []string

	if !r.healthy {
		parts = append(parts, "its healthcheck never passed")
	}

	for _, port := range r.ports.pending {
		parts = append(parts, fmt.Sprintf("port %d did not answer", port))
	}

	return strings.Join(parts, ", ")
}

// ready reports every port answered and the healthcheck, when asked for, passing.
func (r readiness) ready() bool {
	return r.healthy && len(r.ports.pending) == 0
}

// probes reads where each port is published — only once the container has started, since a start can
// move them — and makes a probe of each: the handshake for a Postgres port, a dial for the rest.
func (d *Dependencies) probes(ctx context.Context, c *Container, postgres, dialled []uint16) ([]portProbe, error) {
	probes := make([]portProbe, 0, len(postgres)+len(dialled))

	for _, group := range []struct {
		ports    []uint16
		postgres bool
	}{{ports: postgres, postgres: true}, {ports: dialled}} {
		for _, port := range group.ports {
			addr, err := d.cfg.Engine.Published(ctx, c, port)
			if err != nil {
				return nil, err
			}

			probes = append(probes, portProbe{addr: addr, port: port, postgres: group.postgres})
		}
	}

	return probes, nil
}

// awaitReady waits, until its deadline, for every probe and, when asked, the container's healthcheck.
// A container that exits meanwhile is not waited for any longer.
func (d *Dependencies) awaitReady(ctx context.Context, c *Container, wait readyWait) (readiness, error) {
	// Ended when the container exits; the deadline is each wait's own, so reaching it leaves the ports
	// still pending rather than failing the wait.
	running, stop := context.WithCancel(ctx)
	defer stop()

	exited := d.cfg.Engine.Exited(c)

	go func() {
		select {
		case <-exited:
			stop()
		case <-running.Done():
		}
	}()

	var (
		wg       sync.WaitGroup
		got      = readiness{healthy: !wait.healthy}
		portsErr error
	)

	wg.Go(func() { got.ports, portsErr = awaitPorts(running, wait.deadline, wait.probes) })

	if wait.healthy {
		wg.Go(func() {
			healthCtx, cancel := context.WithDeadline(running, wait.deadline)
			defer cancel()

			got.healthy = d.waitHealthy(healthCtx, c)
		})
	}

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return readiness{}, fmt.Errorf("wait for readiness: %w", err)
	}

	select {
	case <-exited:
		return readiness{}, errExited
	default:
	}

	return got, portsErr
}

// waitHealthy polls the container's health until it passes or ctx ends.
func (d *Dependencies) waitHealthy(ctx context.Context, c *Container) bool {
	for {
		if status, err := d.cfg.Engine.Health(ctx, c); err == nil && status == healthyStatus {
			return true
		}

		timer := time.NewTimer(awaitPoll)

		select {
		case <-ctx.Done():
			timer.Stop()

			return false
		case <-timer.C:
		}
	}
}
