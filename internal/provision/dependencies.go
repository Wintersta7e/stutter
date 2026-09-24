package provision

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// The two paths a started dependency is seeded and restored on.
const (
	// PathPostgres is a datastore: its cluster lives in the container layer and is committed whole.
	PathPostgres = "postgres"
	// PathOther is any other started dependency: its writable paths live on template volumes.
	PathOther = "other"
)

// The outcomes of the dependency steps, each its own exit row. A Postgres endpoint that answers as
// something else at readiness wraps compose.ErrHandshakeContradiction instead: one owner.
var (
	// ErrSeed means a seed, classification or job container could not be created or started, or a seed
	// was not ready within its wait.
	ErrSeed = errors.New("seeding a dependency failed")
	// ErrJob means a discovered job exited non-zero or outlived its wait.
	ErrJob = errors.New("a seed job failed")
	// ErrSnapshot means a snapshot guard refused a seed, its commit failed, or a seed-phase copy failed.
	ErrSnapshot = errors.New("snapshotting a dependency failed")
	// ErrRestore means a dependency could not be restored for a start.
	ErrRestore = errors.New("restoring a dependency failed")
	// ErrDependencyDead means a dependency's restore was not running when a start ended.
	ErrDependencyDead = errors.New("a dependency died during the run")
)

// errDependencyConfig means NewDependencies was given a configuration it cannot provision from.
var errDependencyConfig = errors.New("invalid dependency configuration")

// errOrder means a dependency step was called out of order: an orchestrator's defect, never the user's.
var errOrder = errors.New("dependency step called out of order")

// SeedRecord is how one started dependency was seeded: the report's only account of what every start
// began from. Mounts entries are `<target> <verdict>`, never a source path or a value.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type SeedRecord struct {
	// Service is the compose service.
	Service string
	// Path is PathPostgres or PathOther.
	Path string
	// Jobs are the discovered jobs that ran with this service in their depends_on closure.
	Jobs []string
	// Mounts are every mount dropped, substituted, forced read-only or copied, and a replaced PGDATA.
	Mounts []string
	// Promoted are the ports a handshake promoted to Postgres.
	Promoted []uint16
	// NotListening are the ports that never answered at seed, and are never awaited.
	NotListening []uint16
	// InitScripts reports an entry in the Postgres image's init directory, or a mount at it.
	InitScripts bool
	// Killed reports a seed the engine killed when its grace period ended.
	Killed bool
}

// Waits are the dependency waits. Every value is set by the caller from its one owner; none defaults.
type Waits struct {
	// DependencyReady bounds a seed's readiness, the await sets, and an other-path restore's readiness.
	DependencyReady time.Duration
	// Job bounds each discovered job, from its start.
	Job time.Duration
	// PostgresRestore bounds a Postgres restore's readiness.
	PostgresRestore time.Duration
}

// DependencyConfig is what a check's started dependencies are provisioned from.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type DependencyConfig struct {
	// Engine is the check's engine.
	Engine *Engine
	// Model is the parsed compose project.
	Model *compose.Model
	// Images are the pinned images, by service; every started dependency and discovered job has one.
	Images map[string]compose.Image
	// Network is the dependency network seeds, jobs and restores join.
	Network *Network
	// Helper is the relay image, which the copy helper runs from.
	Helper compose.Image
	// Classification is the final classification, handshake answers applied.
	Classification compose.Classification
	// Waits are the dependency waits.
	Waits Waits
}

// step is how far a check's dependencies have come. Each call requires its predecessor.
type step uint8

const (
	stepNew step = iota
	stepSeeded
	stepJobsRun
	stepSnapshotted
)

// dependency is one started dependency and everything provisioned for it.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type dependency struct {
	// health is the service's healthcheck as the seed runs it.
	health *Healthcheck
	// seed is the seed container, until the snapshot removes it.
	seed *Container
	// restore is the current start's restore container, until the next start replaces it.
	restore *Container
	// templates back plan.templates, one each, in order.
	templates []*Volume
	// perRun are the current restore's volumes, filled from templates.
	perRun []*Volume
	// awaitSet are the ports the seed answered on before its snapshot: every restore waits for them.
	awaitSet []uint16
	// seedStarted is when the seed started, on the monotonic clock.
	seedStarted time.Time
	// snapshot is the committed seed every restore is created from.
	snapshot compose.Image
	image    compose.Image
	dep      compose.Dependency
	record   SeedRecord
	// plan is where the seed's writable paths land.
	plan storagePlan
	// spec is the service's container spec, with only the mounts compose declared.
	spec compose.Spec
	// identity is a Postgres snapshot's system identifier, which its first restore must carry.
	identity uint64
	// healthy reports a service_healthy condition naming the service.
	healthy bool
	// restored reports the snapshot restored once: a Postgres restore's identity is proven the first time.
	restored bool
}

// Dependencies are the dependencies a check starts: each seeded once, snapshotted, and restored before
// every start of the service under test.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Dependencies struct {
	// started are the started dependencies, by service.
	started map[string]*dependency
	// levels are the started services by depends_on level, each level sorted: level 0 depends on no
	// other started service.
	levels [][]string
	// jobs are the discovered jobs, in the order they run.
	jobs []string
	// lastJob is when the last job exited, on the monotonic clock; zero when no job ran.
	lastJob time.Time
	cfg     DependencyConfig
	mu      sync.Mutex
	step    step
}

// NewDependencies derives a check's started dependencies from its final classification.
func NewDependencies(cfg DependencyConfig) (*Dependencies, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}

	started := startedDependencies(cfg.Classification)
	names := make([]string, 0, len(started))
	d := &Dependencies{cfg: cfg, started: make(map[string]*dependency, len(started))}

	for _, dep := range started {
		d.started[dep.Service] = &dependency{
			dep: dep, image: cfg.Images[dep.Service],
			record: SeedRecord{Service: dep.Service, Path: pathOf(dep), Promoted: promoted(dep)},
		}
		names = append(names, dep.Service)
	}

	d.levels = forwardLevels(names, cfg.Model.DependsOn)
	d.jobs = discoverJobs(cfg.Classification.Deps, cfg.Model.DependsOn)

	return d, nil
}

// validate refuses a configuration the dependencies cannot be provisioned from.
func validate(cfg DependencyConfig) error {
	switch {
	case cfg.Engine == nil || cfg.Model == nil || cfg.Network == nil:
		return fmt.Errorf("%w: an engine, a compose model and a dependency network are required", errDependencyConfig)
	case cfg.Waits.DependencyReady <= 0:
		return fmt.Errorf("%w: the DependencyReady wait is not set", errDependencyConfig)
	case cfg.Waits.Job <= 0:
		return fmt.Errorf("%w: the Job wait is not set", errDependencyConfig)
	case cfg.Waits.PostgresRestore <= 0:
		return fmt.Errorf("%w: the PostgresRestore wait is not set", errDependencyConfig)
	case cfg.Helper.ID == "":
		return fmt.Errorf("%w: the copy helper's image is not set", errDependencyConfig)
	default:
		return imagesPinned(cfg)
	}
}

// imagesPinned requires an image for every started dependency and discovered job.
func imagesPinned(cfg DependencyConfig) error {
	for _, dep := range cfg.Classification.Deps {
		if !starts(dep) && (dep.Role != compose.RoleJob || !dep.InClosure) {
			continue
		}

		if cfg.Images[dep.Service].ID == "" {
			return fmt.Errorf("%w: service %s has no pinned image", errDependencyConfig, dep.Service)
		}
	}

	return nil
}

// starts reports a dependency Stutter starts a container of: a datastore or any other dependency.
// Siblings, unused services, the bus, attached services and jobs never start one of their own.
func starts(dep compose.Dependency) bool {
	return dep.Role == compose.RoleDatastore || dep.Role == compose.RoleOther
}

// startedDependencies are the classification's started dependencies, by service name.
func startedDependencies(c compose.Classification) []compose.Dependency {
	var started []compose.Dependency

	for _, dep := range c.Deps {
		if starts(dep) {
			started = append(started, dep)
		}
	}

	slices.SortFunc(started, func(a, b compose.Dependency) int { return cmp.Compare(a.Service, b.Service) })

	return started
}

// pathOf is the path a started dependency is seeded on.
func pathOf(dep compose.Dependency) string {
	if dep.Role == compose.RoleDatastore {
		return PathPostgres
	}

	return PathOther
}

// promoted are the ports a handshake answered as Postgres.
func promoted(dep compose.Dependency) []uint16 {
	var ports []uint16

	for port, answer := range dep.Answers {
		if answer == pg.AnswerPostgres {
			ports = append(ports, port)
		}
	}

	slices.Sort(ports)

	return ports
}

// endpointKeys are the started dependencies' endpoint keys, split by the proxy that serves them: a
// Postgres endpoint by the Postgres proxy, every other TCP endpoint by the opaque one. A UDP endpoint
// is never served. Each list is sorted.
//
//nolint:nonamedreturns // two lists of one type; the names say which is which.
func endpointKeys(started []compose.Dependency) (postgres, opaque []string) {
	for _, dep := range started {
		for _, found := range dep.Endpoints {
			key := harness.EndpointKey(dep.Service, found.Port)

			if found.Protocol == compose.ProtocolPG {
				postgres = append(postgres, key)
			} else if found.Protocol != compose.ProtocolUDP {
				opaque = append(opaque, key)
			}
		}
	}

	slices.Sort(postgres)
	slices.Sort(opaque)

	return slices.Compact(postgres), slices.Compact(opaque)
}

// forwardLevels groups started services by depends_on level: a service's level is one more than the
// deepest started service it depends on. A dependency that does not start places nothing.
func forwardLevels(started []string, dependsOn func(string) map[string]string) [][]string {
	level := map[string]int{}
	isStarted := map[string]bool{}

	for _, name := range started {
		isStarted[name] = true
	}

	var depth func(name string, seen map[string]bool) int

	depth = func(name string, seen map[string]bool) int {
		if found, ok := level[name]; ok {
			return found
		}

		seen[name] = true
		deepest := 0

		for dep := range dependsOn(name) {
			if isStarted[dep] && !seen[dep] {
				deepest = max(deepest, depth(dep, seen)+1)
			}
		}

		delete(seen, name)
		level[name] = deepest

		return deepest
	}

	var levels [][]string

	for _, name := range slices.Sorted(slices.Values(started)) {
		at := depth(name, map[string]bool{})
		for len(levels) <= at {
			levels = append(levels, nil)
		}

		levels[at] = append(levels[at], name)
	}

	return levels
}

// discoverJobs are the seed jobs: every job in the target's depends_on closure, in topological order
// over depends_on among jobs, ties by service name. Nothing else is seed.
func discoverJobs(deps []compose.Dependency, dependsOn func(string) map[string]string) []string {
	waiting := map[string]map[string]bool{}

	for _, dep := range deps {
		if dep.Role == compose.RoleJob && dep.InClosure {
			waiting[dep.Service] = map[string]bool{}
		}
	}

	for job := range waiting {
		for dep := range dependsOn(job) {
			if _, isJob := waiting[dep]; isJob && dep != job {
				waiting[job][dep] = true
			}
		}
	}

	var order []string

	for len(waiting) > 0 {
		next := nextJob(waiting)
		order = append(order, next)
		delete(waiting, next)

		for _, on := range waiting {
			delete(on, next)
		}
	}

	return order
}

// nextJob is the first job by name that waits on no other job. A cycle among jobs leaves none ready:
// compose itself refuses one, so the rest then run by name.
func nextJob(waiting map[string]map[string]bool) string {
	var ready []string

	for job, on := range waiting {
		if len(on) == 0 {
			ready = append(ready, job)
		}
	}

	if len(ready) == 0 {
		ready = slices.Collect(maps.Keys(waiting))
	}

	return slices.Min(ready)
}

// Keys are the endpoint keys of every started dependency, split by the proxy that serves them.
//
//nolint:nonamedreturns // two lists of one type; the names say which is which.
func (d *Dependencies) Keys() (postgres, opaque []string) {
	started := make([]compose.Dependency, 0, len(d.started))
	for _, name := range slices.Sorted(maps.Keys(d.started)) {
		started = append(started, d.started[name].dep)
	}

	return endpointKeys(started)
}

// Discovered are the seed jobs, in the order Jobs runs them. Empty means Jobs is not called.
func (d *Dependencies) Discovered() []string {
	return slices.Clone(d.jobs)
}

// Records are the seed records, one per started dependency, sorted by service.
func (d *Dependencies) Records() []SeedRecord {
	d.mu.Lock()
	defer d.mu.Unlock()

	records := make([]SeedRecord, 0, len(d.started))

	for _, name := range slices.Sorted(maps.Keys(d.started)) {
		record := d.started[name].record
		record.Jobs = slices.Clone(record.Jobs)
		record.Mounts = slices.Clone(record.Mounts)
		record.Promoted = slices.Clone(record.Promoted)
		record.NotListening = slices.Clone(record.NotListening)
		records = append(records, record)
	}

	return records
}

// at requires the dependencies to have reached a step.
func (d *Dependencies) at(call string, want step) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.step != want {
		return fmt.Errorf("%w: %s needs %s first", errOrder, call, stepNames()[want])
	}

	return nil
}

// reach records that the dependencies reached a step.
func (d *Dependencies) reach(to step) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.step = to
}

// stepNames names the call that reaches each step.
func stepNames() map[step]string {
	return map[step]string{
		stepNew: "NewDependencies", stepSeeded: "Seed", stepJobsRun: "Jobs", stepSnapshotted: "Snapshot",
	}
}

// inOrder are the started dependencies, level by level: each level after the ones it depends on.
func (d *Dependencies) inOrder() [][]*dependency {
	out := make([][]*dependency, 0, len(d.levels))

	for _, level := range d.levels {
		deps := make([]*dependency, 0, len(level))
		for _, name := range level {
			deps = append(deps, d.started[name])
		}

		out = append(out, deps)
	}

	return out
}

// eachIn runs fn for every dependency of one level concurrently, and joins what failed.
func eachIn(ctx context.Context, level []*dependency, fn func(context.Context, *dependency) error) error {
	errs := make([]error, len(level))

	var wg sync.WaitGroup

	for index, dep := range level {
		wg.Go(func() { errs[index] = fn(ctx, dep) })
	}

	wg.Wait()

	return errors.Join(errs...)
}
