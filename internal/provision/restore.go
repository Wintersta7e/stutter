package provision

import (
	"archive/tar"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/netip"
	"path"
	"slices"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// errIdentity means a restored cluster is not the one its snapshot recorded.
var errIdentity = errors.New("identity mismatch")

// Restore replaces every started dependency before a start of the service under test, so every start
// begins from the same bytes: the previous restore containers and their volumes are removed —
// dependents first — then each dependency's template volumes are copied into fresh ones and a
// container is created from its snapshot and started, level by level. Nothing is reset by SQL. It
// returns where each endpoint key is published, read from the engine once its container started.
func (d *Dependencies) Restore(ctx context.Context) (map[string]netip.AddrPort, error) {
	if err := d.at("Restore", stepSnapshotted); err != nil {
		return nil, err
	}

	levels := d.inOrder()

	for _, level := range slices.Backward(levels) {
		if err := eachIn(ctx, level, d.removeRestore); err != nil {
			return nil, err
		}
	}

	for _, level := range levels {
		if err := eachIn(ctx, level, d.restoreOne); err != nil {
			return nil, err
		}
	}

	return d.published(ctx)
}

// removeRestore removes a dependency's previous restore container, then its per-run volumes.
func (d *Dependencies) removeRestore(ctx context.Context, dep *dependency) error {
	eng := d.cfg.Engine

	if dep.restore != nil {
		if err := eng.Remove(ctx, dep.restore); err != nil {
			return restoreFailure(dep, "remove the previous restore", err)
		}

		dep.restore = nil
	}

	for _, volume := range dep.perRun {
		if err := eng.Remove(ctx, volume); err != nil {
			return restoreFailure(dep, "remove a previous per-run volume", err)
		}
	}

	dep.perRun = nil

	return nil
}

// restoreOne fills fresh volumes from the templates, creates the restore from the snapshot, proves
// its mounts, starts it and waits for it.
func (d *Dependencies) restoreOne(ctx context.Context, dep *dependency) error {
	eng := d.cfg.Engine
	plan := restorePlan(dep.plan)

	for index := range plan.templates {
		volume, err := eng.CreateVolume(ctx, rules.KindRestore, dep.dep.Service)
		if err != nil {
			return restoreFailure(dep, "create a per-run volume", err)
		}

		dep.perRun = append(dep.perRun, volume)

		err = d.copyTree(ctx, dep.dep.Service, copySource{volume: dep.templates[index]}, volume)
		if err != nil {
			return restoreFailure(dep, "copy "+plan.templates[index].target+" from its template", err)
		}
	}

	c, err := eng.CreateContainer(ctx, d.restoreSpec(dep, plan))
	if err != nil {
		return restoreFailure(dep, "create the restore", err)
	}

	dep.restore = c

	return d.bringUp(ctx, dep, plan)
}

// bringUp proves a created restore's mounts, starts it, waits for it, and proves a Postgres restore's
// identity the first time its snapshot is restored.
func (d *Dependencies) bringUp(ctx context.Context, dep *dependency, plan storagePlan) error {
	eng, c := d.cfg.Engine, dep.restore

	inspected, err := eng.Inspect(ctx, c)
	if err != nil {
		return restoreFailure(dep, "inspect the restore", err)
	}

	if auditErr := mountGuard(inspected.Mounts, plan, dep.perRun, dep.image.Volumes); auditErr != nil {
		return restoreFailure(dep, "audit the restore's mounts", auditErr)
	}

	if copyErr := copyIn(ctx, eng, c, dep.spec.CopyIn); copyErr != nil {
		return restoreFailure(dep, "copy into the restore", copyErr)
	}

	if startErr := eng.Start(ctx, c); startErr != nil {
		return restoreFailure(dep, "start the restore", startErr)
	}

	if err := d.restoreReady(ctx, dep, time.Now()); err != nil {
		return err
	}

	if !dep.plan.pgdata || dep.restored {
		dep.restored = true

		return nil
	}

	if err := d.sameCluster(ctx, dep); err != nil {
		return restoreFailure(dep, "prove its identity", err)
	}

	dep.restored = true

	return nil
}

// restoreSpec is a dependency's restore container: its snapshot, which carries the seed's environment,
// command, user and healthcheck, on the seed's storage again — tmpfs, read-only binds, and a per-run
// volume at every template target, never the image's content — with the seed's aliases and every TCP
// port published. A Postgres restore's healthcheck is off: its readiness is the handshake. Another's
// runs only when a service waits for it to be healthy.
func (d *Dependencies) restoreSpec(dep *dependency, plan storagePlan) ContainerSpec {
	spec := dep.spec
	spec.Image = dep.snapshot.ID
	spec.Env, spec.Unset = nil, nil
	spec.Entrypoint, spec.EntrypointSet, spec.Cmd, spec.CmdSet = nil, false, nil, false
	spec.Mounts = slices.Clone(plan.mounts)

	var health *Healthcheck
	if !plan.pgdata && dep.healthy {
		health = &Healthcheck{Inherit: true}
	}

	return ContainerSpec{
		Kind: rules.KindRestore, Service: dep.dep.Service, Spec: spec, Healthcheck: health,
		Volumes: templateMounts(plan.templates, dep.perRun), Publish: tcpPorts(dep.dep),
		Networks: []NetworkAttach{{
			Network: d.cfg.Network, Aliases: slices.Clone(d.cfg.Classification.DependencyNames[dep.dep.Service]),
		}},
	}
}

// restoreReady waits, from the restore's start, for every Postgres port's handshake, every port of the
// await set, and — on the other path — the condition a service waits on.
func (d *Dependencies) restoreReady(ctx context.Context, dep *dependency, started time.Time) error {
	wait := d.cfg.Waits.DependencyReady
	if dep.plan.pgdata {
		wait = d.cfg.Waits.PostgresRestore
	}

	dialled := slices.DeleteFunc(slices.Clone(dep.awaitSet), func(port uint16) bool {
		return slices.Contains(pgPorts(dep.dep), port)
	})

	probes, err := d.probes(ctx, dep.restore, pgPorts(dep.dep), dialled)
	if err != nil {
		return restoreFailure(dep, "read its published ports", err)
	}

	got, err := d.awaitReady(ctx, dep.restore, readyWait{
		deadline: started.Add(wait), probes: probes, healthy: !dep.plan.pgdata && dep.healthy,
	})

	switch {
	case errors.Is(err, compose.ErrHandshakeContradiction):
		return fmt.Errorf("service %s: %w", dep.dep.Service, err)
	case err != nil:
		return restoreFailure(dep, "wait for it", err)
	case !got.ready():
		return fmt.Errorf("%w: %s was not ready within %v: %s", ErrRestore, dep.dep.Service, wait, got)
	default:
		return nil
	}
}

// sameCluster proves a Postgres restore runs the cluster its snapshot recorded — its pg_control carries
// the recorded system identifier — and that the cluster is running on it.
func (d *Dependencies) sameCluster(ctx context.Context, dep *dependency) error {
	control, err := d.readFile(ctx, dep.restore, path.Join(pgdataPath, pgControlFile))
	if err != nil || len(control) < identifierBytes {
		return fmt.Errorf("%w: no readable %s: %w", errIdentity, pgControlFile, err)
	}

	if got := binary.LittleEndian.Uint64(control[:identifierBytes]); got != dep.identity {
		return fmt.Errorf("%w: the cluster is %#x, its snapshot recorded %#x", errIdentity, got, dep.identity)
	}

	if _, err := d.readFile(ctx, dep.restore, path.Join(pgdataPath, postmasterFile)); err != nil {
		return fmt.Errorf("%w: no %s: the cluster is not running on the snapshot: %w", errIdentity, postmasterFile,
			err)
	}

	return nil
}

// readFile reads one file out of a container through the engine's copy.
func (d *Dependencies) readFile(ctx context.Context, c *Container, name string) ([]byte, error) {
	stream, err := d.cfg.Engine.CopyOut(ctx, c, name)
	if err != nil {
		return nil, err
	}

	defer func() { _ = stream.Close() }()

	archive := tar.NewReader(stream)

	for {
		header, err := archive.Next()
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}

		if header.Typeflag == tar.TypeReg {
			data, err := io.ReadAll(io.LimitReader(archive, readLimit))
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", name, err)
			}

			return data, nil
		}
	}
}

// published reads where every endpoint key is published, from the engine, after every start.
func (d *Dependencies) published(ctx context.Context) (map[string]netip.AddrPort, error) {
	out := map[string]netip.AddrPort{}

	for _, name := range slices.Sorted(maps.Keys(d.started)) {
		dep := d.started[name]

		for _, port := range tcpPorts(dep.dep) {
			addr, err := d.cfg.Engine.Published(ctx, dep.restore, port)
			if err != nil {
				return nil, restoreFailure(dep, "read its published ports", err)
			}

			out[harness.EndpointKey(name, port)] = addr
		}
	}

	return out, nil
}

// restoreFailure names the dependency and the step a restore failed at.
func restoreFailure(dep *dependency, step string, err error) error {
	return fmt.Errorf("%w: %s: %s: %w", ErrRestore, dep.dep.Service, step, err)
}
