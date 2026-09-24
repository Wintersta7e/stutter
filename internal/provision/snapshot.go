package provision

import (
	"context"
	"fmt"
	"slices"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// Snapshot observes which ports every seed answers on, stops every seed gracefully — dependents before
// what they depend on — proves each stopped seed clean, commits it, and removes it; its template
// volumes are kept for the restores.
func (d *Dependencies) Snapshot(ctx context.Context) error {
	if err := d.readyForSnapshot(); err != nil {
		return err
	}

	if err := d.observeAwaitSets(ctx); err != nil {
		return err
	}

	levels := d.inOrder()

	for _, level := range slices.Backward(levels) {
		if err := eachIn(ctx, level, d.stopSeed); err != nil {
			return err
		}
	}

	for _, level := range levels {
		if err := eachIn(ctx, level, d.commitSeed); err != nil {
			return err
		}
	}

	d.reach(stepSnapshotted)

	return nil
}

// readyForSnapshot requires every seed started and every discovered job run.
func (d *Dependencies) readyForSnapshot() error {
	if len(d.jobs) == 0 {
		if err := d.at("Snapshot", stepSeeded); err == nil {
			return nil
		}
	}

	return d.at("Snapshot", stepJobsRun)
}

// observeAwaitSets polls every relayed port of every seed until it answers or the dependency wait ends,
// counted from the last job's exit — or the last seed's start when no job ran — since a service may
// only listen once a job has run. The ports that answered are each seed's await set; one that never
// did is recorded as not listening at seed and is never waited for later: images declare ports they do
// not serve.
func (d *Dependencies) observeAwaitSets(ctx context.Context) error {
	d.mu.Lock()
	from := d.lastJob
	d.mu.Unlock()

	if from.IsZero() {
		for _, dep := range d.started {
			if dep.seedStarted.After(from) {
				from = dep.seedStarted
			}
		}
	}

	deadline := from.Add(d.cfg.Waits.DependencyReady)

	var all []*dependency
	for _, level := range d.inOrder() {
		all = append(all, level...)
	}

	return eachIn(ctx, all, func(ctx context.Context, dep *dependency) error {
		probes, err := d.probes(ctx, dep.seed, pgPorts(dep.dep), otherPorts(dep.dep))
		if err != nil {
			return fmt.Errorf("%w: read the published ports of %s: %w", ErrSnapshot, dep.dep.Service, err)
		}

		got, err := awaitPorts(ctx, deadline, probes)
		if err != nil {
			return fmt.Errorf("service %s: %w", dep.dep.Service, err)
		}

		d.mu.Lock()
		defer d.mu.Unlock()

		dep.awaitSet, dep.record.NotListening = got.ready, got.pending

		return nil
	})
}

// otherPorts are a dependency's TCP ports that do not speak Postgres, sorted.
func otherPorts(dep compose.Dependency) []uint16 {
	return slices.DeleteFunc(tcpPorts(dep), func(port uint16) bool { return slices.Contains(pgPorts(dep), port) })
}

// stopSeed stops a seed with compose's stop signal and grace period, so what it holds is flushed. A
// seed the engine killed when the grace ended is recorded: its unflushed state is absent from every
// start alike, so the comparison stays sound.
func (d *Dependencies) stopSeed(ctx context.Context, dep *dependency) error {
	stop, err := d.cfg.Model.Stop(dep.dep.Service)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrSnapshot, dep.dep.Service, err)
	}

	graceful := Graceful{Grace: stop.Grace, HasGrace: stop.GraceSet}
	if stop.SignalSet {
		graceful.Signal = stop.Signal
	}

	state, err := d.cfg.Engine.StopGracefully(ctx, dep.seed, graceful)
	if err != nil {
		return fmt.Errorf("%w: stop the seed of %s: %w", ErrSnapshot, dep.dep.Service, err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	dep.record.Killed = state.Killed

	return nil
}

// commitSeed proves a stopped seed clean, commits it as the dependency's snapshot, and removes it.
func (d *Dependencies) commitSeed(ctx context.Context, dep *dependency) error {
	eng, service := d.cfg.Engine, dep.dep.Service

	if err := d.guard(ctx, dep); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrSnapshot, service, err)
	}

	snapshot, err := eng.Commit(ctx, dep.seed, rules.KindSnapshot)
	if err != nil {
		return fmt.Errorf("%w: commit the seed of %s: %w", ErrSnapshot, service, err)
	}

	if err := eng.Remove(ctx, dep.seed); err != nil {
		return fmt.Errorf("%w: remove the seed of %s: %w", ErrSnapshot, service, err)
	}

	dep.snapshot, dep.seed = snapshot, nil

	return nil
}

// guard runs the stopped seed's proofs: its mounts, and on the Postgres path its data directory.
func (d *Dependencies) guard(ctx context.Context, dep *dependency) error {
	eng := d.cfg.Engine

	inspected, err := eng.Inspect(ctx, dep.seed)
	if err != nil {
		return err
	}

	if guardErr := mountGuard(inspected.Mounts, dep.plan, dep.templates, dep.image.Volumes); guardErr != nil {
		return guardErr
	}

	if !dep.plan.pgdata {
		return nil
	}

	stream, err := eng.CopyOut(ctx, dep.seed, pgdataPath)
	if err != nil {
		// Absence and a failed copy are not told apart; both fail.
		return fmt.Errorf("%w: no %s could be read: the data directory is unreadable (absent, or the copy failed): %w",
			errGuard, pgVersionFile, err)
	}

	defer func() { _ = stream.Close() }()

	dir, err := readDataDirectory(stream)
	if err != nil {
		return err
	}

	dep.identity, err = pgGuard(dir)

	return err
}
