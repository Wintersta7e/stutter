package provision

import (
	"context"
	"fmt"
	"maps"
	"slices"
)

// Alive reads every current restore container's state, without touching it: one that is not running
// ended the run's dependency out from under it, so the run must not be compared — a dead dependency
// makes a handler look like it did less. The dead one's state and log are kept, and it is named with
// its exit code.
func (d *Dependencies) Alive(ctx context.Context) error {
	if err := d.at("Alive", stepSnapshotted); err != nil {
		return err
	}

	eng := d.cfg.Engine

	for _, name := range slices.Sorted(maps.Keys(d.started)) {
		dep := d.started[name]
		if dep.restore == nil {
			return fmt.Errorf("%w: %s has no restore: Restore runs before every start", errOrder, name)
		}

		state, err := eng.Status(ctx, dep.restore)
		if err != nil {
			return fmt.Errorf("%w: read the restore of %s: %w", ErrDependencyDead, name, err)
		}

		if state.Running {
			continue
		}

		stopped, err := eng.Stop(ctx, dep.restore)
		if err != nil {
			return fmt.Errorf("%w: %s stopped with exit code %d (OOM-killed %v); its log was not kept: %w",
				ErrDependencyDead, name, state.ExitCode, state.OOMKilled, err)
		}

		return fmt.Errorf("%w: %s stopped with exit code %d (OOM-killed %v); log %s", ErrDependencyDead, name,
			stopped.ExitCode, stopped.OOMKilled, stopped.Log)
	}

	return nil
}
