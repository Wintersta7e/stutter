package provision

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// Jobs runs the discovered jobs, one at a time in Discovered's order, each only after the one before
// it exited 0: every job it depends on has then exited 0, and every started service it depends on is
// ready from Seed. A job reaches the seeds directly by their compose names; a model volume it shares
// with a started dependency is that dependency's template, so what the job writes is what the
// snapshot holds.
func (d *Dependencies) Jobs(ctx context.Context) error {
	if err := d.at("Jobs", stepSeeded); err != nil {
		return err
	}

	templates := d.templatesByVolume()

	for _, job := range d.jobs {
		if err := d.runJob(ctx, job, templates); err != nil {
			return err
		}

		d.mu.Lock()
		d.lastJob = time.Now()
		d.mu.Unlock()

		d.creditJob(job)
	}

	d.reach(stepJobsRun)

	return nil
}

// templatesByVolume maps each model volume a started dependency holds as a template to that template:
// the first dependency's by name, when two hold one.
func (d *Dependencies) templatesByVolume() map[string]*Volume {
	out := map[string]*Volume{}

	for _, name := range slices.Sorted(maps.Keys(d.started)) {
		dep := d.started[name]

		for index, tmpl := range dep.plan.templates {
			if _, taken := out[tmpl.volume]; tmpl.volume != "" && !taken {
				out[tmpl.volume] = dep.templates[index]
			}
		}
	}

	return out
}

// runJob creates, audits and runs one job, and removes it.
func (d *Dependencies) runJob(ctx context.Context, job string, templates map[string]*Volume) error {
	eng, img := d.cfg.Engine, d.cfg.Images[job]

	spec, err := d.cfg.Model.Spec(job, img, nil)
	if err != nil {
		return fmt.Errorf("%w: job %s: %w", ErrSeed, job, err)
	}

	created := spec
	mounts, volumes := jobVolumes(spec, templates)
	created.Mounts = mounts

	c, err := eng.CreateContainer(ctx, ContainerSpec{
		Kind: rules.KindJob, Service: job, Spec: created, Volumes: volumes,
		Networks: []NetworkAttach{{Network: d.cfg.Network}},
	})
	if err != nil {
		return setupFailure(ErrSeed, "create job "+job, err)
	}

	runErr := d.auditAndRun(ctx, job, c, spec, img)

	return errors.Join(runErr, eng.Remove(context.WithoutCancel(ctx), c))
}

// auditAndRun audits a created job against the spec compose gives it — a shared volume counts as the
// volume it replaces — then starts it and waits for its exit within the job wait.
func (d *Dependencies) auditAndRun(
	ctx context.Context, job string, c *Container, spec compose.Spec, img compose.Image,
) error {
	eng := d.cfg.Engine

	inspected, err := eng.Inspect(ctx, c)
	if err != nil {
		return fmt.Errorf("%w: inspect job %s: %w", ErrSeed, job, err)
	}

	if _, _, auditErr := compose.Audit(spec, img, inspected); auditErr != nil {
		return fmt.Errorf("%w: job %s: %w", ErrSeed, job, auditErr)
	}

	if copyErr := copyIn(ctx, eng, c, spec.CopyIn); copyErr != nil {
		return setupFailure(ErrSeed, "copy into job "+job, copyErr)
	}

	if startErr := eng.Start(ctx, c); startErr != nil {
		return setupFailure(ErrSeed, "start job "+job, startErr)
	}

	timer := time.NewTimer(d.cfg.Waits.Job)
	defer timer.Stop()

	exceeded := false

	select {
	case <-eng.Exited(c):
	case <-timer.C:
		exceeded = true
	case <-ctx.Done():
		return fmt.Errorf("job %s: %w", job, ctx.Err())
	}

	state, err := eng.Stop(ctx, c)
	if err != nil {
		return fmt.Errorf("%w: stop job %s: %w", ErrJob, job, err)
	}

	switch {
	case exceeded:
		return fmt.Errorf("%w: job %s exceeded its wait of %v (log %s)", ErrJob, job, d.cfg.Waits.Job, state.Log)
	case state.ExitCode != 0:
		return fmt.Errorf("%w: job %s exited %d (log %s)", ErrJob, job, state.ExitCode, state.Log)
	default:
		return nil
	}
}

// creditJob names a job in the record of every started service in its depends_on closure.
func (d *Dependencies) creditJob(job string) {
	seen := map[string]bool{job: true}
	queue := []string{job}

	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]

		for dep := range d.cfg.Model.DependsOn(next) {
			if !seen[dep] {
				seen[dep] = true
				queue = append(queue, dep)
			}
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for name, dep := range d.started {
		if seen[name] {
			dep.record.Jobs = append(dep.record.Jobs, job)
		}
	}
}
