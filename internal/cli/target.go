package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/topology"
)

// step is one named step of an ordered sequence. The names make the order data a test can compare.
type step struct {
	run  func(context.Context) error
	name string
}

// target starts the service under test in a container of its own, once per start: the compose path's
// harness Start. It assembles the compose model's spec, the engine and the topology's placement, and
// nothing else places a target.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type target struct {
	engine   *provision.Engine
	topology *topology.Topology
	deps     *provision.Dependencies
	model    *compose.Model
	// service is the compose service under test.
	service string
	// ca is the host path of the check's CA file, bound read-only into every target.
	ca    string
	image compose.Image
}

// started is what one start has made so far.
type started struct {
	container *provision.Container
	spec      compose.Spec
}

// startAs is the harness Start for one kind of start: the probe start, discovery, or a run's target.
// A start that fails after its container was created removes it, so the next start can take its name.
func (t *target) startAs(kind rules.Kind) harness.Start {
	return func(ctx context.Context, _ harness.Addresses) (harness.Consumer, error) {
		var made started

		for _, each := range t.startSteps(kind, &made) {
			err := each.run(ctx)
			if err == nil {
				continue
			}

			err = fmt.Errorf("start the %s: %s: %w", kind, each.name, err)
			if made.container != nil {
				err = errors.Join(err, t.engine.Remove(ctx, made.container))
			}

			return nil, err
		}

		return &targetConsumer{target: t, container: made.container, exited: t.engine.Exited(made.container)}, nil
	}
}

// startSteps are one start, in order: the relays are live before anything is placed; the container
// is created from the spec on the topology's network, resolver and hostname, given its files, and
// audited against the spec before it runs; it joins its network only once started, so its
// membership is checked after the start.
func (t *target) startSteps(kind rules.Kind, made *started) []step {
	return []step{
		{name: "live", run: t.live},
		{name: "spec", run: func(context.Context) error {
			var err error

			made.spec, err = t.spec()

			return err
		}},
		{name: "create", run: func(ctx context.Context) error {
			var err error

			made.container, err = t.engine.CreateContainer(ctx, t.containerSpec(kind, made.spec))

			return err //nolint:wrapcheck // named with its step by startAs.
		}},
		{name: "copy-in", run: func(ctx context.Context) error {
			err := t.engine.CopyInConfigs(ctx, made.container, made.spec.CopyIn)

			return err //nolint:wrapcheck // named with its step by startAs.
		}},
		{name: "audit", run: func(ctx context.Context) error { return t.audit(ctx, made) }},
		{name: "start", run: func(ctx context.Context) error {
			return t.engine.Start(ctx, made.container)
		}},
		{name: "check-target", run: func(ctx context.Context) error {
			return t.topology.CheckTarget(ctx, made.container)
		}},
	}
}

func (t *target) live(ctx context.Context) error {
	return t.topology.Live(ctx) //nolint:wrapcheck // named with its step by startAs.
}

// spec is the container the service under test runs as: the model's spec with the CA variables set,
// and the check's CA file bound read-only where they point.
func (t *target) spec() (compose.Spec, error) {
	spec, err := t.model.Spec(t.service, t.image, compose.CAEnvironment())
	if err != nil {
		return compose.Spec{}, fmt.Errorf("the container spec of %s: %w", t.service, err)
	}

	spec.Mounts = append(slices.Clone(spec.Mounts), compose.Mount{
		Kind: compose.MountBind, Source: t.ca, Target: compose.CAMountPath, ReadOnly: true,
	})

	return spec, nil
}

// containerSpec places the spec where the topology says every target goes.
func (t *target) containerSpec(kind rules.Kind, spec compose.Spec) provision.ContainerSpec {
	place := t.topology.Placement()
	spec.Hostname = place.Hostname

	return provision.ContainerSpec{
		Kind:     kind,
		Service:  t.service,
		Spec:     spec,
		Networks: []provision.NetworkAttach{place.Attach},
		DNS:      place.DNS,
	}
}

// audit compares the created container with the spec it was created from.
func (t *target) audit(ctx context.Context, made *started) error {
	got, err := t.engine.Inspect(ctx, made.container)
	if err != nil {
		return err //nolint:wrapcheck // named with its step by startAs.
	}

	if _, _, err := compose.Audit(made.spec, t.image, got); err != nil {
		return err //nolint:wrapcheck // named with its step by startAs.
	}

	return nil
}

// targetConsumer is one start's target container, as the harness drives it.
type targetConsumer struct {
	target    *target
	container *provision.Container
	exited    <-chan struct{}
}

var _ harness.Consumer = (*targetConsumer)(nil)

// Exited closes when the container stops by itself.
func (c *targetConsumer) Exited() <-chan struct{} {
	return c.exited
}

// Close retires the container — stopped, its state read before the kill and its log kept; removed,
// or under --keep held stopped until the next start of its name — and then reads whether the relays
// and the dependencies outlived the run. Anything that died beside the service joins the error.
func (c *targetConsumer) Close(ctx context.Context) (replay.Exit, error) {
	byItself := false

	select {
	case <-c.exited:
		byItself = true
	default:
	}

	state, err := c.target.engine.Retire(ctx, c.container)
	errs := []error{err, c.target.topology.Live(ctx)}

	if c.target.deps != nil {
		errs = append(errs, c.target.deps.Alive(ctx))
	}

	return exitFrom(state, byItself), errors.Join(errs...)
}

// exitFrom is how the service ended, from the container's state read before anything stopped it.
// Exited is only a stop the service made by itself; the message it followed is the harness's to fill,
// from its own clock.
func exitFrom(state provision.State, byItself bool) replay.Exit {
	return replay.Exit{
		Log:       state.Log,
		Code:      state.ExitCode,
		Restarts:  state.RestartCount,
		OOMKilled: state.OOMKilled,
		Exited:    byItself,
	}
}
