package provision

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// ClassifyConfig is what the classification containers are started from.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type ClassifyConfig struct {
	// Engine is the check's engine.
	Engine *Engine
	// Model is the parsed compose project.
	Model *compose.Model
	// Images are the pinned images, by service.
	Images map[string]compose.Image
	// Network is the dependency network, which each classification container joins with no alias.
	Network *Network
	// Classification is the classification whose still-opaque endpoints are asked.
	Classification compose.Classification
	// Startup bounds every handshake, from the moment Classify is called: a port nothing serves costs
	// the check this long once, however long its container took to create and start.
	Startup time.Duration
}

// Classify asks every dependency endpoint the static evidence left opaque whether it speaks Postgres,
// on a throwaway container of its service: one that is must be served by the Postgres proxy before any
// listener exists, or its SQL is observed as bytes nobody can read. Every service's container runs
// concurrently, and is removed once each of its endpoints has answered or it has exited.
//
// Only a container the engine fails to create or start is an error; an endpoint that never answers is
// the answer AnswerNone, and one that answers otherwise AnswerOther. The connections are direct, to
// containers this check created, and nothing on them is recorded.
func Classify(ctx context.Context, cfg ClassifyConfig) (map[string]map[uint16]pg.Answer, error) {
	candidates := cfg.Classification.Candidates()
	answers := make(map[string]map[uint16]pg.Answer, len(candidates))
	deadline := time.Now().Add(cfg.Startup)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, service := range slices.Sorted(maps.Keys(candidates)) {
		wg.Go(func() {
			got, err := classifyService(ctx, cfg, deadline, service, candidates[service])

			mu.Lock()
			defer mu.Unlock()

			answers[service] = got

			errs = append(errs, err)
		})
	}

	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return answers, nil
}

// classifyService runs one service's classification container and asks each of its ports.
func classifyService(
	ctx context.Context, cfg ClassifyConfig, deadline time.Time, service string, ports []uint16,
) (map[uint16]pg.Answer, error) {
	img := cfg.Images[service]

	spec, err := composeMounts(cfg.Model, service, img)
	if err != nil {
		return nil, err
	}

	plan, err := classificationPlan(storageInput{spec: spec, imageVolumes: img.Volumes, isDir: isHostDir})
	if err != nil {
		return nil, err
	}

	spec.Mounts = plan.mounts

	c, err := cfg.Engine.CreateContainer(ctx, ContainerSpec{
		Kind: rules.KindVerifier, Service: service, Spec: spec, Publish: ports,
		Networks: []NetworkAttach{{Network: cfg.Network}},
	})
	if err != nil {
		return nil, setupFailure(ErrSeed, "create the classification container of "+service, err)
	}

	answers, err := askOnce(ctx, cfg.Engine, deadline, c, spec, ports)

	return answers, errors.Join(err, cfg.Engine.Remove(context.WithoutCancel(ctx), c))
}

// askOnce starts a classification container and handshakes every port until the deadline. A container
// that exits leaves every port not yet answered AnswerNone.
func askOnce(
	ctx context.Context, eng *Engine, deadline time.Time, c *Container, spec compose.Spec, ports []uint16,
) (map[uint16]pg.Answer, error) {
	if err := copyIn(ctx, eng, c, spec.CopyIn); err != nil {
		return nil, setupFailure(ErrSeed, "copy into the classification container of "+spec.Service, err)
	}

	if err := eng.Start(ctx, c); err != nil {
		return nil, setupFailure(ErrSeed, "start the classification container of "+spec.Service, err)
	}

	asking, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	go func() {
		select {
		case <-eng.Exited(c):
			cancel()
		case <-asking.Done():
		}
	}()

	answers := make(map[uint16]pg.Answer, len(ports))

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)

	for _, port := range ports {
		wg.Go(func() {
			answer := handshakePublished(asking, eng, c, port)

			mu.Lock()
			defer mu.Unlock()

			answers[port] = answer
		})
	}

	wg.Wait()

	return answers, nil
}

// handshakePublished asks one published port the Postgres handshake. A port the engine never
// published is one that cannot answer.
func handshakePublished(ctx context.Context, eng *Engine, c *Container, port uint16) pg.Answer {
	addr, err := eng.Published(ctx, c, port)
	if err != nil {
		return pg.AnswerNone
	}

	answer, err := pg.Handshake(ctx, addr.String())
	if err != nil {
		return pg.AnswerNone
	}

	return answer
}

// composeMounts is a service's container spec carrying only the mounts compose declared: the image's
// VOLUME paths are each path's to place, so none is covered by a volume of its own here.
func composeMounts(model *compose.Model, service string, img compose.Image) (compose.Spec, error) {
	bare := img
	bare.Volumes = nil

	spec, err := model.Spec(service, bare, nil)
	if err != nil {
		return compose.Spec{}, fmt.Errorf("the container spec of %s: %w", service, err)
	}

	return spec, nil
}

// copyIn copies a service's `content:` and `environment:` configs and secrets into its container,
// before it starts, one directory at a time.
func copyIn(ctx context.Context, eng *Engine, c *Container, files []compose.CopyIn) error {
	byDir := map[string][]File{}

	for _, file := range files {
		dir := path.Dir(file.Target)
		byDir[dir] = append(byDir[dir], File{
			Path: path.Base(file.Target), Data: file.Data(), Mode: file.Mode, UID: file.UID, GID: file.GID,
		})
	}

	for _, dir := range slices.Sorted(maps.Keys(byDir)) {
		if err := eng.CopyIn(ctx, c, dir, byDir[dir]); err != nil {
			return err
		}
	}

	return nil
}

// setupFailure names what failed and marks it with its outcome. A refusal of the container itself
// keeps its own outcome: a refused mount is the user's configuration, not the engine failing.
func setupFailure(outcome error, what string, err error) error {
	if errors.Is(err, ErrRefused) {
		return fmt.Errorf("%s: %w", what, err)
	}

	return fmt.Errorf("%w: %s: %w", outcome, what, err)
}
