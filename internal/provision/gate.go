package provision

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// containerKinds are the kinds a container can be.
func containerKinds() []rules.Kind {
	return []rules.Kind{
		rules.KindTarget, rules.KindProbe, rules.KindDiscovery, rules.KindRelay, rules.KindJob, rules.KindSeed,
		rules.KindRestore, rules.KindVerifier, rules.KindHelper,
	}
}

// gate refuses, before any call, a container spec Stutter will not create: the same validation the
// compose model ran before the check's first mutation, run again at each create. It returns the
// pinned image.
func (e *Engine) gate(ctx context.Context, spec ContainerSpec) (compose.Image, error) {
	if !slices.Contains(containerKinds(), spec.Kind) {
		return compose.Image{}, fmt.Errorf("%w: %q is not a container kind", ErrRefused, spec.Kind)
	}

	image, pinned := e.pinnedImage(spec.Spec.Image)
	if !pinned {
		return compose.Image{}, fmt.Errorf("%w: image %s is not one this check pinned", ErrRefused, spec.Spec.Image)
	}

	for _, check := range []func() error{
		func() error { return e.gateAttachments(spec) },
		func() error { return e.gateBinds(ctx, spec) },
		func() error { return gateProcess(spec) },
		func() error { return gateEnv(spec.Spec) },
	} {
		if err := check(); err != nil {
			return compose.Image{}, err
		}
	}

	return image, nil
}

// gateAttachments admits only this check's networks and volumes; a container with no network is a
// helper's alone, and every other joins at least one, so none lands on the default bridge.
func (e *Engine) gateAttachments(spec ContainerSpec) error {
	switch {
	case spec.NoNetwork && (spec.Kind != rules.KindHelper || len(spec.Networks) > 0):
		return fmt.Errorf("%w: only a helper runs with no network", ErrRefused)
	case !spec.NoNetwork && len(spec.Networks) == 0:
		return fmt.Errorf("%w: a %s joins at least one of the check's networks", ErrRefused, spec.Kind)
	case slices.ContainsFunc(spec.Networks, func(a NetworkAttach) bool {
		return a.Network == nil || e.owns(a.Network) != nil
	}):
		return fmt.Errorf("%w: a network that is not this check's", ErrRefused)
	case slices.ContainsFunc(spec.Volumes, func(v VolumeMount) bool {
		return v.Volume == nil || e.owns(v.Volume) != nil || !filepath.IsAbs(v.Target)
	}):
		return fmt.Errorf("%w: a volume mount that is not this check's", ErrRefused)
	default:
		return nil
	}
}

// gateBinds validates every bind source: it must exist as a regular file or directory, off a kernel
// pseudo-filesystem, with no socket, device or FIFO anywhere beneath it. A fingerprint is taken
// once per source for the check.
func (e *Engine) gateBinds(ctx context.Context, spec ContainerSpec) error {
	for _, mount := range spec.Spec.Mounts {
		if mount.Kind == compose.MountFresh || mount.Kind == compose.MountTmpfs {
			continue
		}

		if mount.Kind != compose.MountBind {
			return fmt.Errorf("%w: mount kind %q", ErrRefused, mount.Kind)
		}

		prints, err := e.prints(ctx, mount.Source)
		if err == nil {
			err = compose.CheckSource(mount.Source, prints)
		}

		if err != nil {
			return fmt.Errorf("%w: %w", ErrRefused, err)
		}
	}

	return nil
}

// prints returns the fingerprint of a bind source, taken once for the check.
func (e *Engine) prints(ctx context.Context, source string) (compose.Prints, error) {
	e.mu.Lock()
	cached, ok := e.fingerprints[source]
	e.mu.Unlock()

	if ok {
		return cached, nil
	}

	prints, err := compose.Fingerprint(ctx, []string{source})
	if err != nil {
		return compose.Prints{}, fmt.Errorf("fingerprint the bind source: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.fingerprints == nil {
		e.fingerprints = map[string]compose.Prints{}
	}

	e.fingerprints[source] = prints

	return prints, nil
}

// gateProcess refuses a capability that reaches the host or the engine, and a namespace shared
// with the host or another container.
func gateProcess(spec ContainerSpec) error {
	if slices.ContainsFunc(spec.Spec.CapAdd, compose.RefusedCapability) {
		refusal := &compose.Refusal{Service: spec.Service, Key: "cap_add", Class: compose.K2}

		return fmt.Errorf("%w: %w", ErrRefused, refusal)
	}

	if key, shared := compose.SharedNamespace("", spec.Spec.IPC, "", "", spec.Spec.Cgroup); shared {
		return fmt.Errorf("%w: %w", ErrRefused, &compose.Refusal{Service: spec.Service, Key: key, Class: compose.K4})
	}

	return nil
}

// constructedName reports a name the constructed environment carries itself, or one that steers the
// CLI: never a container variable passed through that environment.
func constructedName(name string) bool {
	return name == "PATH" || strings.HasPrefix(name, "DOCKER_")
}

// gateEnv refuses an environment the create cannot pass exactly, naming the key — never the value.
func gateEnv(spec compose.Spec) error {
	for key, value := range spec.Env {
		switch {
		case key == "" || strings.ContainsAny(key, "= \t\r\n\x00") || strings.ContainsRune(value, 0):
			return fmt.Errorf("%w: variable %q cannot be passed", ErrRefused, key)
		case slices.Contains(spec.Unset, key):
			return fmt.Errorf("%w: variable %s is both set and unset", ErrRefused, key)
		case strings.ContainsAny(value, "\r\n") && constructedName(key):
			return fmt.Errorf("%w: multi-line variable %s would travel as the CLI's own", ErrRefused, key)
		}
	}

	unsettable := func(name string) bool {
		return name == "" || strings.ContainsAny(name, "= \t\r\n\x00") || constructedName(name)
	}

	if i := slices.IndexFunc(spec.Unset, unsettable); i >= 0 {
		return fmt.Errorf("%w: variable %q cannot be unset", ErrRefused, spec.Unset[i])
	}

	return nil
}
