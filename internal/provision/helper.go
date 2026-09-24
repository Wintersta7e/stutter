package provision

import (
	"context"
	"errors"
	"fmt"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/relay"
)

// Where the copy helper sees its two trees, and who it runs as.
const (
	helperSource      = "/src"
	helperDestination = "/dst"
	// rootUID is root. The relay image has no user database, so a user with no group of its own runs
	// in group 0 as well.
	rootUID = "0"
)

// helperCapabilities are all the copy helper keeps: enough to set any owner, mode and time on what it
// writes, and nothing that reaches past its two mounts.
func helperCapabilities() []string {
	return []string{"CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID"}
}

// copySource is where a copy reads from: a user's directory on the host, or a template volume.
type copySource struct {
	volume *Volume
	dir    string
}

// copyTree copies one tree into a fresh volume on a helper container: root inside, so it can keep
// every owner, but with no network, a read-only root, no new privileges and four capabilities. The
// source is mounted read-only — a user's directory is never written — and neither side takes the
// image's content. A copy that fails names the service, the exit code, what the helper said and where
// its log is.
func (d *Dependencies) copyTree(ctx context.Context, service string, from copySource, to *Volume) error {
	eng := d.cfg.Engine

	spec := ContainerSpec{
		Kind: rules.KindHelper, Service: service, NoNetwork: true,
		Volumes: []VolumeMount{{Volume: to, Target: helperDestination, NoCopy: true}},
		Spec: compose.Spec{
			Service: service, Image: d.cfg.Helper.ID, User: rootUID, ReadOnly: true,
			Cmd: relay.Copy{Src: helperSource, Dst: helperDestination}.Args(), CmdSet: true,
			CapDrop: []string{"ALL"}, CapAdd: helperCapabilities(), SecurityOpt: []string{"no-new-privileges"},
		},
	}

	if from.volume != nil {
		spec.Volumes = append(spec.Volumes,
			VolumeMount{Volume: from.volume, Target: helperSource, NoCopy: true, ReadOnly: true})
	} else {
		spec.Spec.Mounts = []compose.Mount{
			{Kind: compose.MountBind, Source: from.dir, Target: helperSource, ReadOnly: true},
		}
	}

	helper, err := eng.CreateContainer(ctx, spec)
	if err != nil {
		return fmt.Errorf("create the copy helper of %s: %w", service, err)
	}

	copyErr := d.runHelper(ctx, service, helper)

	return errors.Join(copyErr, eng.Remove(context.WithoutCancel(ctx), helper))
}

// runHelper starts a created helper and waits for it to exit.
func (d *Dependencies) runHelper(ctx context.Context, service string, helper *Container) error {
	eng := d.cfg.Engine

	if err := eng.Start(ctx, helper); err != nil {
		return fmt.Errorf("start the copy helper of %s: %w", service, err)
	}

	select {
	case <-eng.Exited(helper):
	case <-ctx.Done():
		return fmt.Errorf("the copy helper of %s: %w", service, ctx.Err())
	}

	said, _ := eng.LastStderrLine(ctx, helper) //nolint:errcheck // the exit code decides; the line only explains.

	state, err := eng.Stop(ctx, helper)
	if err != nil {
		return fmt.Errorf("read the copy helper of %s: %w", service, err)
	}

	if state.ExitCode != 0 {
		return fmt.Errorf("%w: the copy for %s exited %d: %s (log %s)", errCopyFailed, service, state.ExitCode, said,
			state.Log)
	}

	return nil
}

// errCopyFailed means a copy helper exited non-zero.
var errCopyFailed = errors.New("copying a tree failed")
