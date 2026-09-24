//go:build linux

package enginetest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// interruptContainers is how many containers an interrupted check holds: enough that its teardown is
// still running when the second interrupt lands.
const interruptContainers = 12

// errUnknownMode means a helper was started in a mode it does not have.
var errUnknownMode = errors.New("unknown helper mode")

// mode is what a helper does in the check it opened.
type mode func(ctx context.Context, spec helperSpec, engine *provision.Engine) error

// modes are the helper modes that open a check.
func modes() map[string]mode {
	return map[string]mode{
		"mini": runMini, "lock": runLock, "crash": runCrash, "compose": runCompose, "setup": runSetupExit,
		"keep": runKeep,
	}
}

// runHelper runs one helper mode and returns the process's exit code.
func runHelper(name string) int {
	var err error

	switch run, ok := modes()[name]; {
	case name == "child":
		// Blocks in whatever the first precondition call spawned, until the test kills this process.
		_, err = provision.Preconditions(context.Background())
	case name == "interrupt":
		err = interruptible(func(ctx context.Context) error { return withEngine(ctx, runInterrupt) })
	case ok:
		err = withEngine(context.Background(), run)
	default:
		err = fmt.Errorf("%w %q", errUnknownMode, name)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "helper %s: %v\n", name, err)

		return 1
	}

	return 0
}

// reportf prints one line of a helper's report to the parent test.
func reportf(format string, args ...any) {
	fmt.Printf(format+"\n", args...) //nolint:forbidigo // a helper's stdout is its report to the parent test.
}

// withEngine opens the check the spec file describes, reports its ID and private directory, and
// runs run in it.
func withEngine(ctx context.Context, run mode) error {
	//nolint:gosec // the parent test names the spec file it wrote for this helper.
	data, err := os.ReadFile(os.Getenv(specVar))
	if err != nil {
		return err
	}

	var spec helperSpec
	if err = json.Unmarshal(data, &spec); err != nil {
		return err
	}

	engine, err := provision.Open(ctx, provision.Options{
		StateDir: spec.StateDir, TempDir: spec.TempDir, Keep: spec.Keep,
	})
	if err != nil {
		return err
	}

	reportf("check %s", engine.CheckID())
	reportf("private %s", engine.PrivateDir())

	return run(ctx, spec, engine)
}

// interruptible runs command the way the CLI's main does: the first SIGINT or SIGTERM releases the
// signals, then cancels, so a second one takes the default action and ends the process at once.
func interruptible(command func(context.Context) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-signals:
		case <-ctx.Done():
		}

		signal.Stop(signals)
		cancel()
	}()

	return command(ctx)
}

// helperTarget creates a network and a target container of the spec's image — the suite's own when
// the spec names none — running script under the shell, or the image's own process when script is
// empty.
func helperTarget(
	ctx context.Context, engine *provision.Engine, spec helperSpec, kind rules.Kind, script string,
) (*provision.Network, provision.ContainerSpec, error) {
	ref := spec.Image
	if ref == "" {
		ref = testImage
	}

	image, err := engine.ResolveImage(ctx, ref, "")
	if err != nil {
		return nil, provision.ContainerSpec{}, err
	}

	network, err := pickSubnet(ctx, engine, serviceRole)
	if err != nil {
		return nil, provision.ContainerSpec{}, err
	}

	cs := targetSpec(image, network)
	cs.Kind, cs.Spec.Env, cs.Spec.Unset = kind, spec.Env, spec.Unset

	if script != "" {
		cs = shell(cs, script)
	}

	return network, cs, nil
}

// runMini creates one target container in a kept check, closes, and reports the container.
func runMini(ctx context.Context, spec helperSpec, engine *provision.Engine) error {
	_, cs, err := helperTarget(ctx, engine, spec, rules.KindTarget, "")

	var container *provision.Container
	if err == nil {
		container, err = engine.CreateContainer(ctx, cs)
	}

	down := engine.Close(ctx, provision.KeepLogs)

	if container != nil {
		reportf("container %s", container.ID())
	}

	return errors.Join(err, down.Err)
}

// runLock holds a live check with one container until the parent closes stdin, then closes it.
func runLock(ctx context.Context, spec helperSpec, engine *provision.Engine) error {
	_, cs, err := helperTarget(ctx, engine, spec, rules.KindTarget, "")
	if err != nil {
		return err
	}

	container, err := engine.CreateContainer(ctx, cs)
	if err != nil {
		return err
	}

	reportf("container %s", container.ID())
	reportf("ready")

	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		return err
	}

	return engine.Close(ctx, provision.DiscardLogs).Err
}

// runCrash creates a network, a volume and a container, reports them, and blocks until the parent
// kills it: a check that dies holding its resources.
func runCrash(ctx context.Context, spec helperSpec, engine *provision.Engine) error {
	network, cs, err := helperTarget(ctx, engine, spec, rules.KindTarget, "")
	if err != nil {
		return err
	}

	volume, err := engine.CreateVolume(ctx, rules.KindTemplateVolume, testService)
	if err != nil {
		return err
	}

	container, err := engine.CreateContainer(ctx, cs)
	if err != nil {
		return err
	}

	reportf("network %s", network.ID())
	reportf("volume %s", volume.Name())
	reportf("container %s", container.ID())
	reportf("ready")

	_, err = io.Copy(io.Discard, os.Stdin)

	return err
}

// runInterrupt creates a network and a dozen containers, then waits for the first interrupt and
// tears the check down keeping its logs, as an interrupted check does.
func runInterrupt(ctx context.Context, spec helperSpec, engine *provision.Engine) error {
	_, cs, err := helperTarget(ctx, engine, spec, rules.KindJob, "")
	if err != nil {
		return err
	}

	for range interruptContainers {
		if _, err := engine.CreateContainer(ctx, cs); err != nil {
			return err
		}
	}

	reportf("ready")
	<-ctx.Done()
	reportf("teardown")

	down := engine.Close(ctx, provision.KeepLogs)

	reportf("closed")

	return down.Err
}

// runCompose reads a compose model from a FIFO the parent never writes, so the compose plugin blocks
// until the parent kills this helper.
func runCompose(ctx context.Context, spec helperSpec, engine *provision.Engine) error {
	_, _, err := engine.ComposeConfig(ctx, spec.Dir, []string{"-f", spec.FIFO}, compose.ConfigRead{})

	return err
}

// runSetupExit stages the CA and the bus store, runs a target that exits 3 at once, stops it, and
// closes keeping the logs, as a check that ends in a setup error does.
func runSetupExit(ctx context.Context, spec helperSpec, engine *provision.Engine) error {
	ca, err := engine.HostPath(provision.HostCA)
	if err == nil {
		err = os.WriteFile(ca, []byte("ca"), 0o600)
	}

	store, storeErr := engine.HostPath(provision.HostStore)
	if storeErr == nil {
		storeErr = os.Mkdir(store, 0o700)
	}

	if storeErr == nil {
		storeErr = os.WriteFile(filepath.Join(store, "stream"), []byte("store"), 0o600)
	}

	if err = errors.Join(err, storeErr); err != nil {
		return err
	}

	_, cs, err := helperTarget(ctx, engine, spec, rules.KindTarget, "exit 3")
	if err != nil {
		return err
	}

	container, err := engine.CreateContainer(ctx, cs)
	if err == nil {
		err = engine.Start(ctx, container)
	}

	if err != nil {
		return err
	}

	<-engine.Exited(container)

	if _, err := engine.Stop(ctx, container); err != nil {
		return err
	}

	down := engine.Close(ctx, provision.KeepLogs)

	removed := 0
	for _, counts := range down.Counts {
		removed += counts.Removed
	}

	for _, log := range down.Logs {
		reportf("log %s", log)
	}

	reportf("removed %d", removed)

	return down.Err
}

// runKeep makes a network, a named volume, a running target mounting it and a committed snapshot
// of it in a kept check, closes, and reports each with what the teardown retained.
func runKeep(ctx context.Context, spec helperSpec, engine *provision.Engine) error {
	network, cs, err := helperTarget(ctx, engine, spec, rules.KindTarget, "exec sleep 600")
	if err != nil {
		return err
	}

	volume, err := engine.CreateVolume(ctx, rules.KindTemplateVolume, testService)
	if err != nil {
		return err
	}

	cs.Volumes = []provision.VolumeMount{{Volume: volume, Target: "/data"}}

	container, err := engine.CreateContainer(ctx, cs)
	if err == nil {
		err = engine.Start(ctx, container)
	}

	if err != nil {
		return err
	}

	snapshot, err := engine.Commit(ctx, container, rules.KindSnapshot)
	if err != nil {
		return err
	}

	down := engine.Close(ctx, provision.KeepLogs)

	reportf("network %s", network.ID())
	reportf("volume %s", volume.Name())
	reportf("container %s", container.ID())
	reportf("image %s", snapshot.ID)

	for _, retained := range down.Retained {
		reportf("retained %s %s", retained.Type, retained.ID)
	}

	reportf("done")

	return down.Err
}
