package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
)

// errBindChanged means a bind source changed while the check ran: the runs read different input, so
// none of them can be compared with another.
var errBindChanged = errors.New("a bind source changed during the check, so every verdict is withheld")

// runCompose is a whole compose check: the pre-run steps, one consumer check per consumer, the end
// fingerprint walk, the report on stdout, then the teardown and its stderr lines. The report is
// rendered BEFORE the teardown, so a second interrupt during teardown still leaves the verdict.
func runCompose(ctx context.Context, run composeRun, stdout, stderr io.Writer) int {
	out := newLockedWriter(stderr)

	stop, _ := watchInterrupt(ctx, out)
	defer stop()

	c := newComposeCheck(run, out)
	inv := c.check(ctx)

	code := inv.ExitCode()
	for _, phase := range c.finalSteps(&inv, stdout, &code) {
		if err := phase.run(ctx); err != nil {
			out.line("stutter: " + phase.name + ": " + err.Error())
		}
	}

	return code
}

// watchInterrupt prints the interrupt line once, when ctx is first cancelled. printed closes once it
// has been printed; stop ends the watch.
func watchInterrupt(ctx context.Context, out *lockedWriter) (func() bool, <-chan struct{}) {
	printed := make(chan struct{})

	stop := context.AfterFunc(ctx, func() {
		out.line(interruptLine())
		close(printed)
	})

	return stop, printed
}

// check runs everything that decides the verdict: the pre-run steps, the consumer checks and the end
// walk. A failure before the first consumer check is the whole check's.
func (c *composeCheck) check(ctx context.Context) report.Invocation {
	inv := c.decide(ctx)

	if c.engine != nil {
		inv.Scrub = []string{c.engine.CheckID(), c.engine.PrivateDir()}
	}

	return inv
}

// decide runs the steps the verdict rests on, and says what they decided.
func (c *composeCheck) decide(ctx context.Context) report.Invocation {
	inv := report.Invocation{Header: c.header, GatesOnly: c.run.gatesOnly}

	if err := c.prepare(ctx); err != nil {
		inv.Setup = c.wholeCheck(ctx, err)

		return inv
	}

	consumers, unnamed, err := c.consumerChecks(ctx)
	if err != nil {
		inv.Setup = c.wholeCheck(ctx, err)

		return inv
	}

	inv.Consumers, inv.Unnamed = consumers, unnamed

	listeners := c.topology.Listeners()
	counts := listeners.Counts()
	c.header.Counts, c.header.Queries = &counts, listeners.Queries()

	later, err := compose.Fingerprint(context.WithoutCancel(ctx), c.model.BindSources(c.classification.Started()))
	if err != nil {
		inv.Setup = fmt.Errorf("the end fingerprint walk: %w", err)

		return inv
	}

	return endWalk(inv, c.prints.Changed(later))
}

// wholeCheck is a failure before any consumer check could run, named as an interrupt when it was one.
func (*composeCheck) wholeCheck(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%s: %w", interruptedSetup, err)
	}

	return err
}

// endWalk withholds every verdict when the end fingerprint walk found a bind source changed.
func endWalk(inv report.Invocation, changed []string) report.Invocation {
	if len(changed) > 0 {
		inv.Setup = fmt.Errorf("%w: %s", errBindChanged, strings.Join(changed, ", "))
	}

	return inv
}

// finalSteps are the check's last three phases, in order: the report to stdout, the teardown, then the
// stderr lines naming what was kept.
func (c *composeCheck) finalSteps(inv *report.Invocation, stdout io.Writer, code *int) []step {
	var down provision.Teardown

	return []step{
		{name: "render", run: func(context.Context) error {
			if err := inv.Render(stdout); err != nil {
				*code = exitUsage

				return err //nolint:wrapcheck // named with its phase by runCompose.
			}

			return nil
		}},
		{name: "teardown", run: func(ctx context.Context) error {
			keep, _ := retention(ctx, *inv, c.found.Exit.Log)

			var errs []error

			for _, each := range c.teardownSteps(keep, &down) {
				if err := each.run(context.WithoutCancel(ctx)); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", each.name, err))
				}
			}

			return errors.Join(errs...)
		}},
		{name: "stderr", run: func(ctx context.Context) error {
			c.keptLines(ctx, *inv, down)

			return nil
		}},
	}
}

// teardownSteps tear the check down in order: the relays and the verifier, then the listeners they pipe
// to; the bus; then everything the ledger records, which the engine removes — or, under --keep, keeps —
// and whose closing audit goes to stderr.
func (c *composeCheck) teardownSteps(keep provision.Retention, down *provision.Teardown) []step {
	return []step{
		{name: "topology", run: func(ctx context.Context) error {
			if c.topology == nil {
				return nil
			}

			return c.topology.Close(ctx)
		}},
		{name: "bus", run: func(context.Context) error {
			if c.bus != nil {
				c.bus.Close()
			}

			return nil
		}},
		{name: "engine", run: func(ctx context.Context) error {
			if c.engine == nil {
				return nil
			}

			*down = c.engine.Close(ctx, keep)
			if _, err := down.WriteTo(c.stderr); err != nil {
				return errors.Join(err, down.Err)
			}

			return down.Err
		}},
	}
}

// keptLines name, on stderr, every log a rendered outcome points at whenever logs were kept, and the
// check itself under --keep.
func (c *composeCheck) keptLines(ctx context.Context, inv report.Invocation, down provision.Teardown) {
	keep, logs := retention(ctx, inv, c.found.Exit.Log)
	if keep == provision.KeepLogs {
		if len(down.Logs) > 0 {
			logs = append(logs, filepath.Dir(down.Logs[0]))
		}

		for _, path := range logs {
			c.stderr.line(logLine(path))
		}
	}

	if c.run.keep && c.engine != nil {
		c.stderr.line(keepLine(c.engine.CheckID()))
	}
}

// retention is what the check-private directory keeps: its logs whenever a rendered outcome names one,
// the check was interrupted — ctx is the check's own, cancelled by an interrupt — or it ended in a gate
// violation or a setup error; the safe direction is to keep more. It returns the logs the outcomes name.
func retention(ctx context.Context, inv report.Invocation, discoveryLog string) (provision.Retention, []string) {
	var logs []string

	add := func(path string) {
		if path != "" && !slices.Contains(logs, path) {
			logs = append(logs, path)
		}
	}

	// Discovery's start always leaves a log; it is implicated only when discovery stopped the check.
	if inv.Setup != nil {
		add(discoveryLog)
	}

	add(exitLog(inv.Setup))

	setUp := false

	for _, consumer := range append(slices.Clone(inv.Consumers), unnamedOf(inv)...) {
		setUp = setUp || consumer.Bucket() == report.BucketSetup
		add(exitLog(consumer.Report.Setup))

		if consumer.Report.Health != nil {
			add(consumer.Report.Health.Exit.Log)
		}
	}

	if ctx.Err() != nil || setUp || len(logs) > 0 || inv.ExitCode() >= report.ExitGateViolated {
		return provision.KeepLogs, logs
	}

	return provision.DiscardLogs, logs
}

// unnamedOf is the unnamed check, as a list of zero or one.
func unnamedOf(inv report.Invocation) []report.ConsumerCheck {
	if inv.Unnamed == nil {
		return nil
	}

	return []report.ConsumerCheck{*inv.Unnamed}
}

// exitLog is the log of a service whose exit stopped a run, when err carries one.
func exitLog(err error) string {
	if exited, ok := errors.AsType[*replay.ExitError](err); ok {
		return exited.Exit.Log
	}

	return ""
}
