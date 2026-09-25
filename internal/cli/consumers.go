package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
	"github.com/Wintersta7e/stutter/internal/report"
)

// errConsumerNotDiscovered means --consumer named a consumer discovery did not find.
var errConsumerNotDiscovered = errors.New("--consumer names a consumer that was not discovered")

// sandbox is one consumer check's session, and what the report reads from it once the check ends.
type sandbox interface {
	check.Session
	Tally() []httpproxy.HostTally
	Timings() harness.Timings
	Holds() []harness.FillHold
}

var _ sandbox = (*harness.Sandbox)(nil)

// consumerPlan is one discovered consumer's place in the check, decided before any consumer check
// runs.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type consumerPlan struct {
	name    string
	reason  string
	outcome report.Outcome
	// admitted are the ranks of the corpus messages its filter subjects admit, in corpus order.
	admitted []uint64
	found    harness.Found
}

// planConsumers decides every discovered consumer's place: named by --consumer or not, on the corpus
// stream or elsewhere, stable-named, a possible target, admitting any corpus message — in name order.
// A --consumer name nothing discovered is refused, naming every consumer that was.
func planConsumers(found harness.Discovery, messages []corpus.Message, selected []string) ([]consumerPlan, error) {
	names := make([]string, 0, len(found.Consumers)+len(found.Elsewhere))
	for _, consumer := range found.Consumers {
		names = append(names, consumer.Name)
	}

	names = append(names, found.Elsewhere...)
	slices.Sort(names)

	for _, name := range selected {
		if !slices.Contains(names, name) {
			return nil, fmt.Errorf("%w: %s; discovered: %s", errConsumerNotDiscovered, name, strings.Join(names, ", "))
		}
	}

	plans := make([]consumerPlan, 0, len(names))

	for _, consumer := range found.Consumers {
		plans = append(plans, planOne(consumer, messages, selected))
	}

	for _, elsewhere := range found.Elsewhere {
		plan := consumerPlan{name: elsewhere, outcome: report.OutcomeNotCovered, reason: elsewhereReason}
		if unselected(selected, elsewhere) {
			plan.outcome, plan.reason = report.OutcomeNotSelected, notSelectedReason
		}

		plans = append(plans, plan)
	}

	slices.SortFunc(plans, func(a, b consumerPlan) int { return strings.Compare(a.name, b.name) })

	return plans, nil
}

// planOne places one consumer on the corpus stream. Its admitted messages come from the one subject
// matcher the bus uses; a consumer admitting none is never run.
func planOne(found harness.Found, messages []corpus.Message, selected []string) consumerPlan {
	plan := consumerPlan{found: found, name: found.Name, outcome: report.OutcomeChecked}

	for _, message := range corpus.Admitted(messages, found.Policy.FilterSubjects) {
		plan.admitted = append(plan.admitted, message.Seq)
	}

	switch {
	case unselected(selected, found.Name):
		plan.outcome, plan.reason = report.OutcomeNotSelected, notSelectedReason
	case found.Unstable:
		plan.outcome, plan.reason = report.OutcomeNotCovered, unstableReason
	case found.Excluded != "":
		plan.outcome, plan.reason = report.OutcomeSetup, found.Excluded
	case len(plan.admitted) == 0:
		plan.outcome, plan.reason = report.OutcomeNotCovered, nothingAdmittedReason
	default:
	}

	return plan
}

// unselected reports a consumer --consumer left out: with no --consumer, every consumer is selected.
func unselected(selected []string, name string) bool {
	return len(selected) > 0 && !slices.Contains(selected, name)
}

// consumerChecks runs one consumer check per selected, covered consumer, serially in name order, each
// with its own sandbox and its own budget. Every sandbox is built before the first check, so a drain one
// consumer cannot use stops the check before anything runs. A service with no consumer at all gets the
// one unnamed check instead.
func (c *composeCheck) consumerChecks(ctx context.Context) ([]report.ConsumerCheck, *report.ConsumerCheck, error) {
	if len(c.found.Consumers)+len(c.found.Elsewhere) == 0 {
		unnamed, err := c.unnamedCheck(ctx)

		return nil, unnamed, err
	}

	plans, err := planConsumers(c.found, c.loaded.Messages, c.run.consumers)
	if err != nil {
		return nil, nil, err
	}

	sandboxes, err := c.sandboxes(plans)
	if err != nil {
		return nil, nil, err
	}

	checks := make([]report.ConsumerCheck, 0, len(plans))
	for index, plan := range plans {
		checks = append(checks, c.checkOne(ctx, plan, sandboxes[index]))
	}

	c.summarise(sandboxes)

	return checks, nil, nil
}

// unnamedCheck is the check a service with no consumer gets: every corpus message, no consumer named,
// no fault licensed. Its reason names what the bus refused during discovery.
func (c *composeCheck) unnamedCheck(ctx context.Context) (*report.ConsumerCheck, error) {
	plan := consumerPlan{outcome: report.OutcomeChecked}
	for _, message := range c.loaded.Messages {
		plan.admitted = append(plan.admitted, message.Seq)
	}

	built, err := c.newSandbox(plan)
	if err != nil {
		return nil, err
	}

	refusals := make([]string, 0, len(c.found.Refusals))
	for _, refusal := range c.found.Refusals {
		refusals = append(refusals, refusal.Subject+" ("+strconv.Itoa(refusal.ErrCode)+": "+refusal.Description+")")
	}

	checked := c.checkOne(ctx, plan, built)
	checked.Reason = unnamedReason(refusals, c.found.ClosedAfterInfo)
	c.summarise([]sandbox{built})

	return &checked, nil
}

// sandboxes builds every checked consumer's sandbox, in plan order; any refusal stops the check whole,
// naming the consumer.
func (c *composeCheck) sandboxes(plans []consumerPlan) ([]sandbox, error) {
	built := make([]sandbox, len(plans))

	for index, plan := range plans {
		if plan.outcome != report.OutcomeChecked {
			continue
		}

		made, err := c.newSandbox(plan)
		if err != nil {
			return nil, fmt.Errorf("consumer %s: %w", plan.name, err)
		}

		built[index] = made
	}

	return built, nil
}

// buildSandbox is a consumer check's sandbox, on the check's listeners and restored from B1.
//
//nolint:ireturn // the seam every consumer check's sandbox is built through.
func (c *composeCheck) buildSandbox(plan consumerPlan) (sandbox, error) {
	made, err := harness.New(sandboxConfig(c.startConfig(rules.KindTarget, &c.b1), plan, c.run, c.loaded.Messages))
	if err != nil {
		return nil, err //nolint:wrapcheck // named with its consumer by sandboxes.
	}

	return made, nil
}

// sandboxConfig is one consumer check's harness configuration: the check's shared start configuration,
// the corpus as loaded — never what a stream holds after another check — the consumer named, and its own
// discovered configuration, which is also what licenses its faults.
func sandboxConfig(base harness.Config, plan consumerPlan, run composeRun, recorded []corpus.Message) harness.Config {
	base.Recorded = recorded
	base.Consumer = plan.name
	base.Policy = plan.found.Policy
	base.Drain = run.drain
	base.HTTPDefault, base.HTTPRoutes = run.httpDefault, run.httpRoutes

	return base
}

// checkOptions is one consumer check: its admitted messages, its own configuration, its own budget,
// and the corpus file each message came from.
func (c *composeCheck) checkOptions(plan consumerPlan) check.Options {
	files := make(map[uint64]string, len(c.loaded.Messages))
	for index, message := range c.loaded.Messages {
		files[message.Seq] = c.loaded.Files[index]
	}

	return check.Options{
		Messages: plan.admitted, Consumer: plan.name, Config: plan.found.Policy, MaxRuns: c.run.maxRuns,
		GatesOnly: c.run.gatesOnly, Files: files,
	}
}

// checkOne runs one consumer's check, or says why it was not run. A check reached after an interrupt,
// and one the interrupt cut short, end in setup; one that completed keeps its verdict.
func (c *composeCheck) checkOne(ctx context.Context, plan consumerPlan, built sandbox) report.ConsumerCheck {
	checked := report.ConsumerCheck{
		Name: plan.name, Reason: plan.reason, Outcome: plan.outcome, Admitted: len(plan.admitted),
	}

	if plan.outcome != report.OutcomeChecked {
		return checked
	}

	if ctx.Err() != nil {
		checked.Outcome, checked.Reason = report.OutcomeSetup, interruptedBefore

		return checked
	}

	result, err := check.Run(ctx, &timedSession{inner: built, out: c.stderr, consumer: plan.name}, c.checkOptions(plan))
	if err != nil {
		result = report.SetupFailed(err)
	}

	checked.Report = result

	if result.Setup != nil {
		checked.Outcome, checked.Reason = report.OutcomeSetup, c.setupReason(result.Setup)
	}

	if err := holdLines(built.Holds(), c.logHold); err != nil {
		c.stderr.line("stutter: the invocation log: " + err.Error())
	}

	return checked
}

// setupReason names why a consumer check stopped where the error alone would not say it plainly: two
// corpus files the bus took for one message, or an interrupt.
func (c *composeCheck) setupReason(err error) string {
	var fill *corpus.FillError

	switch {
	case errors.As(err, &fill):
		return fillCollisionReason(c.fileOf(fill.Seq), c.fileOf(fill.Other))
	case errors.Is(err, context.Canceled):
		return interruptedDuring
	default:
		return ""
	}
}

// fileOf is the corpus file a rank came from; empty for zero or a rank no file holds.
func (c *composeCheck) fileOf(seq uint64) string {
	for index, message := range c.loaded.Messages {
		if message.Seq == seq && seq != 0 {
			return c.loaded.Files[index]
		}
	}

	return ""
}

// summarise reads what the header needs from every consumer check's sandbox: the external hosts the
// stub answered, summed over the checks, and the timings the harness applied.
func (c *composeCheck) summarise(sandboxes []sandbox) {
	tallies := make([][]httpproxy.HostTally, 0, len(sandboxes))

	for _, built := range sandboxes {
		if built == nil {
			continue
		}

		tallies = append(tallies, built.Tally())

		if c.header.Timings == nil {
			timings := built.Timings()
			c.header.Timings = &timings
		}
	}

	hosts := httpproxy.SumTallies(tallies...)
	c.header.Hosts = &hosts
}

// holdLines writes one invocation-log line per Fill hold: never to stdout or stderr.
func holdLines(holds []harness.FillHold, log func(provision.Hold) error) error {
	errs := make([]error, 0, len(holds))

	for _, hold := range holds {
		errs = append(errs, log(provision.Hold{
			Term: hold.Term, Length: hold.Length, Bound: hold.Bound, Bytes: int64(hold.Bytes), Messages: hold.Messages,
		}))
	}

	return errors.Join(errs...)
}

// writeHold writes one hold through the engine's invocation log.
func (c *composeCheck) writeHold(hold provision.Hold) error {
	return c.engine.LogHold(hold) //nolint:wrapcheck // reported by the caller with where it failed.
}
