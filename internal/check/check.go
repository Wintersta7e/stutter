// Package check orchestrates one full run: enforce the gates, inject every fault the recorded
// configuration permits, shrink whatever diverged, and rule on the result.
//
// It owns the order those steps happen in and nothing else. Provisioning lives behind Session,
// because a container provisioner does not exist yet and this package should not have to change
// when it does.
package check

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
	"github.com/Wintersta7e/stutter/internal/shrink"
)

// delayMargin is added to a deadline so a delay fault crosses it rather than racing it.
const delayMargin = 2

// faultOrder is the order faults are attempted in, cheapest and most common first.
//
// Concurrency is absent deliberately: replay refuses it, because injecting it today would
// mis-attribute effects and manufacture false positives.
var faultOrder = []policy.Fault{ //nolint:gochecknoglobals // a fixed schedule, not mutable state.
	policy.FaultDuplicate,
	policy.FaultCrashBeforeAck,
	policy.FaultDelay,
	policy.FaultReorder,
}

// Session is one provisioned service under test, together with everything it writes to.
type Session interface {
	// Reset returns the service and its dependencies to the starting position — datastore and
	// bus-side state alike. A claim left in a key/value bucket corrupts the next run exactly as
	// leftover rows would.
	Reset(ctx context.Context) error
	// Run replays the corpus under one mutation. A non-empty retain limits the run to those stream
	// sequences; everything else is acknowledged without reaching the handler.
	Run(ctx context.Context, name string, mutation replay.Mutation, retain []uint64) (replay.Result, error)
}

// Invariant is a user's standing answer to a divergence Stutter cannot rule on by itself.
//
// Some repeated work is harmless and some is a second charge, and nothing in the effect sequence
// says which — an audit row written twice is append-only by design, while a payment captured twice
// is the bug. Only the person who owns the handler knows, so this is how they say it once instead of
// re-reading the same WARN on every run.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Invariant struct {
	// Consumer limits the rule to one consumer. Empty matches any.
	Consumer string
	// Matches is a substring of the canonical form of the effect that diverged. A substring rather
	// than a pattern because the canonical form is already normalised: the values that would differ
	// between runs have been substituted out, so the stable part is plain text.
	Matches string
	// Because is why, and is rendered beside the finding. A verdict set by declaration has to carry
	// the declaration's reason or nobody can audit it later.
	Because string
	// Impact is the classification to apply: corrupting promotes the finding to a failure, acceptable
	// silences it into Report.Silenced.
	Impact report.Impact
}

// classify returns the impact a declared invariant puts on this divergence, if any.
//
// The canonical form of the effect that DIVERGED is what a rule matches, because that is the effect
// the finding is about. Rules are tried in order and the first match wins, so a specific rule can be
// placed ahead of a general one.
func classify(invariants []Invariant, consumer, diverged string) (Invariant, bool) {
	for _, rule := range invariants {
		if rule.Consumer != "" && rule.Consumer != consumer {
			continue
		}

		if rule.Matches == "" || !strings.Contains(diverged, rule.Matches) {
			continue
		}

		return rule, true
	}

	return Invariant{}, false
}

// Options configures a check.
type Options struct {
	// Messages are the corpus stream sequences, in recorded order.
	Messages []uint64
	// Invariants are the user's standing answers about which repeated work matters. Empty leaves
	// every divergence to the protocol default, which is the zero-declaration starting point.
	Invariants []Invariant
	// Consumer names the consumer under test, for attribution.
	Consumer string
	// Config is the recorded consumer configuration. It decides which faults are legal.
	Config policy.Config
	// MaxRuns caps mutated runs. Zero means one per legal message-and-fault pair.
	MaxRuns int
	// GatesOnly stops after the gates, injecting nothing. It answers "is this service stable enough
	// to be tested at all?", which is worth knowing before spending a full run to find out.
	GatesOnly bool
	// ShrinkAttempts caps each shrink. Zero uses the shrinker's own default.
	ShrinkAttempts int
}

// Run performs the check and returns the report.
//
// An error is returned only when the run could not be performed at all; a report that fails is a
// successful check with a bad verdict, not an error.
func Run(ctx context.Context, session Session, opts Options) (report.Report, error) {
	run := &check{
		session:  session,
		opts:     opts,
		comparer: gate.NewComparer(),
	}

	return run.execute(ctx)
}

type check struct {
	session  Session
	comparer *gate.Comparer
	opts     Options
	// passes counts every replay, including the reference pair and shrink candidates. It only names
	// consumers uniquely.
	passes int
	// injected counts mutated runs, which is what MaxRuns caps. Counting every pass instead would
	// let the reference pair alone exhaust a small budget and silently inject nothing.
	injected int
}

func (c *check) execute(ctx context.Context) (report.Report, error) {
	scan := report.Scan{Consumers: 1, Messages: len(c.opts.Messages)}

	reference, err := c.pass(ctx, "clean", replay.Clean{}, nil)
	if err != nil {
		return report.SetupFailed(err), nil
	}

	repeat, err := c.pass(ctx, "clean", replay.Clean{}, nil)
	if err != nil {
		return report.SetupFailed(err), nil
	}

	referenceEffects := effect.Compared(reference.Effects)

	// Observation leads: a reference that saw nothing makes determinism hold over two empty
	// sequences, and naming that as the violation would send the reader hunting for instability.
	gates := []report.GateCheck{
		{Name: report.GateObservation, Result: gate.Observed(referenceEffects)},
		{Name: report.GateDeterminism, Result: c.comparer.Compare(referenceEffects, effect.Compared(repeat.Effects))},
	}

	health := c.health(reference, referenceEffects)

	// A violated gate stops the run rather than qualifying it: every comparison past this point
	// would be noise, and a caveated finding list is worse than none.
	gated := report.New(scan, gates, nil)
	gated.Health = &health

	if len(gated.Violations()) > 0 || c.opts.GatesOnly {
		return gated, nil
	}

	divergences, err := c.hunt(ctx, reference)
	if err != nil {
		return report.SetupFailed(err), nil
	}

	built := report.New(scan, gates, divergences)
	built.Health = &health

	return built, nil
}

// health summarises the clean run every mutated run is compared against, from the same
// rejected-free view the gates compared.
func (c *check) health(reference replay.Result, compared []effect.Effect) report.Health {
	acted := make(map[uint64]struct{}, len(c.opts.Messages))
	for _, item := range compared {
		acted[item.MessageSeq] = struct{}{}
	}

	silent := 0

	for _, seq := range c.opts.Messages {
		if _, did := acted[seq]; !did {
			silent++
		}
	}

	return report.Health{
		Messages:  len(c.opts.Messages),
		Delivered: reference.Delivered,
		Failed:    reference.Failed,
		Effects:   len(compared),
		Silent:    silent,
		Setup:     reference.Setup,
		Late:      reference.Late,
	}
}

// hunt injects every permitted fault against every message, within the run budget.
func (c *check) hunt(ctx context.Context, reference replay.Result) ([]report.Divergence, error) {
	var found []report.Divergence

	for _, fault := range faultOrder {
		if !c.opts.Config.Permits(fault).Permitted {
			continue
		}

		for _, seq := range c.opts.Messages {
			if c.opts.MaxRuns > 0 && c.injected >= c.opts.MaxRuns {
				return found, nil
			}

			divergence, diverged, err := c.attempt(ctx, reference, fault, seq)
			if err != nil {
				return nil, err
			}

			if diverged {
				found = append(found, divergence)
			}
		}
	}

	return found, nil
}

// attempt runs one fault against one message and, if it diverged, shrinks it to a minimal repro.
func (c *check) attempt(
	ctx context.Context,
	reference replay.Result,
	fault policy.Fault,
	seq uint64,
) (report.Divergence, bool, error) {
	mutation, buildable := c.mutationFor(fault, seq)
	if !buildable {
		return report.Divergence{}, false, nil
	}

	mutated, err := c.pass(ctx, string(fault), mutation, nil)

	// A session may be unable to express a fault against the service it is driving: a run watched on
	// the wire has no way to hold a delivery back before a service that pulls for itself has already
	// been handed it. Skipping matches how a fault with no mutation at all is treated, and injecting
	// something weaker under the same name would be worse than injecting nothing.
	if errors.Is(err, replay.ErrUnsupported) {
		return report.Divergence{}, false, nil
	}

	if err != nil {
		return report.Divergence{}, false, err
	}

	// Counted only once the run happened, so a fault the session refused costs the budget nothing.
	c.injected++

	// Every comparison and every position taken from one uses the rejected-free view: the gate
	// reports an ordinal within the sequence it was given, so indexing a different one would name a
	// different effect in the finding.
	mutatedEffects := effect.Compared(mutated.Effects)

	outcome := c.comparer.Compare(effect.Compared(reference.Effects), mutatedEffects)
	if outcome.OK() {
		return report.Divergence{}, false, nil
	}

	repro, err := c.shrink(ctx, mutation)
	if err != nil {
		return report.Divergence{}, false, err
	}

	return c.describe(outcome, mutatedEffects, fault, mutated.Clause, repro), true, nil
}

// describe turns a comparison that failed into the divergence a report rules on.
func (c *check) describe(
	outcome gate.Result,
	mutatedEffects []effect.Effect,
	fault policy.Fault,
	clause string,
	repro string,
) report.Divergence {
	messageEffects := forMessage(mutatedEffects, outcome.Message)
	metadata := metadataPositions(messageEffects)

	// The mutated run's effect is what diverged, except where the fault made an effect disappear and
	// there is only the reference's to name.
	diverged := outcome.Got
	if diverged == "" {
		diverged = outcome.Want
	}

	declared, _ := classify(c.opts.Invariants, c.opts.Consumer, diverged)

	return report.Divergence{
		Consumer:     c.opts.Consumer,
		Fault:        fault,
		Clause:       clause,
		Summary:      summarise(outcome),
		Repro:        repro,
		Clean:        outcome.Want,
		Mutated:      outcome.Got,
		DivergedKind: kindAt(messageEffects, outcome.Index),
		StubReads:    metadata.stubbed,
		OffScript:    metadata.offScript,
		DivergedAt:   outcome.Index,
		Impact:       declared.Impact,
		Because:      declared.Because,
	}
}

// shrink reduces a divergence to the smallest message set that still reproduces it.
//
// Each candidate is compared against a reference replayed over the SAME retained messages. Reusing
// the full-corpus reference would make every subset look like it reproduces, and the shrink would
// return a repro that proves nothing.
func (c *check) shrink(ctx context.Context, mutation replay.Mutation) (string, error) {
	attempt := func(ctx context.Context, messages []uint64, mutations []replay.Mutation) (bool, error) {
		if len(messages) == 0 || len(mutations) == 0 {
			return false, nil
		}

		clean, err := c.pass(ctx, "shrink-clean", replay.Clean{}, messages)
		if err != nil {
			return false, err
		}

		faulted, err := c.pass(ctx, "shrink-faulted", mutations[0], messages)
		if err != nil {
			return false, err
		}

		return !c.comparer.Compare(effect.Compared(clean.Effects), effect.Compared(faulted.Effects)).OK(), nil
	}

	start := shrink.Candidate{Messages: c.opts.Messages, Mutations: []replay.Mutation{mutation}}

	minimal, stats, err := shrink.Shrink(ctx, start, attempt, shrink.Options{
		MaxAttempts: c.opts.ShrinkAttempts,
	})
	if err != nil {
		return "", fmt.Errorf("shrink %s: %w", mutation.Fault(), err)
	}

	return describeRepro(minimal, stats), nil
}

// pass resets the session and replays once. Every pass gets a distinct consumer name: reusing one
// resumes from the previous run's position instead of replaying from the start.
func (c *check) pass(
	ctx context.Context,
	label string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	if err := c.session.Reset(ctx); err != nil {
		return replay.Result{}, fmt.Errorf("reset before %s: %w", label, err)
	}

	c.passes++
	name := label + "-" + strconv.Itoa(c.passes)

	result, err := c.session.Run(ctx, name, mutation, retain)
	if err != nil {
		return replay.Result{}, fmt.Errorf("run %s: %w", name, err)
	}

	return result, nil
}

// mutationFor builds the mutation that injects a fault against one message.
//
// It returns the interface because selecting an implementation is the whole job; a concrete return
// type would defeat the point.
//
//nolint:ireturn // a factory over the Mutation implementations.
func (c *check) mutationFor(fault policy.Fault, seq uint64) (replay.Mutation, bool) {
	// exhaustive wants the remaining faults listed, revive rejects the identical branch that would
	// produce. The default covers them, and adding an injectable fault means adding a case here.
	switch fault { //nolint:exhaustive // default covers the faults with no injectable mutation.
	case policy.FaultDuplicate:
		return replay.Duplicate{Seq: seq}, true
	case policy.FaultCrashBeforeAck:
		return replay.CrashBeforeAck{Seq: seq, Times: 1}, true
	case policy.FaultDelay:
		// Sized against the deadline governing the first attempt, which is the backoff curve's
		// first entry wherever one is set — never the declared ack wait.
		return replay.Delay{Seq: seq, For: c.opts.Config.Deadline(1) * delayMargin}, true
	case policy.FaultReorder:
		return replay.Reorder{First: seq}, true
	default:
		// Concurrent, drop and the no-fault sentinel have no injectable mutation today.
		return nil, false
	}
}

// forMessage narrows a run's effects to one message's.
//
// The gate reports a position WITHIN a message, so the flat sequence must be narrowed the same way
// before that position means anything. Indexing the flat slice instead names an effect belonging to
// some other message, and the finding then claims a protocol the handler never used.
func forMessage(effects []effect.Effect, message uint64) []effect.Effect {
	narrowed := make([]effect.Effect, 0, len(effects))

	for _, item := range effects {
		if item.MessageSeq == message {
			narrowed = append(narrowed, item)
		}
	}

	return narrowed
}

func kindAt(effects []effect.Effect, index int) effect.Kind {
	if index < 0 || index >= len(effects) {
		return effect.KindOpaque
	}

	return effects[index].Kind
}

// metadataPositions keeps stub metadata in the same per-message coordinate system as the gate's
// divergence index. Effect.Seq is run-global, so copying it here would compare unrelated ordinals.
type effectMetadata struct {
	stubbed   []int
	offScript []int
}

func metadataPositions(effects []effect.Effect) effectMetadata {
	var metadata effectMetadata

	for index, item := range effects {
		if item.Stubbed {
			metadata.stubbed = append(metadata.stubbed, index)
		}

		if item.OffScript {
			metadata.offScript = append(metadata.offScript, index)
		}
	}

	return metadata
}

func summarise(outcome gate.Result) string {
	switch outcome.Class {
	case gate.ClassDivergentSet:
		return "the service did different work under the fault"
	case gate.ClassReordered:
		return "the same work happened in a different order"
	case gate.ClassFieldDrift:
		return "the same work happened with different values"
	case gate.ClassMatch:
		return "no divergence"
	case gate.ClassUnobserved:
		return "nothing was observed to compare"
	default:
		return "the effect sequence differed"
	}
}

func describeRepro(minimal shrink.Candidate, stats shrink.Stats) string {
	repro := "messages " + joinSeqs(minimal.Messages)
	if !stats.Minimal() {
		return repro + " (search hit its attempt cap; smaller may exist)"
	}

	return repro
}

func joinSeqs(seqs []uint64) string {
	parts := make([]string, 0, len(seqs))
	for _, seq := range seqs {
		parts = append(parts, "#"+strconv.FormatUint(seq, 10))
	}

	return strings.Join(parts, ", ")
}
