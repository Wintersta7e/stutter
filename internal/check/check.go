// Package check orchestrates one full run: enforce the gates, inject every fault the recorded
// configuration permits, shrink whatever diverged, and rule on the result.
//
// It owns the order those steps happen in and nothing else. Provisioning lives behind Session,
// because a container provisioner does not exist yet and this package should not have to change
// when it does.
package check

import (
	"cmp"
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

// errCleanStopped means an egress-policy stop ended a clean run, where nothing was faulted: nothing
// can be compared against a run it cut short.
var errCleanStopped = errors.New("an egress-policy stop ended a clean run")

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
	// Files names the corpus file each sequence came from, when the corpus was read from files. Every
	// report carries it, and a repro names the file beside each sequence. Nil names none.
	Files map[uint64]string
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

// Field order is dictated by govet's fieldalignment check, not by reading order.
type check struct {
	session  Session
	comparer *gate.Comparer
	// coverage is one line per fault the hunt considered; the fault being hunted owns the last one.
	coverage []report.Coverage
	opts     Options
	// passes counts every replay, including the reference pair and shrink candidates. It only names
	// consumers uniquely.
	passes int
	// injected counts mutated runs, which is what MaxRuns caps. Counting every pass instead would
	// let the reference pair alone exhaust a small budget and silently inject nothing.
	injected int
	// completed counts the runs that returned, whatever they were for.
	completed int
}

func (c *check) execute(ctx context.Context) (report.Report, error) {
	scan := report.Scan{Consumers: 1, Messages: len(c.opts.Messages)}

	clean, err := c.cleanPair(ctx)
	if err != nil {
		return c.setupFailed(err), nil
	}

	reference, repeat := clean[0], clean[1]
	referenceEffects := effect.Compared(reference.Effects)

	// Observation leads: a reference that saw nothing makes determinism hold over two empty
	// sequences, and naming that as the violation would send the reader hunting for instability.
	observation := report.GateCheck{Name: report.GateObservation, Result: gate.Observed(referenceEffects)}
	gates := []report.GateCheck{
		observation,
		{Name: report.GateDeterminism, Result: c.comparer.Compare(referenceEffects, effect.Compared(repeat.Effects))},
	}

	health := c.health(reference, repeat, referenceEffects)

	// A violated gate stops the run rather than qualifying it: every comparison past this point
	// would be noise, and a caveated finding list is worse than none.
	gated := report.New(scan, gates, nil)
	gated.Health = &health
	gated.GatesOnly = c.opts.GatesOnly
	gated.Files = c.opts.Files

	if len(gated.Violations()) > 0 || c.opts.GatesOnly {
		return gated, nil
	}

	divergences, err := c.hunt(ctx, reference)

	// The clean runs agreed and the service did not under a fault. That is determinism violated, not a
	// broken sandbox, and every finding made so far rests on the same unstable service.
	if unreproduced, found := errors.AsType[*notReproducibleError](err); found {
		gates, err = []report.GateCheck{observation, unreproduced.violation}, nil
	}

	if err != nil {
		return c.setupFailed(err), nil
	}

	built := report.New(scan, gates, divergences)
	built.Health = &health
	built.Files = c.opts.Files
	built.Coverage = c.coverage

	return built, nil
}

// setupFailed reports a check that could not be carried out, counting the runs that completed first.
func (c *check) setupFailed(err error) report.Report {
	failed := report.SetupFailed(err)
	failed.Files = c.opts.Files
	failed.Completed = c.completed

	return failed
}

// cleanPair runs the reference and repeat clean runs, refusing either one nothing can be compared
// against.
func (c *check) cleanPair(ctx context.Context) ([2]replay.Result, error) {
	var pair [2]replay.Result

	for at, which := range []string{"reference", "repeat"} {
		result, err := c.pass(ctx, "clean", replay.Clean{}, nil)
		if err != nil {
			return pair, err
		}

		if err := cleanRunUsable(which, result); err != nil {
			return pair, err
		}

		pair[at] = result
	}

	return pair, nil
}

// cleanRunUsable refuses a clean run nothing can be compared against: one an egress-policy stop
// ended, or one the service exited during before every message was done. Nothing was faulted, so
// neither is a fault's consequence — it is the environment or the service's plain behaviour. An exit
// after every message was done leaves a complete run, and is only recorded.
func cleanRunUsable(which string, result replay.Result) error {
	if result.Stopped != "" {
		return fmt.Errorf("the %s clean run: %w: %s", which, errCleanStopped, result.Stopped)
	}

	if result.Exit.Exited && result.Owed > 0 {
		return fmt.Errorf("the %s clean run: %w", which, &replay.ExitError{Exit: result.Exit})
	}

	return nil
}

// exited keeps an exit only when the service stopped by itself.
func exited(exit replay.Exit) replay.Exit {
	if exit.Exited {
		return exit
	}

	return replay.Exit{}
}

// health summarises the clean run every mutated run is compared against, from the same
// rejected-free view the gates compared. The exit it records is either clean run's, when the service
// exited by itself after every message was done.
func (c *check) health(reference, repeat replay.Result, compared []effect.Effect) report.Health {
	acted := make(map[uint64]struct{}, len(c.opts.Messages))
	for _, item := range compared {
		acted[item.MessageSeq] = struct{}{}
	}

	var silent []uint64

	for _, seq := range c.opts.Messages {
		if _, did := acted[seq]; !did {
			silent = append(silent, seq)
		}
	}

	return report.Health{
		Messages:        len(c.opts.Messages),
		Delivered:       reference.Delivered,
		Failed:          reference.Failed,
		Effects:         len(compared),
		Silent:          len(silent),
		SilentSeqs:      silent,
		Setup:           reference.Setup,
		Late:            reference.Late,
		Refusals:        reference.Refusals,
		NoResponders:    reference.NoResponders,
		FedBack:         reference.FedBack,
		Elsewhere:       reference.Elsewhere,
		ClosedAfterInfo: reference.ClosedAfterInfo,
		Owed:            reference.Owed,
		Exhausted:       reference.Exhausted,
		DeliveryCap:     reference.DeliveryCap,
		Exit:            cmp.Or(exited(reference.Exit), exited(repeat.Exit)),
	}
}

// hunt injects every permitted fault against every message, within the run budget, and accounts for
// every fault it considered in the coverage: illegal, or each pair attempted, unexpressed or cut.
func (c *check) hunt(ctx context.Context, reference replay.Result) ([]report.Divergence, error) {
	var (
		found []report.Divergence
		spent bool
	)

	for _, fault := range faultOrder {
		verdict := c.opts.Config.Permits(fault)
		line := report.Coverage{Fault: fault, Clause: verdict.Clause, Legal: verdict.Permitted}

		if verdict.Permitted {
			line.Pairs = len(c.opts.Messages)
		}

		if spent {
			line.CutByBudget = line.Pairs
		}

		c.coverage = append(c.coverage, line)

		if !verdict.Permitted || spent {
			continue
		}

		diverged, exhausted, err := c.huntFault(ctx, reference, fault)
		if err != nil {
			return nil, err
		}

		found = append(found, diverged...)
		spent = exhausted
	}

	return found, nil
}

// huntFault injects one fault against every message, and reports whether the run budget ran out.
//
// A fault the session cannot express is refused alike for every message, and each attempt costs a
// reset before the refusal: after the first, the fault is skipped for the rest of the consumer's
// messages and counted as unexpressed, so it is paid for once and never once per message.
func (c *check) huntFault(
	ctx context.Context,
	reference replay.Result,
	fault policy.Fault,
) ([]report.Divergence, bool, error) {
	var found []report.Divergence

	line := &c.coverage[len(c.coverage)-1]

	for index, seq := range c.opts.Messages {
		if c.opts.MaxRuns > 0 && c.injected >= c.opts.MaxRuns {
			line.CutByBudget += len(c.opts.Messages) - index

			return found, true, nil
		}

		divergence, diverged, err := c.attempt(ctx, reference, fault, seq)
		if errors.Is(err, replay.ErrUnsupported) {
			line.Unexpressed += len(c.opts.Messages) - index

			return found, false, nil
		}

		if err != nil {
			return nil, false, err
		}

		if diverged {
			found = append(found, divergence)
		}
	}

	return found, false, nil
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

	// A session may be unable to express a fault against the service it is driving: a run watched on
	// the wire has no way to hold a delivery back before a service that pulls for itself has already
	// been handed it. The refusal is returned as it came, and hunt skips the fault — once per consumer
	// — as it skips a fault with no mutation at all; injecting something weaker under the same name
	// would be worse than injecting nothing.
	mutated, err := c.pass(ctx, string(fault), mutation, nil)
	if err != nil {
		return report.Divergence{}, false, err
	}

	// Counted only once the run happened, so a fault the session refused costs the budget nothing.
	c.injected++
	c.coverage[len(c.coverage)-1].Attempted++

	// Every comparison and every position taken from one uses the rejected-free view: the gate
	// reports an ordinal within the sequence it was given, so indexing a different one would name a
	// different effect in the finding.
	referenceEffects := effect.Compared(reference.Effects)
	mutatedEffects := effect.Compared(mutated.Effects)

	outcome := c.comparer.Compare(referenceEffects, mutatedEffects)
	if outcome.OK() {
		return report.Divergence{}, false, nil
	}

	repro, err := c.shrink(ctx, mutation)
	if errors.Is(err, shrink.ErrNotReproducible) {
		return report.Divergence{}, false, notReproducible(fault, seq, outcome)
	}

	if err != nil {
		return report.Divergence{}, false, err
	}

	divergence := c.describe(outcome, mutatedEffects, fault, mutated.Clause, repro)
	divergence.ReadsOnly = readsOnly(
		forMessage(referenceEffects, outcome.Message),
		forMessage(mutatedEffects, outcome.Message),
	)
	// A faulted run that ended on an exit or a stop may be showing the fault's consequence: the
	// comparison stands, and the finding carries how the run ended.
	divergence.Stopped, divergence.Exit = mutated.Stopped, exited(mutated.Exit)

	return divergence, true, nil
}

// notReproducibleError ends a hunt whose divergence the shrink could not reproduce, carrying the
// determinism violation it amounts to out of the hunt so the check reports it as one.
type notReproducibleError struct {
	violation report.GateCheck
}

func (e *notReproducibleError) Error() string {
	return "a divergence under " + string(e.violation.Fault) + " against message " +
		strconv.FormatUint(e.violation.Result.Message, 10) + " did not reproduce"
}

// notReproducible is the determinism violation a divergence amounts to when replaying its fault did
// not diverge again. It names the fault and the message it was aimed at, and keeps the first
// difference the faulted run showed.
func notReproducible(fault policy.Fault, seq uint64, outcome gate.Result) error {
	return &notReproducibleError{violation: report.GateCheck{
		Name:  report.GateDeterminism,
		Fault: fault,
		Result: gate.Result{
			Class:   gate.ClassNotReproducible,
			Want:    outcome.Want,
			Got:     outcome.Got,
			Message: seq,
			Index:   -1,
		},
	}}
}

// readsOnly reports whether everything that differs between two runs' effects for one message is a
// read.
//
// The effects are compared as multisets, not by the first difference. A guard that looks its claim
// up again on redelivery and then does nothing adds a read and nothing else; a guard that misses adds
// the read AND the write, and judging the first difference alone would call that one harmless too.
func readsOnly(reference, mutated []effect.Effect) bool {
	counts := make(map[string]int, len(reference)+len(mutated))
	reads := make(map[string]bool, len(reference)+len(mutated))

	tally := func(effects []effect.Effect, step int) {
		for _, item := range effects {
			key := string(item.Kind) + " " + item.Canonical
			counts[key] += step
			reads[key] = item.Read
		}
	}

	tally(reference, 1)
	tally(mutated, -1)

	differs := false

	for key, count := range counts {
		if count == 0 {
			continue
		}

		if !reads[key] {
			return false
		}

		differs = true
	}

	return differs
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
//
// The starting candidate replays the original experiment, so its clean run is judged like the
// reference. Every later candidate is a subset, which may feed the service inputs it cannot handle on
// their own: one whose clean run ends on an exit before every message is done, or on a stop, simply
// does not reproduce, and the shrink goes on.
func (c *check) shrink(ctx context.Context, mutation replay.Mutation) (string, error) {
	starting := true

	attempt := func(ctx context.Context, messages []uint64, mutations []replay.Mutation) (bool, error) {
		if len(messages) == 0 || len(mutations) == 0 {
			return false, nil
		}

		clean, err := c.pass(ctx, "shrink-clean", replay.Clean{}, messages)
		if err != nil {
			return false, err
		}

		first := starting
		starting = false

		if unusable := cleanRunUsable("shrink's starting", clean); unusable != nil {
			if first {
				return false, unusable
			}

			return false, nil
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

	return describeRepro(minimal, stats, c.opts.Files), nil
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

	c.completed++

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
		// A loop, not one withheld acknowledgement: withholding once is a duplicate by another name.
		return replay.CrashBeforeAck{Seq: seq, Times: policy.CrashLoopWithheld}, true
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
	case gate.ClassNotReproducible:
		return "the divergence did not reproduce"
	default:
		return "the effect sequence differed"
	}
}

func describeRepro(minimal shrink.Candidate, stats shrink.Stats, files map[uint64]string) string {
	repro := "messages " + report.Sequences(minimal.Messages, files)
	if !stats.Minimal() {
		return repro + " (search hit its attempt cap; smaller may exist)"
	}

	return repro
}
