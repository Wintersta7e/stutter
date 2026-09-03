// Package shrink reduces a reproducing failure to the smallest input that still reproduces it.
//
// A report saying "somewhere in these 12,400 messages" is worthless; "these two messages, in this
// order, under this one fault" is actionable. A finding without a minimal repro is not a finding,
// so this runs before a failure is reported, never after.
//
// The search is delta debugging (ddmin) over two axes — which corpus messages are retained, and
// which faults are applied — alternating until neither reduces. Two axes rather than one because a
// failure that needs a single message routinely still carries three mutations, and the mutation
// that survives is the one the user has to fix.
//
// Causal prefixes are not special-cased: dropping the message a later one depends on produces a
// candidate that no longer reproduces, and ddmin discards it by construction.
//
// The package owns no bus, no proxy and no database. Replaying a candidate is the caller's Attempt,
// which is what makes the search testable against a deterministic fake — the search is where the
// bugs in this file are, and exercising them must not cost a container.
package shrink

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Wintersta7e/stutter/internal/replay"
)

// DefaultMaxAttempts bounds a search whose caller did not bound it.
//
// One attempt is a full replay against a live service, so the cap is a wall-clock budget in
// disguise. ddmin finds a k-element cause among n in roughly 2k·log2(n) attempts — 56 for a
// two-message cause in a corpus of twelve thousand — so this leaves ample headroom for the searches
// that converge while refusing to let a pathological one run for an hour.
const DefaultMaxAttempts = 500

// splitFactor is ddmin's branching: a round starts by halving the retained set, and each escalation
// splits every part in two again.
const splitFactor = 2

// complementsWorthTesting is the number of parts above which a complement is something the parts
// loop has not already tried. At two parts each complement IS the other part, and testing them
// again would double the cost of every search that ends in a two-element answer.
const complementsWorthTesting = splitFactor + 1

// ErrNotReproducible means the candidate handed to Shrink did not reproduce the failure.
//
// It is an error rather than an empty result: something upstream believed this input failed, and
// silently returning "nothing to shrink" would turn a broken reproducer into a passing report.
var ErrNotReproducible = errors.New("the starting candidate did not reproduce the failure")

// errCapped stops the search when the attempt budget runs out. It never reaches the caller — a
// truncated search still yields a candidate that genuinely reproduces, and all that is lost is its
// claim to minimality, which Stats.CapHit reports instead.
var errCapped = errors.New("attempt budget exhausted")

// Attempt replays one candidate and reports whether it still reproduces the failure.
//
// The clean reference it compares against MUST be recomputed over the same retained messages, not
// carried over from the full corpus. A clean run of two messages does not produce the first two
// effects of a clean run of twelve thousand, so comparing a subset against the full-corpus
// reference makes every candidate look like it reproduces, and the search then "minimises" to one
// arbitrary message with a fault that has nothing to do with the failure.
//
// An error must mean "the replay did not complete", never "it did not reproduce". Reporting a
// broken replay as a non-reproduction lets the search delete the actual cause and then present
// whatever survived, so an error stops the search rather than steering it.
type Attempt func(ctx context.Context, messages []uint64, mutations []replay.Mutation) (bool, error)

// Candidate is one input to the service under test: the corpus messages delivered, and the faults
// applied while they are delivered.
type Candidate struct {
	// Messages are the stream sequences retained, in delivery order.
	Messages []uint64
	// Mutations are the faults applied to that delivery.
	//
	// A mutation whose target message is no longer retained is inert rather than invalid; the
	// mutation axis removes it, because dropping something inert still reproduces.
	Mutations []replay.Mutation
}

// Options tunes the search. The zero value is usable and applies the package defaults.
type Options struct {
	// MaxAttempts caps calls to Attempt across the whole search, including the one that verifies the
	// starting candidate. Non-positive means DefaultMaxAttempts.
	MaxAttempts int
}

func (o Options) withDefaults() Options {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}

	return o
}

// Stats records what the search cost and what it achieved.
//
// A shrinker that silently does nothing is the failure mode worth catching, and it looks exactly
// like a successful one from the outside. The before-and-after counts make a no-op visible in the
// report instead of leaving "minimal" to be taken on trust.
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Stats struct {
	// Attempts counts calls to Attempt, including the one that verified the starting candidate.
	Attempts int
	// StartMessages is how many messages the failing run delivered before shrinking.
	StartMessages int
	// StartMutations is how many faults it applied before shrinking.
	StartMutations int
	// FinalMessages is how many messages the returned candidate retains.
	FinalMessages int
	// FinalMutations is how many faults it retains.
	FinalMutations int
	// CapHit reports that the attempt budget ran out before the search finished.
	CapHit bool
}

// Minimal reports whether the returned candidate was proved 1-minimal: no single message and no
// single mutation can be removed from it and still reproduce.
//
// A capped search returns a candidate that reproduces but was never proved minimal. A report must
// not call that minimal — claiming a minimality nobody established is how a user stops believing
// the rest of the finding.
func (s Stats) Minimal() bool {
	return !s.CapHit
}

// Shrink reduces start to the smallest candidate that still reproduces, and reports what that cost.
//
// start is verified before any reduction: a candidate that does not reproduce yields
// ErrNotReproducible. An Attempt error is propagated, and both error paths still return the
// smallest candidate proved so far, so a caller that wants to report a partial result can.
func Shrink(
	ctx context.Context,
	start Candidate,
	attempt Attempt,
	opts Options,
) (Candidate, Stats, error) {
	search := &shrinker{
		attempt:   attempt,
		messages:  slices.Clone(start.Messages),
		mutations: slices.Clone(start.Mutations),
		budget:    opts.withDefaults().MaxAttempts,
	}

	kept := retained{
		messages:  indices(len(search.messages)),
		mutations: indices(len(search.mutations)),
	}

	// The budget is at least one, so this first attempt is never the one that exhausts it.
	reproduces, err := search.reproduces(ctx, kept.messages, kept.mutations)
	if err != nil {
		return Candidate{}, search.snapshot(kept), err
	}

	if !reproduces {
		return Candidate{}, search.snapshot(kept), fmt.Errorf(
			"%w: %d messages under %d mutations",
			ErrNotReproducible, len(search.messages), len(search.mutations),
		)
	}

	kept, err = search.alternate(ctx, kept)
	if err != nil && !errors.Is(err, errCapped) {
		return search.materialise(kept), search.snapshot(kept), err
	}

	return search.materialise(kept), search.snapshot(kept), nil
}

// retained is the pair of index sets the search works on: positions into the starting candidate's
// message and mutation slices. Indices rather than values so that both axes share one ddmin and the
// recorded delivery order survives every reduction for free.
type retained struct {
	messages  []int
	mutations []int
}

// predicate reports whether a retained index set on one axis still reproduces.
type predicate func(ctx context.Context, subset []int) (bool, error)

// shrinker is the state one search carries: the starting candidate it indexes into, and the budget
// it spends.
// Field order is dictated by govet's fieldalignment check, not by reading order.
type shrinker struct {
	attempt   Attempt
	messages  []uint64
	mutations []replay.Mutation
	budget    int
	used      int
	capped    bool
}

// alternate runs the two axes until neither reduces.
//
// An axis is re-run only when the other has reduced since it last ran. With the other axis
// unchanged a second pass would replay exactly the attempts that already failed to reduce it, and
// attempts are the expensive thing here. It also gives the exit condition its meaning: when the
// loop ends, each axis was last minimised against the other's final state, so the pair is
// 1-minimal on both axes rather than only on whichever ran last.
func (s *shrinker) alternate(ctx context.Context, kept retained) (retained, error) {
	messagesStale, mutationsStale := true, true

	for messagesStale || mutationsStale {
		if messagesStale {
			reduced, err := ddmin(ctx, kept.messages, func(ctx context.Context, subset []int) (bool, error) {
				return s.reproduces(ctx, subset, kept.mutations)
			})
			mutationsStale = mutationsStale || len(reduced) < len(kept.messages)
			kept.messages = reduced
			messagesStale = false

			if err != nil {
				return kept, err
			}
		}

		if mutationsStale {
			reduced, err := ddmin(ctx, kept.mutations, func(ctx context.Context, subset []int) (bool, error) {
				return s.reproduces(ctx, kept.messages, subset)
			})
			messagesStale = messagesStale || len(reduced) < len(kept.mutations)
			kept.mutations = reduced
			mutationsStale = false

			if err != nil {
				return kept, err
			}
		}
	}

	return kept, nil
}

// ddmin is the classic delta-debugging minimisation, iterative rather than recursive.
//
// The invariant that makes an interrupted search safe: every set held in items reproduces. The
// starting set was verified, and a set is only adopted after an attempt reported a reproduction, so
// when the budget runs out mid-search the caller still gets a genuine repro — just not a proven
// minimal one.
//
// The empty set is never tested, and does not need to be: a candidate delivering no messages
// produces no effects, and one applying no faults IS the clean run each candidate is compared
// against, so neither can reproduce by construction.
func ddmin(ctx context.Context, items []int, test predicate) ([]int, error) {
	granularity := splitFactor

	for len(items) > 1 {
		reduced, next, err := narrow(ctx, items, granularity, test)
		if err != nil {
			return items, err
		}

		if reduced == nil {
			if granularity >= len(items) {
				break
			}

			granularity = min(granularity*splitFactor, len(items))

			continue
		}

		items, granularity = reduced, next
	}

	return items, nil
}

// narrow tries one granularity: each part on its own first, then each part's complement. It returns
// the first set that still reproduces and the granularity to continue at, or a nil set when this
// granularity cannot reduce.
//
// Parts first because a part is the larger reduction. Complements are what find a cause that needs
// one element from each part, which is the whole reason ddmin beats removing elements one at a
// time.
func narrow(
	ctx context.Context,
	items []int,
	granularity int,
	test predicate,
) ([]int, int, error) {
	parts := partition(items, granularity)

	for _, part := range parts {
		reproduces, err := test(ctx, part)
		if err != nil {
			return nil, 0, err
		}

		if reproduces {
			return part, splitFactor, nil
		}
	}

	if len(parts) < complementsWorthTesting {
		return nil, 0, nil
	}

	for _, part := range parts {
		rest := without(items, part)

		reproduces, err := test(ctx, rest)
		if err != nil {
			return nil, 0, err
		}

		if reproduces {
			return rest, max(granularity-1, splitFactor), nil
		}
	}

	return nil, 0, nil
}

// reproduces spends one attempt on a candidate expressed as index sets.
func (s *shrinker) reproduces(ctx context.Context, messages, mutations []int) (bool, error) {
	if s.used >= s.budget {
		s.capped = true

		return false, errCapped
	}

	s.used++

	picked := pick(s.messages, messages)
	faults := pick(s.mutations, mutations)

	reproduces, err := s.attempt(ctx, picked, faults)
	if err != nil {
		return false, fmt.Errorf(
			"replaying a candidate of %d messages under %d mutations: %w",
			len(picked), len(faults), err,
		)
	}

	return reproduces, nil
}

// materialise turns retained index sets back into the candidate a caller can replay or report.
func (s *shrinker) materialise(kept retained) Candidate {
	return Candidate{
		Messages:  pick(s.messages, kept.messages),
		Mutations: pick(s.mutations, kept.mutations),
	}
}

func (s *shrinker) snapshot(kept retained) Stats {
	return Stats{
		Attempts:       s.used,
		StartMessages:  len(s.messages),
		StartMutations: len(s.mutations),
		FinalMessages:  len(kept.messages),
		FinalMutations: len(kept.mutations),
		CapHit:         s.capped,
	}
}

// partition splits items into count contiguous parts of near-equal size. Contiguous and
// index-ordered is what makes the whole search reproducible: the same input always yields the same
// parts in the same order, so the same minimal candidate comes back every time.
func partition(items []int, count int) [][]int {
	count = min(count, len(items))
	parts := make([][]int, 0, count)

	for index := range count {
		start := index * len(items) / count
		end := (index + 1) * len(items) / count
		parts = append(parts, items[start:end])
	}

	return parts
}

// without returns items minus excluded, preserving the order of items.
func without(items, excluded []int) []int {
	drop := make(map[int]struct{}, len(excluded))
	for _, index := range excluded {
		drop[index] = struct{}{}
	}

	rest := make([]int, 0, len(items)-len(excluded))

	for _, index := range items {
		if _, skip := drop[index]; !skip {
			rest = append(rest, index)
		}
	}

	return rest
}

// pick materialises one axis: the values the retained indices point at, in index order.
func pick[T any](from []T, chosen []int) []T {
	out := make([]T, len(chosen))
	for at, index := range chosen {
		out[at] = from[index]
	}

	return out
}

// indices numbers a slice's positions, the identity set every axis starts from.
func indices(count int) []int {
	out := make([]int, count)
	for index := range out {
		out[index] = index
	}

	return out
}
