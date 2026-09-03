// Package gate implements the checks that must hold before any divergence report is emitted.
//
// Both gates have the same shape: run the reference twice and demand an identical effect sequence.
// Determinism compares two unmutated clean runs; differential re-keying compares two clean runs
// whose corpora were pseudonymised under different keys. A violation of either means every
// downstream comparison is noise, so the report is withheld rather than qualified.
package gate

import (
	"regexp"
	"strconv"

	"github.com/Wintersta7e/stutter/internal/effect"
)

// Class names the way two effect sequences failed to match.
//
// The class does not change the verdict — anything but ClassMatch withholds the report. It changes
// where to look, which is the difference between a debuggable gate and one that only says "differs".
type Class string

const (
	// ClassMatch means the sequences are identical and the gate holds.
	ClassMatch Class = "match"
	// ClassDivergentSet means the runs produced different effects: the service under test is
	// genuinely unstable, or something outside Stutter's control changed between runs.
	ClassDivergentSet Class = "divergent-set"
	// ClassReordered means the runs produced the same effects in a different order, which points at
	// concurrency inside a single handler rather than at instability.
	ClassReordered Class = "reordered"
	// ClassFieldDrift means the effects have identical structure and differ only in literal values.
	// This is the one class that is Stutter's own fault: the normaliser is incomplete.
	ClassFieldDrift Class = "field-drift"
)

// Result reports whether two effect sequences matched, and where they first diverged.
type Result struct {
	// Class names the failure mode, or ClassMatch.
	Class Class
	// Want is the canonical form from the reference sequence at Index, empty if it ran short.
	Want string
	// Got is the canonical form from the compared sequence at Index, empty if it ran short.
	Got string
	// Message is the corpus message whose effects differed. Zero when the difference is in message
	// order rather than inside one message.
	Message uint64
	// Index is the first differing position WITHIN that message's effects, or -1 when they match.
	Index int
}

// OK reports whether the gate held.
func (r Result) OK() bool {
	return r.Class == ClassMatch
}

// Describe renders the result as the operator-facing explanation of where to look next.
func (r Result) Describe() string {
	position := "position " + strconv.Itoa(r.Index)
	if r.Message != 0 {
		position = "message #" + strconv.FormatUint(r.Message, 10) + ", effect " + strconv.Itoa(r.Index)
	}

	switch r.Class {
	case ClassMatch:
		return "gate held: both runs produced an identical effect sequence"
	case ClassReordered:
		return "messages were handled in a different order at " + position +
			":\n  want: " + r.Want + "\n   got: " + r.Got
	case ClassFieldDrift:
		return "identical structure, differing values at " + position +
			" — the normaliser is incomplete:\n  want: " + r.Want + "\n   got: " + r.Got
	case ClassDivergentSet:
		return "different effects at " + position +
			" — the service under test is not deterministic:\n  want: " + r.Want + "\n   got: " + r.Got
	default:
		return "unclassified gate failure at " + position
	}
}

// Comparer checks effect sequences from runs that should have behaved identically.
type Comparer struct {
	masker *regexp.Regexp
}

// NewComparer builds a Comparer.
func NewComparer() *Comparer {
	// Masks every run of characters that can appear in an identifier or a number, so that
	// "qty = 11" and "qty = 8" collapse to one shape while "UPDATE stock" and "DELETE FROM stock"
	// do not.
	return &Comparer{masker: regexp.MustCompile(`[0-9a-fA-F]+`)}
}

// Compare checks a sequence against the reference it should have reproduced.
//
// Effects are compared message by message, not as one flat run. A fault that adds an effect shifts
// every effect after it, and a flat comparison then reports the shift instead of the cause — the
// first difference lands where the two runs are describing different messages, which reads as a
// database write having replaced a publish. Grouping first means an extra effect is reported
// against the message that produced it.
//
// Classification past the first difference is a diagnostic heuristic, not a verdict: a misclassified
// failure still withholds the report, it just points at the wrong place first.
func (c *Comparer) Compare(reference, compared []effect.Effect) Result {
	referenceRuns := group(reference)
	comparedRuns := group(compared)

	// Message order first. A difference here is a reordering, and comparing contents before it
	// would report an arbitrary position inside whichever message moved.
	if index := firstDifference(order(referenceRuns), order(comparedRuns)); index >= 0 {
		return Result{
			Class: ClassReordered,
			Index: index,
			Want:  at(order(referenceRuns), index),
			Got:   at(order(comparedRuns), index),
		}
	}

	for _, run := range referenceRuns {
		referenceForms := canonicals(run.effects)
		comparedForms := canonicals(find(comparedRuns, run.message))

		index := firstDifference(referenceForms, comparedForms)
		if index < 0 {
			continue
		}

		return Result{
			Class:   c.classify(referenceForms, comparedForms, index),
			Index:   index,
			Message: run.message,
			Want:    at(referenceForms, index),
			Got:     at(comparedForms, index),
		}
	}

	return Result{Class: ClassMatch, Index: -1}
}

// messageRun is one message's effects, in the order it produced them.
type messageRun struct {
	effects []effect.Effect
	message uint64
}

// group collects effects by the message they were attributed to, keeping first-appearance order so
// a reordering is still visible.
func group(effects []effect.Effect) []messageRun {
	var runs []messageRun

	index := make(map[uint64]int, len(effects))

	for _, item := range effects {
		at, seen := index[item.MessageSeq]
		if !seen {
			index[item.MessageSeq] = len(runs)
			runs = append(runs, messageRun{message: item.MessageSeq})
			at = len(runs) - 1
		}

		runs[at].effects = append(runs[at].effects, item)
	}

	return runs
}

func order(runs []messageRun) []string {
	names := make([]string, 0, len(runs))
	for _, run := range runs {
		names = append(names, "message #"+strconv.FormatUint(run.message, 10))
	}

	return names
}

func find(runs []messageRun, message uint64) []effect.Effect {
	for _, run := range runs {
		if run.message == message {
			return run.effects
		}
	}

	return nil
}

func (c *Comparer) classify(reference, compared []string, index int) Class {
	if sameMultiset(reference, compared) {
		return ClassReordered
	}

	if len(reference) == len(compared) && c.sameSkeleton(reference[index], compared[index]) {
		return ClassFieldDrift
	}

	return ClassDivergentSet
}

// sameSkeleton reports whether two canonical forms differ only inside literal values. It is a hint
// about where to look, never a verdict.
func (c *Comparer) sameSkeleton(a, b string) bool {
	return c.masker.ReplaceAllString(a, "#") == c.masker.ReplaceAllString(b, "#")
}

func sameMultiset(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	counts := make(map[string]int, len(a))
	for _, value := range a {
		counts[value]++
	}

	for _, value := range b {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}

	return true
}

func firstDifference(a, b []string) int {
	for index := range max(len(a), len(b)) {
		if at(a, index) != at(b, index) {
			return index
		}
	}

	return -1
}

// canonicals folds Kind into the compared string so that identical text carried by two different
// protocols is not treated as a match.
func canonicals(effects []effect.Effect) []string {
	out := make([]string, len(effects))
	for index, item := range effects {
		out[index] = string(item.Kind) + " " + item.Canonical
	}

	return out
}

func at(values []string, index int) string {
	if index >= len(values) {
		return ""
	}

	return values[index]
}
