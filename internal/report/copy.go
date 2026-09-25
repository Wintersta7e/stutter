package report

import (
	"strconv"

	"github.com/Wintersta7e/stutter/internal/gate"
)

// The compose path's user-visible sentences, gathered in one file so they can be reviewed and reworded
// together. Tests assert the facts each one must state, never its wording.

// siblingReservation holds back a finding because another consumer of the same service was unstable:
// it names that consumer and how its runs differed.
func siblingReservation(consumer string, class gate.Class) string {
	return "held back: consumer " + consumer + " did not behave the same way twice (" + string(class) +
		"), so the service may be unstable under this consumer too"
}

// setupAfterRuns closes a setup error that struck after runs completed: something was replayed, and
// still no verdict stands.
func setupAfterRuns(completed int) string {
	return plural(completed, "run") + " completed before the setup failed; no verdict was reached, " +
		"so no findings were computed."
}

// Compose-path notes. The reference path's notes advise declaring an invariant or overriding a stub,
// which only a Go caller can do; each of these states the same observation with what a compose user
// can do instead.
const (
	composeMailNote = "a repeated mail submission is annoying rather than corrupting; " +
		"invariants that silence or promote it are declared from Go only"
	composeEgressNote = "a repeated outbound call may be a charge or may be harmless and Stutter cannot " +
		"tell which; invariants that promote or silence it are declared from Go only"
	composeReadNote = "the only difference is a read — a guard looking again changes nothing unless the " +
		"read calls a function that writes; invariants that promote or silence it are declared from Go only"
	composeUnclassifiedNote = "may be acceptable; invariants that silence it are declared from Go only"
	// composeGuardOverrideNote names the flag that gives a stubbed endpoint a firm reply.
	composeGuardOverrideNote = "a stubbed reply may have decided this — give that endpoint a reply with " +
		"--routes to get a firm verdict"
	// composeGuardNextStep closes the guard-dependence note on the compose path.
	composeGuardNextStep = "divergence was acceptable. Give the stub named above a reply with --routes " +
		"to get a firm verdict."
)

// bucketWord is the word a consumer's block opens with.
func bucketWord(bucket Bucket) string {
	switch bucket {
	case BucketNotSelected:
		return "NOT SELECTED"
	case BucketNotCovered:
		return "NOT COVERED"
	case BucketSetup:
		return "SETUP"
	case BucketGate:
		return "GATE"
	case BucketFail:
		return string(StatusFail)
	case BucketWarn:
		return string(StatusWarn)
	case BucketHeld:
		return string(StatusHeld)
	case BucketPass:
		return string(StatusPass)
	default:
		return string(bucket)
	}
}

// admittedText is how many corpus messages a consumer's filters admit, and where the faults it is
// checked under come from: its configuration as discovered from the running service.
func admittedText(admitted, total int) string {
	return strconv.Itoa(admitted) + " of " + plural(total, "corpus message") +
		" admitted; faults licensed by its discovered configuration"
}

// notJudgedLabel opens the line naming the messages that produced no effect on the clean run.
const notJudgedLabel = "NOT JUDGED"

// notJudgedText says why those messages count toward nothing.
func notJudgedText(sequences string) string {
	return sequences + ": no effect on the clean run, so no fault aimed at them was judged"
}

// coverageLabel opens every coverage line.
const coverageLabel = "coverage  "

// legalCoverage is one legal fault's pairs, each spent exactly one way.
func legalCoverage(fault string, line Coverage) string {
	return fault + ": legal, " + plural(line.Pairs, "pair") + " — " + strconv.Itoa(line.Attempted) +
		" attempted, " + strconv.Itoa(line.Unexpressed) + " unexpressed, " + strconv.Itoa(line.CutByBudget) +
		" cut by the run budget"
}

// illegalCoverage is a fault the consumer's configuration refuses, and the clause that refuses it.
func illegalCoverage(fault, clause string) string {
	return fault + ": not legal — " + clause
}

// noLegalFault closes the coverage of a consumer whose configuration licenses no fault.
const noLegalFault = "0 legal faults: nothing was injected against this consumer, so this is not a pass"

// Header row labels. Every row of a compose report's header opens with its label, so a reader and a
// test alike can find and count the rows.
const (
	rowInputs       = "Compose:"
	rowDeclarations = "Declared:"
	rowEngine       = "Engine:"
	rowImage        = "Image:"
	rowHostMode     = "Host mode:"
	rowDependencies = "Dependencies:"
	rowSeeds        = "Seeds:"
	rowSpec         = "Spec changes:"
	rowCorpus       = "Corpus:"
	rowHosts        = "External hosts:"
	rowDisclosure   = "Not observed:"
	rowReach        = "Reach:"
	rowSetupEgress  = "Setup egress:"
)

// halfCloseLimit is disclosed beside host-alias mode, where it applies.
const halfCloseLimit = "a reply sent after the client's half-close is lost on this engine's host path; " +
	"it can hide a finding in an opaque dependency and never make one"

// The disclosure block's fixed sentences: what a compose check never observes.
const (
	disclosureUDP      = "UDP traffic: never proxied, so never compared"
	disclosureLoopback = "the service's own loopback: traffic inside its container is never seen"
	disclosureLiterals = "hosts named by IP literal or loopback bypass DNS and reach no proxy"
)

// reservedPorts discloses the stub relay's own source ports, which the catch-all never answers.
func reservedPorts(first, last uint16) string {
	return "ports " + strconv.Itoa(int(first)) + "–" + strconv.Itoa(int(last)) +
		" of external hosts: the stub relay's own source ports, never answered"
}

// reachSentence states that the proxies are reachable from every container on the engine while a
// check runs, and that only the check's own connections are served.
const reachSentence = "Stutter's proxies are reachable from every container on the engine during a check; " +
	"a connection without this check's token is refused and counted"

// setupEgressSentence discloses that setup is not observed.
const setupEgressSentence = "jobs and dependencies may reach the network during setup, unobserved"
