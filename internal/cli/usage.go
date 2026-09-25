package cli

import (
	"strconv"
	"strings"
)

// The command line's user-visible sentences, gathered in one file so they can be reviewed and
// reworded together. Tests assert the facts each one must state, never its wording.

// keptText follows the kept check's ID: what was kept, and the one command that removes it.
func keptText(checkID string) string {
	return "the check's resources are kept, stopped, until `stutter clean --check " + checkID + "` removes them"
}

// interruptText is printed once, at the first interrupt.
const interruptText = "teardown under way; a second interrupt exits at once and leaves the rest for `stutter clean`"

// Why a discovered consumer was not checked, or where its check stopped.
const (
	notSelectedReason     = "not named by --consumer"
	elsewhereReason       = "it consumes a stream other than --stream: named, never checked"
	unstableReason        = "its name does not survive a start of the service, so no run can find it by name"
	nothingAdmittedReason = "its filter subjects admit no corpus message, so it was never run"
	interruptedBefore     = "interrupted before its check started; no verdict"
	interruptedDuring     = "interrupted during its check; no verdict"
)

// fillCollisionReason names the two corpus files the bus took for one message.
func fillCollisionReason(file, other string) string {
	if other == "" {
		return "corpus file " + file + " landed on a message already in the stream; no verdict"
	}

	return "corpus files " + file + " and " + other + " were taken by the bus for one message; no verdict"
}

// unnamedReason is why a service with no consumer gets one unnamed check, and what the bus refused it.
func unnamedReason(refusals []string, closedAfterInfo int) string {
	reason := "no consumer was discovered on any stream"
	if len(refusals) > 0 {
		reason += "; the bus refused " + strings.Join(refusals, ", ")
	}

	return reason + "; " + strconv.Itoa(closedAfterInfo) + " bus connections closed after the greeting without a byte"
}

// interruptedSetup names an interrupt that came before any consumer check could run.
const interruptedSetup = "interrupted before the first consumer check"

// nothingToCheck names both paths a check can take.
const nothingToCheck = "name --compose <file> --service <name> --stream <name> --corpus <dir> to check your " +
	"own service, or --postgres <dsn> to check the built-in reference consumer"

// gateInjectsNothing follows gate's refusal of --max-runs.
const gateInjectsNothing = "gate injects nothing, so it has no run budget"
