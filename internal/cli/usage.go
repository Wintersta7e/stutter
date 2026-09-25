package cli

import (
	"flag"
	"io"
	"strconv"
	"strings"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
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

// usage is the help text. Every default and every limit it states is read from the constant that owns
// it, so the help cannot drift from what the code does.
func usage() string {
	var text strings.Builder

	text.WriteString(usageCommands)
	text.WriteString("\nFlags for check and gate (--max-runs is check's alone):\n")
	writeFlags(&text, newFlagSet(commandCheck, &settings{}, io.Discard))
	text.WriteString("\nFlags for clean:\n")
	writeFlags(&text, newCleanFlagSet(&cleanSettings{}, io.Discard))

	variables := compose.CAVariables()

	text.WriteString(usageSetupCost(strings.Join(variables[:], ", ")))
	text.WriteString(usageTouches)
	text.WriteString(usageConsumers(harness.DeliveryCap))
	text.WriteString(usageFaults)
	text.WriteString(usageExits)
	text.WriteString(usageRequirements(compose.MinVersion))

	return text.String()
}

// flagColumn is the width of a flag's name and placeholder in the help.
const flagColumn = 22

// writeFlags lists every flag the set defines, with what it takes and its default.
func writeFlags(text *strings.Builder, set *flag.FlagSet) {
	set.VisitAll(func(f *flag.Flag) {
		name := "--" + f.Name
		if placeholder := flagPlaceholder(f.Name); placeholder != "" {
			name += " " + placeholder
		}

		line := "  " + name + strings.Repeat(" ", max(flagColumn-len(name), 1)) + f.Usage
		if owned := flagDefault(f.Name); owned != "" {
			line += "\n" + strings.Repeat(" ", flagColumn+len("  ")) + "(" + owned + ")"
		}

		text.WriteString(line + "\n")
	})
}

// flagPlaceholder is what a flag takes; a switch takes nothing.
func flagPlaceholder(name string) string {
	switch name {
	case "compose", flagRoutes:
		return "<file>"
	case flagService, flagStream, flagProfile, flagConsumer:
		return "<name>"
	case flagCorpus:
		return "<dir>"
	case flagQuiesce, flagStartup, flagDrain:
		return "<duration>"
	case flagMaxRuns:
		return "<n>"
	case "postgres":
		return "<dsn>"
	case "check":
		return "<id>"
	default:
		return ""
	}
}

// flagDefault is a flag's default as its owner states it.
func flagDefault(name string) string {
	switch name {
	case flagMaxRuns:
		return "default " + strconv.Itoa(defaultMaxRuns) + " per consumer check; 0 tries every legal pair"
	case flagQuiesce:
		return "default " + replay.DefaultQuiesce.String() + " on the compose path, " + toy.DefaultQuiesce.String() +
			" on the reference path"
	case flagStartup:
		return "default " + harness.DefaultStartup.String()
	case flagDrain:
		return "default: each consumer's redelivery floor, which it may only lengthen"
	default:
		return ""
	}
}

const usageCommands = `stutter — delivery-fault testing for message-bus consumers.

Usage:
  stutter check --compose <file> --service <name> --stream <name> --corpus <dir>
      Check your own compose service: replay the corpus under every fault its consumers'
      own configuration licenses, and report every consumer whose work diverges.
  stutter gate --compose <file> --service <name> --stream <name> --corpus <dir>
      The same inputs, gates only: is this service stable enough to test? gate injects
      nothing, so it never reports a pass.
  stutter check --postgres <dsn>
  stutter gate --postgres <dsn>
      The built-in reference consumer, replayed into a Postgres you name. Stutter wipes its
      own fixture rows between runs, so point it at a scratch database.
  stutter clean [--check <id>] [--dry-run]
      Remove what a check left behind: after --keep, or a check that could not tear down.
  stutter version
  stutter help
`

// usageSetupCost is what a service must already do for a check to need no written declaration.
func usageSetupCost(caVariables string) string {
	return `
What your service needs for zero written declaration:
  The command line names four things: --compose, --service, --stream, --corpus. Nothing else is
  written, provided the compose file already runs the service on its own and the service:
    1. runs its migrations as a service_completed_successfully job or through its image's init
       scripts;
    2. makes external calls only over HTTP/1.1, and over TLS only from runtimes that honour
       the CA variables Stutter sets:
       ` + caVariables + `;
    3. does no work on its own timer while a run is in progress;
    4. does not require TLS to Postgres (sslmode=require or stricter);
    5. mounts no engine socket and uses no privileged, devices or volumes_from;
    6. has one bus identity.
  You write the corpus by hand: one file per message, in the service's own wire encoding,
  named by its order and subject, with an optional .headers sidecar.
  EVERYTHING ELSE TAKES A HAND-WRITTEN COMPOSE OVERRIDE, passed as one more --compose file: a
  migration run by hand becomes a compose job, a JVM or libcurl service points its own trust
  store at the CA file, a timer is disabled, a misclassified dependency gets an x-stutter role.
  The one third-party service Stutter has been run against needed a two-line override.
  --routes, --consumer and --profile are optional; there is no --seed.
`
}

const usageTouches = `
What a compose check touches:
  It parses the whole compose project and reads every service definition, other services'
  environment values included, printing none. It builds and pulls images with your
  credentials; build steps carry your own build's host access, and every build leaves BuildKit
  cache and build-history records. It runs your code — the service, its jobs and its
  dependencies — in containers it creates, and jobs and dependencies may reach the network
  during setup, unobserved. On the engine your Docker CLI resolves it creates, commits and
  removes containers, networks, volumes and images, for every started dependency, every run.
  It removes only what its per-check ledger proves it created: never by filter, label or prune,
  never with a compose lifecycle command. It never mounts a volume it did not create, reads bind
  sources and never writes them, never attaches to your networks, never moves your image tags,
  leaves the images it pulled, and issues no SQL of its own. Log folders are kept after a gate
  violation, a setup error or an interrupt, and --keep keeps everything, snapshot images holding
  your compose environment included, until stutter clean.
`

// usageConsumers is how the consumers of one service are checked.
func usageConsumers(deliveryCap int) string {
	return `
Consumers:
  Every consumer the service creates on --stream is discovered and checked one at a time, the
  others paused; --consumer narrows the check to the ones it names.
  A run delivers one message at most ` + strconv.Itoa(deliveryCap) + ` times: the first delivery, the crash loop's
  withheld ones, and one retry of the service's own.
`
}

const usageFaults = `
Faults:
  Against a compose service Stutter injects duplicate and crash_before_ack — a crash loop that
  withholds the acknowledgement twice, not a duplicate by another name — each only where the
  consumer's discovered configuration licenses it. delay and reorder need Stutter to hand over
  the message itself, so only the reference path injects them.
`

const usageExits = `
Exit codes:
  0  every consumer passed or held (gate: the gates held; gate never reports a pass)
  1  at least one consumer failed
  2  a gate was violated, so no findings were computed for it — this is not a test failure
  3  setup error, or the command could not be run
  Consumers combine failure first: any failure exits 1, whatever the others did; then any setup
  error exits 3, then any gate violation 2. A check that judged no consumer at all exits 3, so a
  service none of whose consumers can be checked must be left out of CI deliberately.
`

// usageRequirements is where a compose check runs.
func usageRequirements(composeFloor string) string {
	return `
Requirements:
  --compose runs on Linux hosts, WSL2 included, only: darwin and native Windows builds run the
  reference path and refuse --compose. It needs Docker Engine (Docker Desktop or dockerd) on a
  local socket, with Compose ` + composeFloor + ` or later; Podman, remote, rootless and mirrored-WSL
  engines are refused, and so is running Stutter inside a container. The binary is a static
  build (CGO_ENABLED=0). The test suite and its gate need a reachable engine unless
  STUTTER_TEST_DOCKER says to skip it.
`
}
