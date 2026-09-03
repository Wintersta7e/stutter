// Package cli implements the stutter command line.
//
// It lives here rather than in cmd/ so the commands can be tested by calling them: exit codes and
// rendered output are the contract, and both are checkable without spawning a process.
package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/version"
)

// exitUsage mirrors report's setup-error code: a command that could not be run at all did not
// produce a verdict, and must not be mistaken for one.
const exitUsage = 3

const (
	defaultMaxRuns  = 3
	defaultConsumer = "reserve_stock"
	hashKeyLen      = 32
	// referenceAckWait is short so a delay fault crosses it in seconds rather than minutes.
	referenceAckWait = time.Second
	referenceRetries = 6
)

const usage = `stutter — delivery-fault testing for message-bus consumers.

Usage:
  stutter check --postgres <dsn>   Replay with every legal fault and report what diverged
  stutter gate  --postgres <dsn>   Run only the gates: is this service stable enough to test?
  stutter version                  Print the build identity

Flags for check and gate:
  --postgres <dsn>    Reachable Postgres connection string. Stutter replays into it and wipes
                      its own fixture rows between runs, so point it at a scratch database.
  --max-runs <n>      Cap mutated runs (default 3; 0 means every legal message-and-fault pair)
  --consumer <name>   Name that findings attribute to (default reserve_stock)

Exit codes:
  0  every consumer passed and the gates held
  1  at least one failure
  2  a gate was violated, so no findings were computed — this is not a test failure
  3  setup error, or the command could not be run

The only service stutter can provision today is its own reference consumer: one non-idempotent
handler, one idempotent control, and one guarded control. Running it against your own service needs
compose provisioning, which is not built yet.
`

// Run executes one command and returns the process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)

		return exitUsage
	}

	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version.String())

		return 0
	case "check":
		return runCheck(ctx, args[1:], stdout, stderr, command{name: "check"})
	case "gate":
		return runCheck(ctx, args[1:], stdout, stderr, command{name: "gate", gatesOnly: true})
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)

		return 0
	default:
		fmt.Fprintf(stderr, "stutter: unknown command %q\n\n%s", args[0], usage)

		return exitUsage
	}
}

type settings struct {
	postgres string
	consumer string
	maxRuns  int
}

func parse(args []string, stderr io.Writer, name string) (settings, error) {
	set := flag.NewFlagSet(name, flag.ContinueOnError)
	set.SetOutput(stderr)

	var parsed settings

	set.StringVar(&parsed.postgres, "postgres", "", "reachable Postgres connection string")
	set.StringVar(&parsed.consumer, "consumer", defaultConsumer, "name findings attribute to")
	set.IntVar(&parsed.maxRuns, "max-runs", defaultMaxRuns, "cap on mutated runs, 0 for no cap")

	if err := set.Parse(args); err != nil {
		return settings{}, fmt.Errorf("parse %s flags: %w", name, err)
	}

	if parsed.postgres == "" {
		return settings{}, errNoDatabase
	}

	return parsed, nil
}

// command distinguishes check from gate. It is a value rather than a bool parameter so the call
// sites read as what they are instead of as true and false.
type command struct {
	name      string
	gatesOnly bool
}

func runCheck(ctx context.Context, args []string, stdout, stderr io.Writer, run command) int {
	parsed, err := parse(args, stderr, run.name)
	if err != nil {
		fmt.Fprintf(stderr, "stutter %s: %v\n", run.name, err)

		return exitUsage
	}

	result, err := execute(ctx, parsed, run.gatesOnly)
	if err != nil {
		fmt.Fprintf(stderr, "stutter %s: %v\n", run.name, err)

		return exitUsage
	}

	if err := result.Render(stdout); err != nil {
		fmt.Fprintf(stderr, "stutter %s: write the report: %v\n", run.name, err)

		return exitUsage
	}

	return result.ExitCode()
}

// referenceConfig is the delivery contract the reference consumer is checked under. A real run
// reads this from the recorded consumer instead.
func referenceConfig() policy.Config {
	return policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{corpus.SubjectPrefix + ">"},
		AckWait:        referenceAckWait,
		MaxDeliver:     referenceRetries,
		MaxAckPending:  1,
	}
}
