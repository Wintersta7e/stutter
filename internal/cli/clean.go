package cli

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/Wintersta7e/stutter/internal/provision"
)

// cleanSettings are clean's flags.
type cleanSettings struct {
	check  string
	dryRun bool
}

func newCleanFlagSet(parsed *cleanSettings, stderr io.Writer) *flag.FlagSet {
	set := flag.NewFlagSet(commandClean, flag.ContinueOnError)
	set.SetOutput(stderr)
	set.Usage = func() {}

	set.StringVar(&parsed.check, "check", "", "only this check's resources, and those carrying its ID alone")
	set.BoolVar(&parsed.dryRun, "dry-run", false, "list what would be removed and remove nothing")

	return set
}

// cleanCommand removes what dead checks on this engine and host left behind, listing each resource on
// stdout. It succeeds when every removal it attempted succeeded, or when it removed nothing on purpose.
func cleanCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	var parsed cleanSettings

	set := newCleanFlagSet(&parsed, stderr)
	if err := set.Parse(args); err != nil {
		fmt.Fprintf(stderr, "stutter clean: %v\n", err)

		return exitUsage
	}

	if set.NArg() > 0 {
		fmt.Fprintf(stderr, "stutter clean: %v: %v\n", errPositional, set.Args())

		return exitUsage
	}

	result, err := provision.Clean(
		ctx,
		provision.CleanOptions{Stdout: stdout, CheckID: parsed.check, DryRun: parsed.dryRun},
	)
	if err != nil {
		fmt.Fprintf(stderr, "stutter clean: %v\n", err)

		return exitUsage
	}

	if len(result.Failed) > 0 && !parsed.dryRun {
		return exitUsage
	}

	return 0
}
