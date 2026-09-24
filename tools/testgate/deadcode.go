package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// deadcode compares deadcode -json on stdin with the baseline file named in args.
func deadcode(args []string, s streams) int {
	if len(args) != 1 {
		fmt.Fprintln(s.stderr, "usage: deadcode -json ./cmd/stutter | testgate deadcode <baseline file>")

		return exitUsage
	}

	baseline, err := os.ReadFile(args[0]) //nolint:gosec // the baseline the gate names, in the repository
	if err != nil {
		fmt.Fprintf(s.stderr, "deadcode: reading the baseline: %v\n", err)

		return exitUsage
	}

	d, err := testgate.CompareDeadcode(s.stdin, strings.Split(string(baseline), "\n"))
	if err != nil {
		fmt.Fprintf(s.stderr, "deadcode: %v\n", err)

		return exitUsage
	}

	// The baseline holds every reported name but the new ones, and the gone ones besides.
	fmt.Fprintf(s.stdout, "deadcode reported=%d baseline=%d\n", len(d.Reported), len(d.Reported)-len(d.New)+len(d.Gone))

	for _, name := range d.Reported {
		fmt.Fprintf(s.stdout, "unreachable %s\n", name)
	}

	for _, name := range d.New {
		fmt.Fprintf(s.stdout, "new %s\n", name)
	}

	for _, name := range d.Gone {
		fmt.Fprintf(s.stdout, "gone %s\n", name)
	}

	if len(d.New)+len(d.Gone) > 0 {
		fmt.Fprintln(s.stderr, "deadcode: FAIL — the functions unreachable from the CLI differ from the baseline: "+
			"wire or delete a new one, or name it in the baseline with the caller that will use it; "+
			"remove a gone one from the baseline")

		return exitFail
	}

	return exitPass
}
