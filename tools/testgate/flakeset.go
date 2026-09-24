package main

import (
	"fmt"
	"os"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// flakeset derives the flake job's packages from go list -test -json on stdin and prints one import
// path per line on stdout; the set sizes and every exclusion go to stderr.
func flakeset(s streams) int {
	flake, err := testgate.FlakeSet(s.stdin, os.ReadFile)

	fmt.Fprintf(s.stderr, "flake packages with-tests=%d postgres=%d docker=%d derived=%d\n", len(flake.WithTests),
		len(flake.Postgres), len(flake.Docker), len(flake.Derived))

	for _, p := range flake.Postgres {
		fmt.Fprintf(s.stderr, "excluded %s (postgres)\n", p)
	}

	for _, p := range flake.Docker {
		fmt.Fprintf(s.stderr, "excluded %s (docker)\n", p)
	}

	if err != nil {
		fmt.Fprintf(s.stderr, "flakeset: FAIL — %v\n", err)

		return exitFail
	}

	for _, p := range flake.Derived {
		fmt.Fprintln(s.stdout, p)
	}

	return exitPass
}
