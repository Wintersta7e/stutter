package main

import (
	"fmt"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// localProofRule is how a local-only proof is kept, printed with every listing.
const localProofRule = "built only with -tags localproof; run by hand on the engine it needs; " +
	"recorded in the guard ledger; CI never compiles them"

// localproofs lists the local-only proofs among the NUL-separated Go files on stdin.
func localproofs(s streams) int {
	paths, err := nulSeparated(s.stdin)
	if err != nil {
		fmt.Fprintf(s.stderr, "localproofs: reading the file list: %v\n", err)

		return exitUsage
	}

	files, err := readFiles(paths)
	if err != nil {
		fmt.Fprintf(s.stderr, "localproofs: %v\n", err)

		return exitUsage
	}

	proofs, err := testgate.LocalProofs(files)

	fmt.Fprintf(s.stdout, "local-only proofs files=%d\n", len(proofs))
	fmt.Fprintf(s.stdout, "rule: %s\n", localProofRule)

	for _, p := range proofs {
		fmt.Fprintln(s.stdout, p)
	}

	if err != nil {
		fmt.Fprintf(s.stderr, "localproofs: FAIL — %v\n", err)

		return exitFail
	}

	return exitPass
}
