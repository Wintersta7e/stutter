package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// tree snapshots or compares the working tree, read from git status --porcelain=v1 -z on stdin,
// relative to the current directory: the repository root.
func tree(args []string, s streams) int {
	flags := flag.NewFlagSet("tree", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	snapshot := flags.String("snapshot", "", "save the tree to this file, before the suite")
	compare := flags.String("compare", "", "compare the tree with the snapshot in this file, after the suite")

	if err := flags.Parse(args); err != nil {
		return exitUsage
	}

	if (*snapshot == "") == (*compare == "") {
		fmt.Fprintln(s.stderr, "tree: exactly one of -snapshot and -compare")

		return exitUsage
	}

	now, err := testgate.ReadTree(s.stdin, ".")
	if err != nil {
		fmt.Fprintf(s.stderr, "tree: %v\n", err)

		return exitUsage
	}

	if *snapshot != "" {
		return saveTree(now, *snapshot, s)
	}

	return compareTree(now, *compare, s)
}

// saveTree writes the snapshot.
func saveTree(now testgate.Tree, path string, s streams) int {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(s.stderr, "tree: %v\n", err)

		return exitUsage
	}

	if err := now.Save(f); err != nil {
		_ = f.Close()

		fmt.Fprintf(s.stderr, "tree: %v\n", err)

		return exitUsage
	}

	if err := f.Close(); err != nil {
		fmt.Fprintf(s.stderr, "tree: %v\n", err)

		return exitUsage
	}

	return exitPass
}

// compareTree prints the tree line and every path the suite added or changed; any fails.
func compareTree(now testgate.Tree, path string, s streams) int {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(s.stderr, "tree: FAIL — no snapshot to compare with, absent: %v\n", err)

		return exitFail
	}

	defer func() { _ = f.Close() }()

	before, err := testgate.LoadTree(f)
	if err != nil {
		fmt.Fprintf(s.stderr, "tree: FAIL — %v\n", err)

		return exitFail
	}

	changed := now.Changed(before)

	line, err := testgate.FormatAudit(testgate.AuditTree, "", len(changed))
	if err != nil {
		fmt.Fprintf(s.stderr, "tree: %v\n", err)

		return exitUsage
	}

	fmt.Fprintln(s.stdout, line)

	for _, p := range changed {
		fmt.Fprintf(s.stdout, "  %s\n", p)
	}

	if len(changed) > 0 {
		fmt.Fprintln(s.stderr, "tree: FAIL — the test suite left the working tree changed")

		return exitFail
	}

	return exitPass
}

// nocontainers fails when a module file names the container library.
func nocontainers(args []string, s streams) int {
	files, err := readFiles(args)
	if err != nil {
		fmt.Fprintf(s.stderr, "nocontainers: %v\n", err)

		return exitUsage
	}

	n, err := testgate.ScanNoContainers(files)

	fmt.Fprintf(s.stdout, "testcontainers files=%d occurrences=%d\n", len(files), n)

	if err != nil {
		fmt.Fprintf(s.stderr, "nocontainers: FAIL — %v\n", err)

		return exitFail
	}

	return exitPass
}
