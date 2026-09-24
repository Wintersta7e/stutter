// Command testgate runs the repository's own gates over what CI and the local gate feed it: the
// file list, the test suite's JSON, the package list. Each subcommand prints what it scanned and
// exits 1 when its gate fails.
//
//	git ls-files -z | testgate builddef [extra paths...]
//	go test -json ./... | testgate count [-docker=suite|excluded]
//	go test -json -run ... | testgate expect -test T -action pass|fail|skip [-reason P]
//	go list -test -json ./... | testgate flakeset
//	deadcode -json ./cmd/stutter | testgate deadcode <baseline file>
//	git ls-files -z '*.go' | testgate localproofs
//	git status --porcelain=v1 -z --untracked-files=all | testgate tree -snapshot|-compare <file>
//	testgate nocontainers go.mod go.sum
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// Exit codes: a gate that fails is 1; a command that could not run is 2.
const (
	exitPass  = 0
	exitFail  = 1
	exitUsage = 2
)

// streams is a run's input, output and environment.
type streams struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	getenv func(string) string
}

func main() {
	os.Exit(run(os.Args[1:], streams{stdin: os.Stdin, stdout: os.Stdout, stderr: os.Stderr, getenv: os.Getenv}))
}

// run dispatches one subcommand and returns the process's exit code.
func run(args []string, s streams) int {
	if len(args) == 0 {
		fmt.Fprintln(
			s.stderr,
			"usage: testgate builddef|count|expect|flakeset|deadcode|localproofs|tree|nocontainers ...",
		)

		return exitUsage
	}

	switch args[0] {
	case "builddef":
		return builddef(args[1:], s)
	case "count":
		return count(args[1:], s)
	case "expect":
		return expect(args[1:], s)
	case "flakeset":
		return flakeset(s)
	case "deadcode":
		return deadcode(args[1:], s)
	case "localproofs":
		return localproofs(s)
	case "tree":
		return tree(args[1:], s)
	case "nocontainers":
		return nocontainers(args[1:], s)
	default:
		fmt.Fprintf(s.stderr, "testgate: unknown subcommand %q\n", args[0])

		return exitUsage
	}
}

// builddef scans the NUL-separated paths on stdin, plus extra, for builds of the product.
func builddef(extra []string, s streams) int {
	stdin, stdout, stderr := s.stdin, s.stdout, s.stderr

	paths, err := nulSeparated(stdin)
	if err != nil {
		fmt.Fprintf(stderr, "builddef: reading the file list: %v\n", err)

		return exitUsage
	}

	files, err := readFiles(append(paths, extra...))
	if err != nil {
		fmt.Fprintf(stderr, "builddef: %v\n", err)

		return exitUsage
	}

	scan := testgate.ScanBuilds(files)

	fmt.Fprintf(stdout, "build-definitions files=%d found=%d cgo-assignments=%d cgo-default=%s\n", scan.Files,
		len(scan.Found), len(scan.CgoAssignments), scan.CgoDefault)

	for _, m := range scan.Found {
		fmt.Fprintf(stdout, "%s:%d: %s\n", m.Path, m.Line, m.Text)
	}

	for _, m := range scan.CgoAssignments {
		fmt.Fprintf(stdout, "cgo-assignment %s:%d: %s\n", m.Path, m.Line, m.Text)
	}

	if err := scan.Err(); err != nil {
		fmt.Fprintf(stderr, "builddef: FAIL — %v\n", err)

		return exitFail
	}

	return exitPass
}

// nulSeparated reads a NUL-separated list, as git ls-files -z prints it.
func nulSeparated(r io.Reader) ([]string, error) {
	var out []string

	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if i := bytes.IndexByte(data, 0); i >= 0 {
			return i + 1, data[:i], nil
		}

		if atEOF && len(data) > 0 {
			return len(data), data, nil
		}

		return 0, nil, nil
	})

	for scanner.Scan() {
		if entry := strings.TrimSpace(scanner.Text()); entry != "" {
			out = append(out, entry)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("splitting: %w", err)
	}

	return out, nil
}

// readFiles reads every path.
func readFiles(paths []string) ([]testgate.SourceFile, error) {
	files := make([]testgate.SourceFile, 0, len(paths))

	for _, p := range paths {
		content, err := os.ReadFile(p) //nolint:gosec // the repository's own file list, or the gate's arguments
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", p, err)
		}

		files = append(files, testgate.SourceFile{Path: p, Content: content})
	}

	return files, nil
}
