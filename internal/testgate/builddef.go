// Package testgate holds the filters CI and the local gate run over the repository and over the test
// suite's own output: pure functions of their input, so every one of them is tested like any code.
package testgate

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
)

// binaryProbe is how much of a file is read for a NUL byte, the sign it is not text.
const binaryProbe = 8 << 10

// makeRecipe is the Makefile target whose recipe is the one build of the product.
const makeRecipe = "build"

var (
	// buildPattern matches an invocation of the Go toolchain's build or install subcommand, spelled
	// with \s+ so this source never holds the phrase it looks for.
	buildPattern = regexp.MustCompile(`(?:^|[\s;&|("'` + "`" + `])(?:go|\$\(GO\))\s+(build|install)(?:\s|$)`)
	// compileCheckPattern matches an output that writes nothing.
	compileCheckPattern = regexp.MustCompile(`(?:^|\s)-o(?:\s+|=)/dev/null(?:\s|$)`)
	// targetPattern matches a Makefile rule line and captures its target; an assignment is not one.
	targetPattern = regexp.MustCompile(`^([A-Za-z0-9_./-]+)\s*:(?:[^=]|$)`)
	// cgoPattern matches the cgo switch.
	cgoPattern = regexp.MustCompile(`\bCGO_ENABLED\b`)
	// cgoDefaultPattern matches a Makefile assignment or export of the build's CGO parameter.
	cgoDefaultPattern = regexp.MustCompile(`^\s*(export\s+|override\s+)?CGO\s*(?:(\?=|::?=|\+=|!=|=)\s*(\S*)\s*)?$`)
	// makeCgoPattern matches CGO passed to make on its command line, which overrides the default.
	makeCgoPattern = regexp.MustCompile(`(?:\bmake|\$\(MAKE\))\s[^|;&]*\bCGO=(\S*)`)
	// makeEnvPattern matches make told to let the environment override the Makefile, spelled so this
	// source never holds the words it looks for.
	makeEnvPattern = regexp.MustCompile(`(?:(?:\bmake|\$\(MAKE\))(?:\s[^|;&]*)?\s-e(?:\s|$)|\bMAKE(?:FLAGS)\b)`)
)

// errBuilds means the tree does not hold exactly one build of the product, in the Makefile's build
// recipe, with cgo set on that recipe line only.
var errBuilds = errors.New("build definitions")

// SourceFile is one file a scan reads.
type SourceFile struct {
	Path    string
	Content []byte
}

// BuildMatch is one line a scan matched.
type BuildMatch struct {
	Path string
	Text string
	Line int
}

// BuildScan is what ScanBuilds found.
type BuildScan struct {
	// CgoDefault is the value the Makefile assigns the build's CGO parameter; empty when none.
	CgoDefault string
	// Found holds every line that builds the product: a build or install that writes a file.
	Found []BuildMatch
	// CgoAssignments holds every cgo setting that could make the build dynamic: CGO_ENABLED outside
	// the build recipe's own line, a CGO default other than exactly `CGO = 0`, CGO passed to make on
	// its command line, and make told to let the environment win.
	CgoAssignments []BuildMatch
	// Files is the number of files scanned.
	Files int
	// recipes is how many of Found sit in the Makefile's build recipe.
	recipes int
}

// ScanBuilds finds every build of the product in files. A test file, a binary file and a comment
// line are never read as one.
func ScanBuilds(files []SourceFile) BuildScan {
	var scan BuildScan

	for _, f := range files {
		if strings.HasSuffix(f.Path, "_test.go") ||
			bytes.IndexByte(f.Content[:min(len(f.Content), binaryProbe)], 0) >= 0 {
			continue
		}

		scan.Files++
		scanFile(&scan, f)
	}

	return scan
}

// line is one non-comment line of a scanned file, with what its file makes of it.
type line struct {
	path     string
	text     string
	target   string
	number   int
	makefile bool
	workflow bool
	recipe   bool
}

// scanFile adds one file's matches to s.
func scanFile(s *BuildScan, f SourceFile) {
	makefile := path.Base(f.Path) == "Makefile"
	workflow := strings.HasPrefix(f.Path, ".github/")
	target := ""

	for n, text := range strings.Split(string(f.Content), "\n") {
		trimmed := strings.TrimSpace(text)
		if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "//") {
			continue
		}

		recipe := makefile && strings.HasPrefix(text, "\t")
		if m := targetPattern.FindStringSubmatch(text); makefile && !recipe && m != nil {
			target = m[1]
		}

		addLine(s, line{
			path: f.Path, text: trimmed, target: target, number: n + 1,
			makefile: makefile, workflow: workflow, recipe: recipe,
		})
	}
}

// addLine adds one line's matches to s.
func addLine(s *BuildScan, l line) {
	match := BuildMatch{Path: l.path, Line: l.number, Text: l.text}

	if writesProduct(l.text) {
		s.Found = append(s.Found, match)

		if l.recipe && l.target == makeRecipe {
			s.recipes++
		}
	}

	if dynamicCgo(l) {
		s.CgoAssignments = append(s.CgoAssignments, match)
	}

	if m := cgoDefaultPattern.FindStringSubmatch(l.text); m != nil && l.makefile && !l.recipe {
		s.CgoDefault = m[3]

		// Only a plain assignment of 0 holds: `?=` yields to the environment, an export reaches every
		// recipe, and any other value builds with cgo.
		if m[1] != "" || m[2] != "=" || m[3] != "0" {
			s.CgoAssignments = append(s.CgoAssignments, match)
		}
	}
}

// dynamicCgo reports whether l sets CGO_ENABLED anywhere but the build recipe's own line, runs make
// with a CGO other than 0 on its command line, or tells make to let the environment win.
func dynamicCgo(l line) bool {
	if cgoPattern.MatchString(l.text) && (l.workflow || l.makefile && !l.recipe) {
		return true
	}

	if m := makeCgoPattern.FindStringSubmatch(l.text); m != nil && m[1] != "0" {
		return true
	}

	return makeEnvPattern.MatchString(l.text)
}

// writesProduct reports whether text builds or installs something that writes a file: a build whose
// output is not /dev/null, or an install that is not an external module at a version.
func writesProduct(text string) bool {
	m := buildPattern.FindStringSubmatch(text)
	if m == nil {
		return false
	}

	if m[1] == "install" {
		return !strings.Contains(text, "@")
	}

	return !compileCheckPattern.MatchString(text)
}

// Err reports why the scan fails, or nil when it found exactly one build, the Makefile's build
// recipe, and no cgo setting beside that recipe line.
func (s BuildScan) Err() error {
	var problems []string

	if s.Files == 0 {
		problems = append(problems, "no files were scanned")
	}

	if len(s.Found) != 1 || s.recipes != 1 {
		problems = append(problems, fmt.Sprintf("want exactly one build, the Makefile's %s recipe; found %d",
			makeRecipe, len(s.Found)))
	}

	if s.CgoDefault != "0" {
		problems = append(problems, fmt.Sprintf("cgo-default=%q: the Makefile must assign exactly CGO = 0",
			s.CgoDefault))
	}

	if len(s.CgoAssignments) > 0 {
		problems = append(problems, fmt.Sprintf("%d cgo settings that could make the build dynamic",
			len(s.CgoAssignments)))
	}

	if problems == nil {
		return nil
	}

	return fmt.Errorf("%w: %s", errBuilds, strings.Join(problems, "; "))
}
