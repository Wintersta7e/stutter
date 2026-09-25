package cli

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// ownedDefaults are the defaults help states, each as its owner renders it.
func ownedDefaults() map[string]string {
	return map[string]string{
		"max-runs":              strconv.Itoa(defaultMaxRuns),
		"compose quiesce":       replay.DefaultQuiesce.String(),
		"reference quiesce":     toy.DefaultQuiesce.String(),
		"startup":               harness.DefaultStartup.String(),
		"delivery cap":          strconv.Itoa(harness.DeliveryCap),
		"compose floor version": compose.MinVersion,
	}
}

// TestHelpReadsEveryDefaultFromItsOwner: help lists every command a user runs and every flag, each
// default read from the constant that owns it — never typed a second time, where it would drift.
func TestHelpReadsEveryDefaultFromItsOwner(t *testing.T) {
	t.Parallel()

	help := usage()

	for _, command := range []string{commandCheck, commandGate, commandClean, "version", "help"} {
		if !strings.Contains(help, "stutter "+command) {
			t.Errorf("help does not list stutter %s", command)
		}
	}

	if strings.Contains(help, "relay") {
		t.Error("help lists the hidden relay command")
	}

	flags := 0

	sets := []*flag.FlagSet{
		newFlagSet(commandCheck, &settings{}, io.Discard), newFlagSet(commandGate, &settings{}, io.Discard),
		newCleanFlagSet(&cleanSettings{}, io.Discard),
	}

	for _, set := range sets {
		set.VisitAll(func(f *flag.Flag) {
			flags++

			if !strings.Contains(help, "--"+f.Name) {
				t.Errorf("help does not list --%s", f.Name)
			}
		})
	}

	for owner, value := range ownedDefaults() {
		if !strings.Contains(help, value) {
			t.Errorf("help does not state the %s default %s", owner, value)
		}
	}

	literals := typedLiterals(t)
	for _, literal := range literals {
		for owner, value := range ownedDefaults() {
			typed := value
			if _, isNumber := strconv.Atoi(value); isNumber == nil {
				// A bare number is everywhere; the default it would drift from is stated beside a word.
				typed = "default " + value
			}

			if strings.Contains(literal, typed) {
				t.Errorf("usage.go types the %s default by hand: %q", owner, literal)
			}
		}
	}

	t.Logf("flags=%d literals=%d", flags, len(literals))

	if flags == 0 || len(literals) == 0 {
		t.Fatal("no flag or no string literal was checked")
	}
}

// typedLiterals are every string literal in usage.go.
func typedLiterals(t *testing.T) []string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), "usage.go", nil, 0)
	if err != nil {
		t.Fatalf("parse usage.go: %v", err)
	}

	var literals []string

	ast.Inspect(file, func(node ast.Node) bool {
		if literal, isLiteral := node.(*ast.BasicLit); isLiteral && literal.Kind == token.STRING {
			literals = append(literals, literal.Value)
		}

		return true
	})

	return literals
}
