package rules_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// labelValue is what the engine accepts as a label value Stutter writes and a filter can match
// without quoting: lowercase words joined by hyphens.
var labelValue = regexp.MustCompile(`^[a-z]+(-[a-z]+)*$`)

// The kind vocabulary is closed: every Kind constant the package declares is in Kinds(), and
// Kinds() holds nothing else. A kind declared but left out of Kinds() would be a label value the
// sweep and clean never recognise, so its resources could never be removed.
func TestTheKindVocabularyIsClosed(t *testing.T) {
	t.Parallel()

	declared := declaredKinds(t)
	kinds := rules.Kinds()
	t.Logf("kinds=%d declared=%d", len(kinds), len(declared))

	if len(kinds) != 13 {
		t.Fatalf("Kinds() holds %d kinds, want the 13 of the vocabulary", len(kinds))
	}

	seen := make(map[rules.Kind]bool, len(kinds))
	for _, kind := range kinds {
		if seen[kind] {
			t.Errorf("kind %q is listed twice", kind)
		}

		seen[kind] = true

		if !labelValue.MatchString(string(kind)) {
			t.Errorf("kind %q is not a plain label value", kind)
		}

		if !declared[string(kind)] {
			t.Errorf("kind %q is in Kinds() but declared by no constant", kind)
		}
	}

	for value := range declared {
		if !seen[rules.Kind(value)] {
			t.Errorf("constant kind %q is missing from Kinds()", value)
		}
	}

	if rules.KindTemplateVolume != "template-volume" {
		t.Errorf("KindTemplateVolume = %q, want the hyphenated template-volume", rules.KindTemplateVolume)
	}
}

// The `.test` key is the verification suite's; no product constant may ever be it, or a product
// resource would look like a test's and a test's cleanup would reach it.
func TestNoProductKeyIsTheTestKey(t *testing.T) {
	t.Parallel()

	keys := []string{rules.Namespace, rules.LabelCheck, rules.LabelKind, rules.LabelBuild, rules.LabelService}
	for _, key := range keys {
		if strings.HasSuffix(key, ".test") {
			t.Errorf("product key %q is the test key", key)
		}

		if key != rules.Namespace && !strings.HasPrefix(key, rules.Namespace+".") {
			t.Errorf("label key %q is outside the namespace %q", key, rules.Namespace)
		}
	}

	literals := stringLiterals(t)
	t.Logf("keys=%d literals=%d", len(keys), len(literals))

	if len(literals) == 0 {
		t.Fatal("no string literal found in rules.go")
	}

	for _, literal := range literals {
		if strings.HasSuffix(literal, ".test") {
			t.Errorf("rules.go declares the test key fragment %q", literal)
		}
	}
}

// declaredKinds parses rules.go and returns the value of every constant whose type is Kind.
func declaredKinds(t *testing.T) map[string]bool {
	t.Helper()

	file := parseRules(t)
	out := make(map[string]bool)

	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}

		ident, ok := spec.Type.(*ast.Ident)
		if !ok || ident.Name != "Kind" {
			return true
		}

		for _, value := range spec.Values {
			lit, ok := value.(*ast.BasicLit)
			if !ok {
				t.Fatalf("a Kind constant is not a string literal: %T", value)
			}

			text, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatalf("unquote %s: %v", lit.Value, err)
			}

			out[text] = true
		}

		return true
	})

	return out
}

// stringLiterals returns every string literal in rules.go.
func stringLiterals(t *testing.T) []string {
	t.Helper()

	var out []string

	ast.Inspect(parseRules(t), func(node ast.Node) bool {
		lit, ok := node.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}

		text, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Fatalf("unquote %s: %v", lit.Value, err)
		}

		out = append(out, text)

		return true
	})

	return out
}

func parseRules(t *testing.T) *ast.File {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), "rules.go", nil, 0)
	if err != nil {
		t.Fatalf("parse rules.go: %v", err)
	}

	return file
}
