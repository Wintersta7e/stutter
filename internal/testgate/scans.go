package testgate

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// localProofTag is the build tag of a proof run only by hand, on the engine it needs.
const localProofTag = "localproof"

// errHiddenFromCI means a production file compiles only under the local-proof tag, so CI never
// builds it.
var errHiddenFromCI = errors.New("a file only local builds compile")

// LocalProofs returns the Go files whose build constraint names the local-proof tag: proofs CI
// never compiles. Each must be a test file; a production file under the tag is an error.
func LocalProofs(files []SourceFile) ([]string, error) {
	var proofs, hidden []string

	for _, f := range files {
		if !strings.HasSuffix(f.Path, ".go") || !constrainedTo(f.Content, localProofTag) {
			continue
		}

		if strings.HasSuffix(f.Path, "_test.go") {
			proofs = append(proofs, f.Path)
		} else {
			hidden = append(hidden, f.Path)
		}
	}

	slices.Sort(proofs)

	if hidden != nil {
		return proofs, fmt.Errorf("%w: %s", errHiddenFromCI, strings.Join(hidden, ", "))
	}

	return proofs, nil
}

// constrainedTo reports whether a Go file's build constraint, above its package clause, names tag.
func constrainedTo(content []byte, tag string) bool {
	for line := range strings.Lines(string(content)) {
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "package ") {
			return false
		}

		if expr, ok := strings.CutPrefix(line, "//go:build "); ok {
			return slices.Contains(strings.FieldsFunc(expr, notInTag), tag)
		}
	}

	return false
}

// notInTag reports whether r cannot be part of a build tag: it separates the tags of a constraint.
func notInTag(r rune) bool {
	return r != '_' && r != '.' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z')
}
