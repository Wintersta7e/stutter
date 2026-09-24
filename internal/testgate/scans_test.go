package testgate_test

import (
	"slices"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

func TestLocalProofsAreListed(t *testing.T) {
	t.Parallel()

	tagged := "//go:build localproof\n\npackage x_test\n"
	files := []testgate.SourceFile{
		file("internal/a/desktop_test.go", tagged),
		file("internal/b/wsl_test.go", "//go:build linux && localproof\n\npackage b_test\n"),
		file("internal/a/plain_test.go", "package a_test\n"),
		file("internal/a/plain.go", "package a\n\n// a comment naming localproof is not a constraint\n"),
	}

	proofs, err := testgate.LocalProofs(files)
	if err != nil {
		t.Fatal(err)
	}

	if want := []string{"internal/a/desktop_test.go", "internal/b/wsl_test.go"}; !slices.Equal(proofs, want) {
		t.Fatalf("local proofs %q, want %q", proofs, want)
	}

	hidden := append(slices.Clone(files), file("internal/a/hidden.go", tagged))
	if proofs, err := testgate.LocalProofs(hidden); err == nil {
		t.Fatalf("a production file only local builds compile was listed as %q; want an error", proofs)
	}
}

func TestTestcontainersIsNeverADependency(t *testing.T) {
	t.Parallel()

	clean := []testgate.SourceFile{
		file("go.mod", "module x\n\nrequire github.com/nats-io/nats.go v1.0.0\n"),
		file("go.sum", "github.com/nats-io/nats.go v1.0.0 h1:abc=\n"),
	}

	if n, err := testgate.ScanNoContainers(clean); n != 0 || err != nil {
		t.Fatalf("clean: occurrences=%d, %v; want 0 and nil", n, err)
	}

	dirty := append(slices.Clone(clean),
		file("go.sum", "github.com/"+"testcontainers/testcontainers-go v0.40.0 h1:abc=\n"))
	if n, err := testgate.ScanNoContainers(dirty); n != 1 || err == nil {
		t.Fatalf("a go.sum line: occurrences=%d, %v; want 1 and an error", n, err)
	}

	if _, err := testgate.ScanNoContainers(nil); err == nil {
		t.Fatal("zero files scanned passed")
	}
}
