package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// subBuilddef is the subcommand these tests run.
const subBuilddef = "builddef"

func TestBuilddefPrintsWhatItScanned(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	makefile := filepath.Join(dir, "Makefile")
	extra := filepath.Join(dir, "gate.sh")

	if err := os.WriteFile(makefile, []byte("build:\n\tCGO_ENABLED=0 $(GO) build -o x ./cmd/x\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(extra, []byte("go build -o bin/x ./cmd/x\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer

	if code := run([]string{subBuilddef}, strings.NewReader(makefile+"\x00"), &stdout, &stderr); code != exitPass {
		t.Fatalf("the recipe alone: exit %d, stderr %q", code, stderr.String())
	}

	if !strings.HasPrefix(stdout.String(), "build-definitions files=1 found=1 cgo-assignments=0\n") {
		t.Fatalf("stdout %q does not begin with the count line", stdout.String())
	}

	stdout.Reset()

	if code := run(
		[]string{subBuilddef, extra},
		strings.NewReader(makefile+"\x00"),
		&stdout,
		&stderr,
	); code != exitFail {
		t.Fatalf("a second build: exit %d, want %d", code, exitFail)
	}

	if !strings.Contains(stdout.String(), extra+":1: go build -o bin/x ./cmd/x") {
		t.Fatalf("stdout %q does not name the second build", stdout.String())
	}
}

func TestAnUnknownSubcommandIsAUsageError(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer

	if code := run([]string{"nosuch"}, strings.NewReader(""), &stdout, &stderr); code != exitUsage {
		t.Fatalf("exit %d, want %d", code, exitUsage)
	}

	if code := run(nil, strings.NewReader(""), &stdout, &stderr); code != exitUsage {
		t.Fatalf("no subcommand: exit %d, want %d", code, exitUsage)
	}

	if code := run([]string{subBuilddef}, strings.NewReader("no/such/file\x00"), &stdout, &stderr); code != exitUsage {
		t.Fatalf("an unreadable path: exit %d, want %d", code, exitUsage)
	}
}
