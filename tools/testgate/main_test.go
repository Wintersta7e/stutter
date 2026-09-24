package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Arguments these tests pass more than once.
const (
	subBuilddef = "builddef"
	subCount    = "count"
	subExpect   = "expect"
	flagAction  = "-action"
	optOutValue = "skip"
)

// ran is one run of the tool.
type ran struct {
	stdout string
	stderr string
	code   int
}

// runWith runs the tool with stdin and env.
func runWith(args []string, stdin string, env map[string]string) ran {
	var stdout, stderr bytes.Buffer

	code := run(args, streams{
		stdin: strings.NewReader(stdin), stdout: &stdout, stderr: &stderr,
		getenv: func(key string) string { return env[key] },
	})

	return ran{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

func TestBuilddefPrintsWhatItScanned(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	makefile := filepath.Join(dir, "Makefile")
	extra := filepath.Join(dir, "gate.sh")

	if err := os.WriteFile(makefile, []byte("CGO = 0\nbuild:\n\tCGO_ENABLED=$(CGO) $(GO) build -o x ./cmd/x\n"),
		0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(extra, []byte("go build -o bin/x ./cmd/x\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got := runWith([]string{subBuilddef}, makefile+"\x00", nil)
	if got.code != exitPass {
		t.Fatalf("the recipe alone: exit %d, stderr %q", got.code, got.stderr)
	}

	if !strings.HasPrefix(got.stdout, "build-definitions files=1 found=1 cgo-assignments=0 cgo-default=0\n") {
		t.Fatalf("stdout %q does not begin with the count line", got.stdout)
	}

	got = runWith([]string{subBuilddef, extra}, makefile+"\x00", nil)
	if got.code != exitFail || !strings.Contains(got.stdout, extra+":1: go build -o bin/x ./cmd/x") {
		t.Fatalf("a second build: exit %d, stdout %q; want %d naming it", got.code, got.stdout, exitFail)
	}
}

func TestAnUnknownSubcommandIsAUsageError(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"nosuch"}, nil, {subCount, "-docker=maybe"}, {subExpect, flagAction, "fail"}, {subCount, "-nosuch"},
	} {
		if got := runWith(args, "", nil); got.code != exitUsage {
			t.Errorf("%q: exit %d, want %d", args, got.code, exitUsage)
		}
	}

	if got := runWith([]string{subBuilddef}, "no/such/file\x00", nil); got.code != exitUsage {
		t.Fatalf("an unreadable path: exit %d, want %d", got.code, exitUsage)
	}
}

// skipStream is go test -json for one passing test and one opted-out Docker skip.
const skipStream = `{"Action":"pass","Package":"example.test/m","Test":"TestA"}
{"Action":"output","Package":"example.test/m","Test":"TestTheEngineAnswers",` +
	`"Output":"    engine_test.go:18: STUTTER_TEST_DOCKER=skip: the Docker tests were not run\n"}
{"Action":"skip","Package":"example.test/m","Test":"TestTheEngineAnswers"}
`

func TestCountHonoursTheOptOutOnlyOutsideCI(t *testing.T) {
	t.Parallel()

	got := runWith([]string{subCount}, skipStream, map[string]string{"STUTTER_TEST_DOCKER": optOutValue})
	if got.code != exitPass || !strings.HasSuffix(got.stdout, "DOCKER SUITE NOT RUN (1 skipped)\n") {
		t.Fatalf("opted out locally: exit %d, stdout %q", got.code, got.stdout)
	}

	got = runWith([]string{subCount}, skipStream,
		map[string]string{"STUTTER_TEST_DOCKER": optOutValue, "GITHUB_ACTIONS": "true"})
	if got.code != exitFail || !strings.Contains(got.stdout, "\nrefused: STUTTER_TEST_DOCKER=skip is not honoured") {
		t.Fatalf("opted out in CI: exit %d, stdout %q", got.code, got.stdout)
	}

	got = runWith([]string{subCount, "-docker=excluded"}, skipStream, nil)
	if got.code != exitFail || !strings.Contains(got.stdout, "docker=excluded by derivation\n") {
		t.Fatalf("not opted out: exit %d, stdout %q", got.code, got.stdout)
	}
}

func TestExpectNamesTheOutcome(t *testing.T) {
	t.Parallel()

	args := []string{
		"expect",
		"-test",
		"TestTheEngineAnswers",
		flagAction,
		optOutValue,
		"-reason",
		"STUTTER_TEST_DOCKER=skip:",
	}
	if got := runWith(args, skipStream, nil); got.code != exitPass || !strings.Contains(got.stdout, optOutValue) {
		t.Fatalf("exit %d, stdout %q, stderr %q", got.code, got.stdout, got.stderr)
	}

	args = []string{subExpect, "-test", "TestRenamed", flagAction, "fail"}
	if got := runWith(args, skipStream, nil); got.code != exitFail || !strings.Contains(got.stderr, "did not run") {
		t.Fatalf("a renamed test: exit %d, stderr %q", got.code, got.stderr)
	}
}
