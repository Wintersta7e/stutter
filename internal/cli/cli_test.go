package cli_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/cli"
	"github.com/Wintersta7e/stutter/internal/report"
)

const envPostgres = "STUTTER_TEST_POSTGRES"

// outcome is what one command produced. A struct rather than three bare returns: two adjacent
// strings invite being swapped at the call site, and a swapped stdout and stderr would make a test
// assert the opposite of what it means.
type outcome struct {
	stdout string
	stderr string
	code   int
}

func run(t *testing.T, args ...string) outcome {
	t.Helper()

	var stdout, stderr bytes.Buffer

	code := cli.Run(t.Context(), args, &stdout, &stderr)

	return outcome{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

func TestCommandSurface(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		wantOut  string
		wantErr  string
		args     []string
		wantCode int
	}{
		{name: "no arguments prints usage and claims no verdict", args: nil, wantErr: "Usage:", wantCode: 3},
		{name: "help goes to stdout", args: []string{"help"}, wantOut: "Usage:", wantCode: 0},
		{name: "unknown command names it", args: []string{"wibble"}, wantErr: `unknown command "wibble"`, wantCode: 3},
		{name: "version exits clean", args: []string{"version"}, wantCode: 0},
		{
			name:     "check without a database is a setup error",
			args:     []string{"check"},
			wantErr:  "--postgres is required",
			wantCode: 3,
		},
		{
			name:     "gate without a database is a setup error",
			args:     []string{"gate"},
			wantErr:  "--postgres is required",
			wantCode: 3,
		},
		{
			name:     "an unknown flag does not run a check",
			args:     []string{"check", "--nonsense"},
			wantErr:  "flag provided but not defined",
			wantCode: 3,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := run(t, testCase.args...)

			if got.code != testCase.wantCode {
				t.Errorf("exit = %d, want %d (stderr: %s)", got.code, testCase.wantCode, got.stderr)
			}

			if testCase.wantOut != "" && !strings.Contains(got.stdout, testCase.wantOut) {
				t.Errorf("stdout = %q, want it to contain %q", got.stdout, testCase.wantOut)
			}

			if testCase.wantErr != "" && !strings.Contains(got.stderr, testCase.wantErr) {
				t.Errorf("stderr = %q, want it to contain %q", got.stderr, testCase.wantErr)
			}
		})
	}
}

// TestVersionWritesSomething guards against a command that exits zero having printed nothing, which
// reads as success but tells the caller nothing.
func TestVersionWritesSomething(t *testing.T) {
	t.Parallel()

	got := run(t, "version")
	if got.code != 0 {
		t.Fatalf("exit = %d, want 0", got.code)
	}

	if strings.TrimSpace(got.stdout) == "" {
		t.Error("version printed nothing")
	}
}

// TestUsageIsHonestAboutProvisioning keeps the help text from promising a capability that does not
// exist. Stutter cannot yet point at a user's own service, and the usage must say so.
func TestUsageIsHonestAboutProvisioning(t *testing.T) {
	t.Parallel()

	if got := run(t, "help"); !strings.Contains(got.stdout, "not built yet") {
		t.Errorf("usage does not disclose that compose provisioning is missing:\n%s", got.stdout)
	}
}

// TestRelayIsAHiddenCommand keeps the relay out of the user's view while the binary still runs it: a
// relay container's entrypoint is this binary, so the command must dispatch, and a user has no use
// for it, so the usage must not offer it.
func TestRelayIsAHiddenCommand(t *testing.T) {
	t.Parallel()

	if help := run(t, "help"); strings.Contains(help.stdout, "relay") {
		t.Errorf("the usage mentions relay:\n%s", help.stdout)
	}

	got := run(t, "relay")
	if got.code != 2 {
		t.Errorf("relay with no mode: exit = %d, want 2 (stderr: %s)", got.code, got.stderr)
	}

	if strings.Contains(got.stderr, "unknown command") {
		t.Errorf("relay was not dispatched: %s", got.stderr)
	}
}

// TestCheckAgainstAReferenceConsumer is the whole binary end to end: provision, gate, inject,
// shrink, report, exit. It must FAIL, because the reference consumer carries a handler that
// genuinely loses stock under redelivery.
func TestCheckAgainstAReferenceConsumer(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv(envPostgres)
	if dsn == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	got := run(t, "check", "--postgres", dsn, "--max-runs", "1")
	t.Logf("report:\n%s", got.stdout)

	if got.code != 1 {
		t.Fatalf("exit = %d, want 1\nstdout:\n%s\nstderr:\n%s", got.code, got.stdout, got.stderr)
	}

	if !strings.Contains(got.stdout, "FAIL") {
		t.Errorf("no FAIL line in the report:\n%s", got.stdout)
	}

	if !strings.Contains(got.stdout, "legal because") {
		t.Errorf("a finding does not name the clause that made its fault legal:\n%s", got.stdout)
	}
}

// TestGateAgainstAReferenceConsumer proves the gates-only mode injects nothing: a stable service
// passes it, and passing says the service is testable rather than that it is correct.
func TestGateAgainstAReferenceConsumer(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv(envPostgres)
	if dsn == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	got := run(t, "gate", "--postgres", dsn)
	t.Logf("report:\n%s", got.stdout)

	if got.code != 0 {
		t.Fatalf("exit = %d, want 0 — the reference consumer should hold the gates\nstderr: %s",
			got.code, got.stderr)
	}

	if strings.Contains(got.stdout, "FAIL") {
		t.Errorf("gates-only mode reported a finding, so it injected a fault:\n%s", got.stdout)
	}

	// Nothing was injected, so nothing was survived: a PASS here is a verdict no fault earned.
	held := 0

	for line := range strings.Lines(got.stdout) {
		if strings.HasPrefix(line, string(report.StatusPass)) {
			t.Errorf("gates-only mode rendered a PASS line: %q", line)
		}

		if strings.HasPrefix(line, string(report.StatusHeld)) {
			held++
		}
	}

	if held != 1 {
		t.Errorf("%d lines open with %q, want exactly one:\n%s", held, report.StatusHeld, got.stdout)
	}
}
