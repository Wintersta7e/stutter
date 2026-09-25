package cli_test

import (
	"bytes"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
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
			name:     "check naming nothing to check names both paths",
			args:     []string{"check"},
			wantErr:  "--compose",
			wantCode: 3,
		},
		{
			name:     "gate naming nothing to check names both paths",
			args:     []string{"gate"},
			wantErr:  "--postgres",
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

// TestUsageIsHonestAboutProvisioning keeps the help text to what the compose path does and needs: the
// four names, what a service must already do for zero declaration and the override file for the rest,
// the hosts and engines it runs on, and the two faults a compose service can be given.
func TestUsageIsHonestAboutProvisioning(t *testing.T) {
	t.Parallel()

	help := run(t, "help").stdout

	tokens := []string{
		"--compose", "--service", "--stream", "--corpus", "x-stutter", "service_completed_successfully",
		"sslmode=require", "privileged", "volumes_from", "HTTP/1.1", "Linux", "WSL2", "Compose",
		"STUTTER_TEST_DOCKER", "duplicate", "crash_before_ack",
	}

	for _, token := range tokens {
		if !strings.Contains(help, token) {
			t.Errorf("help does not carry %q", token)
		}
	}

	if strings.Contains(help, "not built yet") {
		t.Error("help still says compose provisioning is not built")
	}

	t.Logf("tokens=%d", len(tokens))
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

// TestAClosedDatabasePortIsASetupError keeps an unreachable database a setup error that names where it
// was looked for, never a verdict.
func TestAClosedDatabasePortIsASetupError(t *testing.T) {
	t.Parallel()

	var config net.ListenConfig

	// Above the kernel's source-port range, so no socket another test opens takes it meanwhile.
	closed := ""

	for port := 65500; port < 65600 && closed == ""; port++ {
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

		listener, err := config.Listen(t.Context(), "tcp4", addr)
		if err == nil {
			_ = listener.Close()
			closed = addr
		}
	}

	got := run(t, "check", "--postgres", "postgres://u:p@"+closed+"/db")

	if got.code != 3 {
		t.Errorf("exit = %d, want 3 (stderr: %s)", got.code, got.stderr)
	}

	// A setup failure is rendered in the report, on stdout, like any other outcome.
	if !strings.Contains(got.stdout+got.stderr, closed) {
		t.Errorf("the output does not name %s:\nstdout: %s\nstderr: %s", closed, got.stdout, got.stderr)
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

// TestConsumerIsComposeOnly: --consumer selects among discovered consumers, which only the compose
// path has; on the reference path it is refused before anything is dialled.
func TestConsumerIsComposeOnly(t *testing.T) {
	t.Parallel()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var accepted atomic.Int32

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			accepted.Add(1)

			_ = conn.Close()
		}
	}()

	got := run(t, "check", "--postgres", "postgres://u:p@"+listener.Addr().String()+"/db", "--consumer", "x")

	if closeErr := listener.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	t.Logf("accepts=%d", accepted.Load())

	if got.code != 3 || !strings.Contains(got.stderr, "--consumer") {
		t.Errorf("exit = %d, stderr = %q; want 3 naming --consumer", got.code, got.stderr)
	}

	if accepted.Load() != 0 {
		t.Errorf("the refused check dialled the database %d times", accepted.Load())
	}
}
