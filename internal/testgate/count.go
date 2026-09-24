package testgate

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"
)

// The Docker suite's opt-out, stated once: the variable, the one value that skips the Docker tests,
// and the prefix every skip it causes begins with.
const (
	OptOutVariable = "STUTTER_TEST_DOCKER"
	OptOutValue    = "skip"
	OptOutPrefix   = OptOutVariable + "=" + OptOutValue + ":"
)

// DockerMode says whether the counted run included the Docker suite.
type DockerMode string

// The Docker modes.
const (
	// DockerSuite is a run of every package: the Docker suite included.
	DockerSuite DockerMode = "suite"
	// DockerExcluded is a run of a package set derived to leave out every Docker package.
	DockerExcluded DockerMode = "excluded"
)

// Options is what the counter knows about the run it counts.
type Options struct {
	// Docker says whether the run included the Docker suite.
	Docker DockerMode
	// OptOut is STUTTER_TEST_DOCKER=skip in the counter's environment.
	OptOut bool
	// CI is GITHUB_ACTIONS=true in the counter's environment: the opt-out is refused there.
	CI bool
	// RequireAudited fails a run with no audited binary invocation.
	RequireAudited bool
	// RequireStatic fails a run with no static audit line.
	RequireStatic bool
}

// testEvent is one line of go test -json.
type testEvent struct {
	Action      string `json:"Action"`      //nolint:tagliatelle // the toolchain's own field name
	Package     string `json:"Package"`     //nolint:tagliatelle // the toolchain's own field name
	Test        string `json:"Test"`        //nolint:tagliatelle // the toolchain's own field name
	Output      string `json:"Output"`      //nolint:tagliatelle // the toolchain's own field name
	FailedBuild string `json:"FailedBuild"` //nolint:tagliatelle // the toolchain's own field name
}

// logLinePattern matches a line t.Logf or t.Skipf wrote, and captures its message.
var logLinePattern = regexp.MustCompile(`^\s+[\w./-]+\.go:\d+: (.*?)\s*$`)

// errMalformed means a line of go test -json output was not a JSON event.
var errMalformed = errors.New("malformed go test -json line")

// skippedTest is one skipped test and the reason it gave.
type skippedTest struct {
	name   string
	reason string
}

// failedTest is one failed test and everything it printed.
type failedTest struct {
	name   string
	output []string
}

// packageFailure is a package that failed with no failing test: a build failure or a TestMain that
// never ran the tests.
type packageFailure struct {
	pkg    string
	reason string
}

// The go test -json actions the counter reads.
const (
	actionOutput = "output"
	actionPass   = "pass"
	actionFail   = "fail"
	actionSkip   = "skip"
)

// Tally is every test-level outcome of one go test -json run, and the audit lines it printed.
type Tally struct {
	failedPackages map[string]bool
	auditedChecks  map[string]bool
	skipped        []skippedTest
	failed         []failedTest
	packages       []packageFailure
	audits         []string
	elapsed        time.Duration
	pass           int
	fail           int
	skip           int
	static         int
	malformed      int
}

// Count reads go test -json from r and counts test-level outcomes; package-level events count only
// as a package failure with no failing test. It errors only when r cannot be read.
func Count(r io.Reader) (Tally, error) {
	start := time.Now()
	tally := Tally{failedPackages: map[string]bool{}, auditedChecks: map[string]bool{}}
	outputs := map[string][]string{}

	err := readEvents(r, func(e testEvent, err error) {
		if err != nil {
			tally.malformed++

			return
		}

		addEvent(&tally, e, outputs)
	})
	tally.elapsed = time.Since(start)

	return tally, err
}

// Verdict returns every condition the run fails; none means it passed.
func (t Tally) Verdict(opts Options) []string {
	var failed []string

	add := func(cond bool, format string, args ...any) {
		if cond {
			failed = append(failed, fmt.Sprintf(format, args...))
		}
	}

	add(t.fail > 0, "%d tests failed", t.fail)
	add(len(t.packages) > 0, "%d packages failed with no failing test", len(t.packages))
	add(t.pass == 0, "no test passed")
	add(t.skip > t.admitted(opts), "%d tests skipped", t.skip-t.admitted(opts))
	add(opts.CI && t.optOutSkips() > 0, "%d opt-out skips refused under GITHUB_ACTIONS=true", t.optOutSkips())
	add(opts.RequireAudited && len(t.auditedChecks) == 0, "no audited invocation")
	add(opts.RequireStatic && t.static == 0, "no static audit")
	add(t.malformed > 0, "malformed lines=%d", t.malformed)

	return failed
}

// Write prints the tally: counts, the Docker mode, every skip with its reason, every failed test's
// output, every package failure and audit line, and a last line that says how the run ended.
func (t Tally) Write(w io.Writer, opts Options) error {
	var b strings.Builder

	fmt.Fprintf(&b, "tests pass=%d fail=%d skip=%d elapsed=%s\n", t.pass, t.fail, t.skip,
		t.elapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "docker=%s\n", dockerLine(opts))

	for _, s := range t.skipped {
		fmt.Fprintf(&b, "skipped %s: %s\n", s.name, s.reason)
	}

	for _, f := range t.failed {
		fmt.Fprintf(&b, "failed %s\n%s", f.name, strings.Join(f.output, ""))
	}

	for _, p := range t.packages {
		fmt.Fprintf(&b, "package failed %s: %s\n", p.pkg, p.reason)
	}

	for _, a := range t.audits {
		fmt.Fprintln(&b, a)
	}

	fmt.Fprintf(&b, "audited invocations=%d required=%s\n", len(t.auditedChecks), requirement(opts.RequireAudited))
	fmt.Fprintf(&b, "static audits=%d required=%s\n", t.static, requirement(opts.RequireStatic))

	if n := t.optOutSkips(); opts.CI && n > 0 {
		fmt.Fprintf(&b, "refused: %s=%s is not honoured under GITHUB_ACTIONS=true (%d skips)\n", OptOutVariable,
			OptOutValue, n)
	}

	fmt.Fprintf(&b, "malformed lines=%d\n", t.malformed)

	switch verdict := t.Verdict(opts); {
	case len(verdict) > 0:
		fmt.Fprintf(&b, "counter: FAIL — %s\n", strings.Join(verdict, "; "))
	case opts.OptOut:
		fmt.Fprintf(&b, "DOCKER SUITE NOT RUN (%d skipped)\n", t.admitted(opts))
	default:
	}

	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("writing the tally: %w", err)
	}

	return nil
}

// admitted is the number of skips the opt-out admits: prefixed skips, opted out, outside CI.
func (t Tally) admitted(opts Options) int {
	if !opts.OptOut || opts.CI {
		return 0
	}

	return t.optOutSkips()
}

// optOutSkips is the number of skips the opt-out caused.
func (t Tally) optOutSkips() int {
	n := 0

	for _, s := range t.skipped {
		if strings.HasPrefix(s.reason, OptOutPrefix) {
			n++
		}
	}

	return n
}

// addEvent counts one event into t; outputs holds each unfinished test's output.
func addEvent(t *Tally, e testEvent, outputs map[string][]string) {
	key := e.Package + "." + e.Test

	switch {
	case e.Action == actionOutput:
		if e.Test != "" {
			outputs[key] = append(outputs[key], e.Output)
		}

		addAudit(t, e.Output)
	case e.Test == "":
		addPackage(t, e)
	default:
		addTest(t, e, key, outputs[key])
		delete(outputs, key)
	}
}

// addPackage counts a package-level event: a failure with no failing test is a failure of its own.
func addPackage(t *Tally, e testEvent) {
	switch {
	case e.Action != actionFail:
	case e.FailedBuild != "":
		t.packages = append(t.packages, packageFailure{pkg: e.Package, reason: "build failed"})
	case !t.failedPackages[e.Package]:
		t.packages = append(t.packages, packageFailure{pkg: e.Package, reason: "no test failed"})
	default:
	}
}

// addTest counts a test-level outcome; output is everything the test printed.
func addTest(t *Tally, e testEvent, key string, output []string) {
	switch e.Action {
	case actionPass:
		t.pass++
	case actionFail:
		t.fail++
		t.failed = append(t.failed, failedTest{name: key, output: output})
		t.failedPackages[e.Package] = true
	case actionSkip:
		t.skip++
		t.skipped = append(t.skipped, skippedTest{name: key, reason: skipReason(output)})
	default:
	}
}

// addAudit records into t the audit line one output line carries, if any.
func addAudit(t *Tally, output string) {
	text, found := auditIn(output)
	if !found {
		return
	}

	audit, ok := ParseAudit(text)
	if !ok {
		t.malformed++

		return
	}

	t.audits = append(t.audits, text)

	// Audited invocations are the distinct checks the recorder lines name.
	if audit.Kind == AuditRecorder {
		t.auditedChecks[audit.Check] = true
	}

	if audit.Kind == AuditStatic {
		t.static++
	}
}

// skipReason is the last message a skipped test logged: t.Skipf's reason.
func skipReason(output []string) string {
	for _, line := range slices.Backward(output) {
		if m := logLinePattern.FindStringSubmatch(line); m != nil {
			return m[1]
		}
	}

	return ""
}

// readEvents calls each for every non-empty line of r, with the event or the line's decode error.
func readEvents(r io.Reader, each func(testEvent, error)) error {
	reader := bufio.NewReader(r)

	for {
		line, err := reader.ReadString('\n')
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			var e testEvent
			if decodeErr := json.Unmarshal([]byte(trimmed), &e); decodeErr != nil {
				each(e, fmt.Errorf("%w: %q", errMalformed, trimmed))
			} else {
				each(e, nil)
			}
		}

		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("reading go test -json: %w", err)
		}
	}
}

// dockerLine says whether the Docker suite ran.
func dockerLine(opts Options) string {
	switch {
	case opts.Docker == DockerExcluded:
		return "excluded by derivation"
	case opts.OptOut:
		return "opted out (" + OptOutVariable + "=" + OptOutValue + ")"
	default:
		return string(DockerSuite)
	}
}

// requirement spells whether a condition is armed.
type requirement bool

// String returns yes or no.
func (r requirement) String() string {
	if r {
		return "yes"
	}

	return "no"
}
