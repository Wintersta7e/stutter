package testgate_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// event is one line of go test -json, as the toolchain writes it.
type event struct {
	Action      string `json:"Action"`                //nolint:tagliatelle // the toolchain's own field name
	Package     string `json:"Package,omitempty"`     //nolint:tagliatelle // the toolchain's own field name
	ImportPath  string `json:"ImportPath,omitempty"`  //nolint:tagliatelle // the toolchain's own field name
	Test        string `json:"Test,omitempty"`        //nolint:tagliatelle // the toolchain's own field name
	Output      string `json:"Output,omitempty"`      //nolint:tagliatelle // the toolchain's own field name
	FailedBuild string `json:"FailedBuild,omitempty"` //nolint:tagliatelle // the toolchain's own field name
}

// stream renders events as go test -json does: one JSON object per line.
func stream(t *testing.T, events ...event) string {
	t.Helper()

	var b strings.Builder

	for _, e := range events {
		line, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}

		b.Write(line)
		b.WriteByte('\n')
	}

	return b.String()
}

const pkg = "example.test/m"

// The go test -json actions the synthetic streams use.
const (
	aPass   = "pass"
	aFail   = "fail"
	aSkip   = "skip"
	aOutput = "output"
)

func pass(test string) event { return event{Action: aPass, Package: pkg, Test: test} }

func fail(test string) event { return event{Action: aFail, Package: pkg, Test: test} }

func skip(test string) event { return event{Action: aSkip, Package: pkg, Test: test} }

func output(test, text string) event {
	return event{Action: aOutput, Package: pkg, Test: test, Output: text}
}

// skipped is a test that logs reason and skips, the way t.Skipf reports it.
func skipped(test, reason string) []event {
	return []event{
		output(test, "=== RUN   "+test+"\n"),
		output(test, "    x_test.go:12: "+reason+"\n"),
		output(test, "--- SKIP: "+test+" (0.00s)\n"),
		skip(test),
	}
}

func count(t *testing.T, events ...event) testgate.Tally {
	t.Helper()

	tally, err := testgate.Count(strings.NewReader(stream(t, events...)))
	if err != nil {
		t.Fatalf("Count: %v", err)
	}

	return tally
}

// report writes tally under opts and returns its lines.
func report(t *testing.T, tally testgate.Tally, opts testgate.Options) []string {
	t.Helper()

	var b strings.Builder
	if err := tally.Write(&b, opts); err != nil {
		t.Fatal(err)
	}

	return strings.Split(strings.TrimRight(b.String(), "\n"), "\n")
}

func suite() testgate.Options { return testgate.Options{Docker: testgate.DockerSuite} }

const dockerSkip = "STUTTER_TEST_DOCKER=skip: the Docker tests were not run"

func TestTheCounterFailsOnAnySkip(t *testing.T) {
	t.Parallel()

	tally := count(t, append(skipped("TestNeedsADatabase", "STUTTER_TEST_POSTGRES not set"), pass("TestA"))...)

	verdict := tally.Verdict(suite())
	if len(verdict) == 0 {
		t.Fatal("a skipped test passed the count")
	}

	lines := report(t, tally, suite())
	if !strings.HasPrefix(lines[0], "tests pass=1 fail=0 skip=1 elapsed=") {
		t.Fatalf("first line %q", lines[0])
	}

	want := "skipped " + pkg + ".TestNeedsADatabase: STUTTER_TEST_POSTGRES not set"
	if !slices.Contains(lines, want) {
		t.Fatalf("the report does not name the skip %q:\n%s", want, strings.Join(lines, "\n"))
	}

	if last := lines[len(lines)-1]; !strings.HasPrefix(last, "counter: FAIL — ") {
		t.Fatalf("last line %q; want the failure line", last)
	}
}

func TestTheCounterCountsTestLevelEventsOnly(t *testing.T) {
	t.Parallel()

	tally := count(t,
		event{Action: aSkip, Package: "example.test/notests"},
		pass("TestA"), pass("TestA/sub"),
		event{Action: aPass, Package: pkg},
	)

	lines := report(t, tally, suite())
	if !strings.HasPrefix(lines[0], "tests pass=2 fail=0 skip=0 ") {
		t.Fatalf("first line %q: a package-level event was counted", lines[0])
	}

	if verdict := tally.Verdict(suite()); len(verdict) != 0 {
		t.Fatalf("verdict %q; a package with no test files is not a skip", verdict)
	}
}

func TestTheCounterFailsOnZeroPasses(t *testing.T) {
	t.Parallel()

	tally := count(t, event{Action: aSkip, Package: "example.test/notests"})
	if verdict := tally.Verdict(suite()); !slices.Contains(verdict, "no test passed") {
		t.Fatalf("verdict %q; want no test passed", verdict)
	}
}

func TestTheCounterFailsOnAPackageFailure(t *testing.T) {
	t.Parallel()

	// A compile error and a TestMain panic, as go1.27.1 reports them: neither has a test event.
	build := event{Action: "build-fail", ImportPath: "example.test/broken [example.test/broken.test]"}
	buildFail := event{
		Action: "fail", Package: "example.test/broken",
		FailedBuild: "example.test/broken [example.test/broken.test]",
	}
	panics := event{Action: aFail, Package: "example.test/panics"}

	tally := count(t, pass("TestA"), build, buildFail,
		event{Action: aOutput, Package: "example.test/panics", Output: "panic: boom\n"}, panics)

	lines := report(t, tally, suite())
	for _, want := range []string{
		"package failed example.test/broken: build failed",
		"package failed example.test/panics: no test failed",
	} {
		if !slices.Contains(lines, want) {
			t.Errorf("the report lacks %q:\n%s", want, strings.Join(lines, "\n"))
		}
	}

	if !strings.HasPrefix(lines[0], "tests pass=1 fail=0 skip=0 ") || len(tally.Verdict(suite())) == 0 {
		t.Fatalf("a package failure with no failing test passed: %q", lines)
	}

	// A package that failed because a test failed is that test's failure, not a second one.
	tally = count(t, output("TestB", "    x_test.go:3: wrong\n"), fail("TestB"), event{Action: aFail, Package: pkg})

	lines = report(t, tally, suite())
	if !slices.Contains(lines, "failed "+pkg+".TestB") || !slices.Contains(lines, "    x_test.go:3: wrong") {
		t.Fatalf("the failed test and its output are not printed:\n%s", strings.Join(lines, "\n"))
	}

	for _, line := range lines {
		if strings.HasPrefix(line, "package failed") {
			t.Fatalf("a package with a failing test is reported twice: %q", line)
		}
	}
}

func TestTheOptOutAdmitsOnlyPrefixedSkips(t *testing.T) {
	t.Parallel()

	opts := testgate.Options{Docker: testgate.DockerSuite, OptOut: true}

	tally := count(t, append(skipped("TestTheEngineAnswers", dockerSkip), pass("TestA"))...)
	if verdict := tally.Verdict(opts); len(verdict) != 0 {
		t.Fatalf("verdict %q; an opted-out Docker skip is admitted", verdict)
	}

	lines := report(t, tally, opts)
	if lines[len(lines)-1] != "DOCKER SUITE NOT RUN (1 skipped)" {
		t.Fatalf("last line %q; want the opt-out's line", lines[len(lines)-1])
	}

	if !slices.Contains(lines, "docker=opted out (STUTTER_TEST_DOCKER=skip)") {
		t.Fatalf("the docker line is missing:\n%s", strings.Join(lines, "\n"))
	}

	events := append(skipped("TestTheEngineAnswers", dockerSkip), pass("TestA"))
	events = append(events, skipped("TestNeedsADatabase", "STUTTER_TEST_POSTGRES not set")...)

	if verdict := count(t, events...).Verdict(opts); len(verdict) == 0 {
		t.Fatal("a database skip was admitted under the Docker opt-out")
	}
}

func TestTheOptOutIsRefusedUnderGitHubActions(t *testing.T) {
	t.Parallel()

	opts := testgate.Options{Docker: testgate.DockerSuite, OptOut: true, CI: true}
	tally := count(t, append(skipped("TestTheEngineAnswers", dockerSkip), pass("TestA"))...)

	if verdict := tally.Verdict(opts); len(verdict) == 0 {
		t.Fatal("an opted-out Docker skip passed under GITHUB_ACTIONS=true")
	}

	lines := report(t, tally, opts)
	if !slices.Contains(
		lines,
		"refused: STUTTER_TEST_DOCKER=skip is not honoured under GITHUB_ACTIONS=true (1 skips)",
	) {
		t.Fatalf("the refusal is not printed:\n%s", strings.Join(lines, "\n"))
	}

	for _, line := range lines {
		if strings.HasPrefix(line, "DOCKER SUITE NOT RUN") {
			t.Fatalf("a refused opt-out printed %q", line)
		}
	}
}

func TestTheCounterRequiresAuditedInvocations(t *testing.T) {
	t.Parallel()

	opts := testgate.Options{Docker: testgate.DockerSuite, RequireAudited: true}

	bare := count(t, pass("TestA"))
	if verdict := bare.Verdict(opts); !slices.Contains(verdict, "no audited invocation") {
		t.Fatalf("verdict %q; zero audited invocations must fail when required", verdict)
	}

	audited := count(t,
		output("TestRun", "    audit_test.go:40: STUTTER-AUDIT recorder check=abc calls=4 mutating=2 prune=0 "+
			"foreign-touched=0\n"),
		output("TestRun", "    audit_test.go:40: STUTTER-AUDIT recorder check=def calls=1 mutating=0 prune=0 "+
			"foreign-touched=0\n"),
		pass("TestRun"),
	)
	if verdict := audited.Verdict(opts); len(verdict) != 0 {
		t.Fatalf("verdict %q with two audited invocations", verdict)
	}

	lines := report(t, audited, opts)
	if !slices.Contains(lines, "audited invocations=2 required=yes") ||
		!slices.Contains(lines, "STUTTER-AUDIT recorder check=abc calls=4 mutating=2 prune=0 foreign-touched=0") {
		t.Fatalf("the audit lines are not printed:\n%s", strings.Join(lines, "\n"))
	}

	if verdict := bare.Verdict(testgate.Options{Docker: testgate.DockerExcluded, RequireAudited: false}); len(
		verdict) != 0 {
		t.Fatalf("verdict %q; audits are not required when the Docker suite is excluded", verdict)
	}
}

func TestTheCounterCountsStaticAudits(t *testing.T) {
	t.Parallel()

	static := "STUTTER-AUDIT static files-go=90 files-workflow=1 files-make=1 files-script=2 spawners=6 verbs=4 " +
		"compose-sites=2 mutators=3"
	opts := testgate.Options{Docker: testgate.DockerSuite, RequireStatic: true}

	tally := count(t, output("TestAuditOneHolds", "    audit1_test.go:9: "+static+"\n"), pass("TestAuditOneHolds"))
	if lines := report(t, tally, opts); !slices.Contains(lines, "static audits=1 required=yes") {
		t.Fatalf("the static audit is not counted:\n%s", strings.Join(lines, "\n"))
	}

	if verdict := count(t, pass("TestA")).Verdict(opts); !slices.Contains(verdict, "no static audit") {
		t.Fatalf("verdict %q; zero static audits must fail when required", verdict)
	}
}

func TestAMalformedLineFailsTheCount(t *testing.T) {
	t.Parallel()

	for name, input := range map[string]string{
		"not json": "not json at all\n",
		"unknown audit kind": stream(t, output("TestA", "    a_test.go:1: STUTTER-AUDIT wibble check=a n=1\n"),
			pass("TestA")),
		"wrong audit keys": stream(t, output("TestA", "    a_test.go:1: STUTTER-AUDIT tree created=1\n"),
			pass("TestA")),
	} {
		tally, err := testgate.Count(strings.NewReader(stream(t, pass("TestB")) + input))
		if err != nil {
			t.Fatalf("%s: Count: %v", name, err)
		}

		if verdict := tally.Verdict(suite()); !containsPrefix(verdict, "malformed") {
			t.Errorf("%s: verdict %q; a malformed line must fail the count", name, verdict)
		}
	}
}

func containsPrefix(lines []string, prefix string) bool {
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}

	return false
}
