package main

import (
	"flag"
	"fmt"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// auditedInvocationsRequired arms the counter's audited-invocation condition. It stays false until
// the first binary-level Docker test that runs inside the deletion audit lands: before that, zero
// audited invocations is every run's truth, not a bypassed audit.
const auditedInvocationsRequired = false

// staticAuditRequired arms the counter's static-audit condition. It stays false until the static
// audit of the tree's spawners, verbs and mutators lands; from then on a run without its line means
// that audit did not run.
const staticAuditRequired = false

// count counts go test -json on stdin and fails on any skip, failure or missing audit.
func count(args []string, s streams) int {
	flags := flag.NewFlagSet("count", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	docker := flags.String("docker", string(testgate.DockerSuite),
		"suite: the run includes the Docker suite; excluded: its package set leaves every Docker package out")

	if err := flags.Parse(args); err != nil {
		return exitUsage
	}

	mode := testgate.DockerMode(*docker)
	if mode != testgate.DockerSuite && mode != testgate.DockerExcluded {
		fmt.Fprintf(s.stderr, "count: -docker=%s is neither %s nor %s\n", mode, testgate.DockerSuite,
			testgate.DockerExcluded)

		return exitUsage
	}

	optOut := s.getenv(testgate.OptOutVariable) == testgate.OptOutValue
	opts := testgate.Options{
		Docker:         mode,
		OptOut:         optOut,
		CI:             s.getenv("GITHUB_ACTIONS") == "true",
		RequireAudited: auditedInvocationsRequired && mode == testgate.DockerSuite && !optOut,
		RequireStatic:  staticAuditRequired && mode == testgate.DockerSuite,
	}

	tally, err := testgate.Count(s.stdin)
	if err != nil {
		fmt.Fprintf(s.stderr, "count: %v\n", err)

		return exitUsage
	}

	if err := tally.Write(s.stdout, opts); err != nil {
		fmt.Fprintf(s.stderr, "count: %v\n", err)

		return exitUsage
	}

	if len(tally.Verdict(opts)) > 0 {
		return exitFail
	}

	return exitPass
}

// expect checks one named test's single outcome in go test -json on stdin.
func expect(args []string, s streams) int {
	flags := flag.NewFlagSet("expect", flag.ContinueOnError)
	flags.SetOutput(s.stderr)
	test := flags.String("test", "", "the test's name, exactly")
	action := flags.String("action", "", "pass, fail or skip")
	reason := flags.String("reason", "", "the prefix a skip's reason must begin with")

	if err := flags.Parse(args); err != nil {
		return exitUsage
	}

	if *test == "" || *action == "" {
		fmt.Fprintln(s.stderr, "expect: -test and -action are required")

		return exitUsage
	}

	got, err := testgate.Expect(s.stdin, *test, *action, *reason)
	if err != nil {
		fmt.Fprintf(s.stderr, "expect: FAIL — %v\n", err)

		return exitFail
	}

	fmt.Fprintf(s.stdout, "expect: %s\n", got)

	return exitPass
}
