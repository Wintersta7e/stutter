package testgate_test

import (
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

func TestExpectFailsWhenTheTestDidNotRun(t *testing.T) {
	t.Parallel()

	const name = "TestTheEngineAnswers"

	cases := []struct {
		name   string
		input  string
		action string
		reason string
		ok     bool
	}{
		{
			name: "renamed away: no event", input: stream(t, event{
				Action: aOutput, Package: pkg,
				Output: "testing: warning: no tests to run\n",
			}, event{Action: aPass, Package: pkg}),
			action: aFail,
		},
		{name: "the wanted failure", input: stream(t, fail(name)), action: aFail, ok: true},
		{name: "the wrong outcome", input: stream(t, pass(name)), action: aFail},
		{
			name: "a skip with the prefix", input: stream(t, skipped(name, dockerSkip)...), action: aSkip,
			reason: "STUTTER_TEST_DOCKER=skip:", ok: true,
		},
		{
			name: "a skip for another reason", input: stream(t, skipped(name, "no engine")...), action: aSkip,
			reason: "STUTTER_TEST_DOCKER=skip:",
		},
		{name: "ran twice", input: stream(t, fail(name), fail(name)), action: aFail},
		{name: "a subtest is not the test", input: stream(t, fail(name+"/sub")), action: aFail},
	}

	for _, tc := range cases {
		got, err := testgate.Expect(strings.NewReader(tc.input), name, tc.action, tc.reason)
		if (err == nil) != tc.ok {
			t.Errorf("%s: Expect = %q, %v; want ok=%v", tc.name, got, err, tc.ok)
		}
	}
}
