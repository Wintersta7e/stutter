package dockertest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision"
)

// The outcomes decide can reach.
const (
	outcomeFatal = "fatal"
	outcomeSkip  = "skip"
	outcomeRun   = "run"
)

// stopped is what the fake reporter panics with, so a Fatalf or Skipf ends decide the way
// runtime.Goexit ends a real test.
type stopped struct{ outcome string }

// fakeReporter records the one outcome decide reached.
type fakeReporter struct {
	parent  *testing.T
	message string
}

func (*fakeReporter) Helper() {}

func (f *fakeReporter) Fatalf(format string, args ...any) {
	f.message = fmt.Sprintf(format, args...)

	panic(stopped{outcome: outcomeFatal})
}

func (f *fakeReporter) Skipf(format string, args ...any) {
	f.message = fmt.Sprintf(format, args...)

	panic(stopped{outcome: outcomeSkip})
}

func (f *fakeReporter) Context() context.Context { return f.parent.Context() }

// decision is what one decide call did.
type decision struct {
	outcome string
	message string
	probed  bool
}

// runDecide runs decide under a fake reporter.
func runDecide(t *testing.T, value string, probe func() (provision.Identity, error)) decision {
	t.Helper()

	r := &fakeReporter{parent: t}
	got := decision{outcome: outcomeRun}
	counted := func() (provision.Identity, error) {
		got.probed = true

		return probe()
	}

	func() {
		defer func() {
			if v := recover(); v != nil {
				s, ok := v.(stopped)
				if !ok {
					panic(v)
				}

				got.outcome = s.outcome
			}
		}()

		decide(r, value, counted)
	}()

	got.message = r.message

	return got
}

func TestAnUnreachableEngineFailsUnlessOptedOut(t *testing.T) {
	t.Parallel()

	const endpoint = "unix:///nonexistent.sock"

	unreachable := func() (provision.Identity, error) {
		return provision.Identity{}, fmt.Errorf("%w: the engine at %s did not answer", provision.ErrPrecondition,
			endpoint)
	}
	reachable := func() (provision.Identity, error) {
		return provision.Identity{Endpoint: endpoint, Arch: "amd64", EngineID: "engine-1"}, nil
	}

	cases := []struct {
		probe    func() (provision.Identity, error)
		name     string
		value    string
		outcome  string
		prefix   string
		contains []string
		probed   bool
	}{
		{
			name: "unset and unreachable fails", value: "", probe: unreachable, outcome: outcomeFatal,
			contains: []string{endpoint, "STUTTER_TEST_DOCKER=skip"}, probed: true,
		},
		{
			name: "skip with no engine skips", value: "skip", probe: unreachable, outcome: outcomeSkip,
			prefix: "STUTTER_TEST_DOCKER=skip:",
		},
		{
			name: "skip with an engine skips too", value: "skip", probe: reachable, outcome: outcomeSkip,
			prefix: "STUTTER_TEST_DOCKER=skip:",
		},
		{
			name: "an unknown value fails with no engine", value: "bogus", probe: unreachable, outcome: outcomeFatal,
			contains: []string{`"bogus"`, `""`, `"skip"`},
		},
		{
			name: "an unknown value fails with an engine", value: "bogus", probe: reachable, outcome: outcomeFatal,
			contains: []string{`"bogus"`, `""`, `"skip"`},
		},
		{name: "unset and reachable runs", value: "", probe: reachable, outcome: outcomeRun, probed: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := runDecide(t, tc.value, tc.probe)
			if got.outcome != tc.outcome {
				t.Fatalf("STUTTER_TEST_DOCKER=%q: outcome %s (%q), want %s", tc.value, got.outcome, got.message,
					tc.outcome)
			}

			if got.probed != tc.probed {
				t.Errorf("STUTTER_TEST_DOCKER=%q: probe called = %v, want %v", tc.value, got.probed, tc.probed)
			}

			for _, want := range tc.contains {
				if !strings.Contains(got.message, want) {
					t.Errorf("message %q does not name %s", got.message, want)
				}
			}

			if tc.prefix != "" && !strings.HasPrefix(got.message, tc.prefix) {
				t.Errorf("skip reason %q does not begin %q", got.message, tc.prefix)
			}
		})
	}
}

func TestTheEngineCarriesWhatThePreconditionsRead(t *testing.T) {
	t.Parallel()

	engine := decide(&fakeReporter{parent: t}, "", func() (provision.Identity, error) {
		return provision.Identity{Endpoint: "unix:///run/e.sock", Arch: "arm64", EngineID: "id-7"}, nil
	})

	if engine.Endpoint() != "unix:///run/e.sock" || engine.Arch() != "arm64" || engine.ID() != "id-7" {
		t.Fatalf("engine = %q %q %q, want the identity's endpoint, arch and ID",
			engine.Endpoint(), engine.Arch(), engine.ID())
	}

	key, value := engine.TestLabel()
	if !strings.HasSuffix(key, ".test") || len(value) != 32 {
		t.Fatalf("test label %s=%s: want a .test key and 32 hex characters", key, value)
	}

	if _, other := (Engine{}).TestLabel(); other != value {
		t.Fatalf("the test label value changed within one process: %s then %s", value, other)
	}
}
