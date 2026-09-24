package dependencytest_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// classifyFixture runs the classification containers of classify.yaml once and returns their answers,
// the monotonic time they took, and the fixture.
func classifyFixture(t *testing.T) (map[string]map[uint16]pg.Answer, *fixture, stopwatch) {
	t.Helper()

	f := newFixture(t, fixturePath(t, "classify.yaml"), fixtureOptions{})
	clock := startStopwatch()

	answers, err := provision.Classify(t.Context(), f.classifyConfig())
	if err != nil {
		t.Fatalf("provision.Classify() error = %v", err)
	}

	t.Logf("classification took %v (startup limit %v); answers %v", clock.elapsed(), startupLimit, answers)

	return answers, f, clock
}

// TestClassificationPromotesAPostgresPort promotes a port the static evidence left opaque once a
// throwaway container of its service answers the Postgres handshake on it, and leaves no container.
func TestClassificationPromotesAPostgresPort(t *testing.T) {
	t.Parallel()

	answers, f, _ := classifyFixture(t)

	if got := answers["db"][6000]; got != pg.AnswerPostgres {
		t.Fatalf(`answers["db"][6000] = %q, want %q`, got, pg.AnswerPostgres)
	}

	promoted, err := compose.Classify(f.model, f.images, answers)
	if err != nil {
		t.Fatalf("compose.Classify() with the answers error = %v", err)
	}

	protocol := compose.Protocol("")

	for _, dep := range promoted.Deps {
		for _, found := range dep.Endpoints {
			if dep.Service == "db" && found.Port == 6000 {
				protocol = found.Protocol
			}
		}
	}

	if protocol != compose.ProtocolPG {
		t.Errorf("db:6000 is served as %q after the handshake, want %q", protocol, compose.ProtocolPG)
	}

	if left := f.containersOfKind(t, rules.KindVerifier); len(left) != 0 {
		t.Errorf("the engine still holds %d classification containers: %v", len(left), left)
	}
}

// TestAnUnservedPortAnswersNone gives a port nothing serves the answer none within the startup limit:
// its container is asked no longer than that, and the port stays opaque.
func TestAnUnservedPortAnswersNone(t *testing.T) {
	t.Parallel()

	answers, _, clock := classifyFixture(t)
	took := clock.elapsed()

	if got := answers["db"][7000]; got != pg.AnswerNone {
		t.Errorf(`answers["db"][7000] = %q, want %q`, got, pg.AnswerNone)
	}

	// The total includes removing the container after the limit, which took over a second on a 4-CPU
	// host. Halfway to twice the limit still fails a classification that ignored the limit or applied it
	// twice; one that restarted it per container adds only create and start, which no wall-clock bound
	// can resolve on a shared runner (measured 0.4 s).
	if bound := startupLimit + startupLimit/2; took > bound {
		t.Errorf("classification took %v, want within %v of a %v startup limit", took, bound, startupLimit)
	}
}
