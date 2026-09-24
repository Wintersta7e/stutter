package dockertest_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
)

func TestMain(m *testing.M) {
	dockertest.Main(m)
}

// TestTheEngineAnswers is the one Docker test the opt-out contract is proven on: it fails without an
// engine, skips only when opted out, and fails on an unknown opt-out value.
func TestTheEngineAnswers(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)

	if engine.Endpoint() == "" || engine.Arch() == "" || engine.ID() == "" {
		t.Fatalf("engine id=%q arch=%q endpoint=%q: the preconditions accepted an engine they could not name",
			engine.ID(), engine.Arch(), engine.Endpoint())
	}

	t.Logf("engine id=%s arch=%s endpoint=%s", engine.ID(), engine.Arch(), engine.Endpoint())
}
