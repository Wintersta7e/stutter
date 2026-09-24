// Package dockertest is the only route a test takes to a container engine.
//
// Require is the first call of every Docker test. It asks the product's own preconditions whether an
// engine is usable, so a test and the product never disagree about that, and a test that cannot
// reach one FAILS unless STUTTER_TEST_DOCKER=skip says not to run it. Every other helper hangs off
// the Engine that Require returns, so no test can reach Docker without passing that gate first.
//
// Only test files import this package; the linter keeps it out of the product.
package dockertest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/testgate"
)

// The opt-out's variable, its one skipping value, and the prefix of every skip it causes, which the
// skip counter admits and nothing else; declared once, where the counter reads them.
const (
	OptOutVariable = testgate.OptOutVariable
	optOutValue    = testgate.OptOutValue
	SkipPrefix     = testgate.OptOutPrefix
)

// testLabelKey is the label every resource a test creates carries. The product never writes or reads
// it.
const testLabelKey = rules.Namespace + ".test"

// reporter is the part of testing.TB the gate uses; a unit test supplies a fake.
type reporter interface {
	Helper()
	Fatalf(format string, args ...any)
	Skipf(format string, args ...any)
	Context() context.Context
}

// Engine is an engine the product's preconditions accepted.
type Engine struct {
	identity provision.Identity
}

// Endpoint returns the endpoint the preconditions pinned, as DOCKER_HOST spells it.
func (e Engine) Endpoint() string { return e.identity.Endpoint }

// Arch returns the engine's architecture, in Go's naming.
func (e Engine) Arch() string { return e.identity.Arch }

// ID returns the engine's own ID.
func (e Engine) ID() string { return e.identity.EngineID }

// TestLabel returns the label every test-made resource carries: the reserved test key and a value
// random to this test process.
//
//nolint:nonamedreturns // two strings of one type: the names say which is the key.
func (Engine) TestLabel() (key, value string) {
	return testLabelKey, labelValue()
}

// probed holds the one precondition read of this test process.
type probed struct {
	err      error
	identity provision.Identity
}

var (
	//nolint:gochecknoglobals // one engine probe per test process, whichever test asks first.
	probeOnce sync.Once
	//nolint:gochecknoglobals // one engine probe per test process, whichever test asks first.
	probeResult probed
	//nolint:gochecknoglobals // set by Main; the build helper refuses to run without it.
	mainInstalled atomic.Bool
	//nolint:gochecknoglobals // one test-label value per test process.
	labelValue = sync.OnceValue(randomHex)
)

// randomHex returns 16 random bytes as hex.
func randomHex() string {
	var b [16]byte

	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error.

	return hex.EncodeToString(b[:])
}

// Require is the first call of every Docker test. Under STUTTER_TEST_DOCKER=skip it skips the
// test; unset or empty, it returns the engine the product's preconditions accept, and FAILS the
// test when there is none; any other value FAILS the test.
func Require(tb testing.TB) Engine {
	tb.Helper()

	return decide(tb, os.Getenv(OptOutVariable), func() (provision.Identity, error) {
		probeOnce.Do(func() {
			// Detached from the first caller's cancellation, so a first test that ends early cannot
			// leave a cancelled probe behind for every later one. Each read carries its own deadline.
			ctx := context.WithoutCancel(tb.Context())
			probeResult.identity, probeResult.err = provision.Preconditions(ctx)
		})

		return probeResult.identity, probeResult.err
	})
}

// decide applies the opt-out variable's value to the engine the probe reads.
func decide(r reporter, value string, probe func() (provision.Identity, error)) Engine {
	r.Helper()

	switch value {
	case optOutValue:
		r.Skipf("%s the Docker tests were not run", SkipPrefix)
	case "":
	default:
		r.Fatalf("%s=%q is not accepted: leave it unset or empty (%q) to run the Docker tests, or set %q to skip "+
			"them", OptOutVariable, value, "", optOutValue)
	}

	identity, err := probe()
	if err != nil {
		r.Fatalf("no usable Docker engine: %v. The Docker tests need a reachable engine with a supported "+
			"compose plugin; to skip them, set %s=%s", err, OptOutVariable, optOutValue)
	}

	return Engine{identity: identity}
}

// Main runs a Docker test package's tests and then removes the binaries Binary built for them. A
// package whose tests build the product calls it from its TestMain; Go exits with m.Run's code.
func Main(m *testing.M) {
	mainInstalled.Store(true)

	defer processBuilder.remove()

	m.Run()
}
