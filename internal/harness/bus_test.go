package harness_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/effect"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// TestATLSFirstBusClientStopsTheRun: a bus client that starts a TLS handshake leaves the proxy
// nothing to read. Counted as setup, it reported a handler that did nothing; it stops the run.
func TestATLSFirstBusClientStopsTheRun(t *testing.T) {
	t.Parallel()

	built, _ := quirkySandbox(t, observedConfig(), quirks{tlsFirst: true}, nil, "ORD-TLS-1")

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if !errors.Is(err, natsproxy.ErrUnsupportedBus) || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("Run() error = %v, want the run stopped as an unsupported bus client naming TLS", err)
	}
}

// TestACoreSubscriberOnACorpusSubjectStopsTheRun: a core subscription on a corpus subject is handed
// every staged message beside the consumer, and its work lands in the consumer's windows with no way
// to separate it.
func TestACoreSubscriberOnACorpusSubjectStopsTheRun(t *testing.T) {
	t.Parallel()

	built, _ := quirkySandbox(t, observedConfig(), quirks{coreSubscribe: true}, nil, "ORD-CORE-1")

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err == nil {
		t.Fatal("Run() error = nil, want the run stopped for the core subscriber")
	}

	t.Logf("the run stopped: %v", err)

	for _, want := range []string{toy.SubjectOrderCreated, "1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Run() error = %v, want it to name %q", err, want)
		}
	}
}

// TestADeliveryOnAnotherStreamIsCounted: the service's own bus work on another stream is neither
// scoped nor checked, but how much of it happened inside the run's windows is on record.
func TestADeliveryOnAnotherStreamIsCounted(t *testing.T) {
	t.Parallel()

	orders := []string{"ORD-SIDE-1", "ORD-SIDE-2"}
	built, _ := quirkySandbox(t, observedConfig(), quirks{sideStream: true}, nil, orders...)

	result, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Elsewhere != len(orders) {
		t.Errorf("Elsewhere = %d, want one side delivery per order (%d)", result.Elsewhere, len(orders))
	}
}

// results wraps a session and keeps every run's result.
type results struct {
	inner check.Session
	runs  []replay.Result
}

func (r *results) Reset(ctx context.Context) error {
	return r.inner.Reset(ctx)
}

func (r *results) Run(
	ctx context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	result, err := r.inner.Run(ctx, name, mutation, retain)
	if err == nil {
		r.runs = append(r.runs, result)
	}

	return result, err
}

// TestASoleEphemeralConsumerIsCheckedAcrossRuns: a consumer the client names afresh on every start
// cannot be found by name from one run to the next; as the only one, it is found by being the only
// one.
func TestASoleEphemeralConsumerIsCheckedAcrossRuns(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	seen := &startups{}
	built, recorded := quirkySandbox(t, config, quirks{ephemeral: true, idempotent: true, seen: seen}, nil,
		"ORD-EPHEMERAL-1")

	session := &results{inner: built}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: recorded,
		Consumer: observedConsumer,
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if violations := result.Violations(); len(result.Gates) == 0 || len(violations) != 0 {
		t.Fatalf("gates = %v, violations = %v, want every gate held\n%s", result.Gates, violations, result)
	}

	for at, run := range session.runs {
		if run.Delivered == 0 {
			t.Errorf("run %d delivered nothing", at+1)
		}
	}

	names := seen.consumerNames()
	t.Logf("%d starts, consumers %q", len(names), names)

	if len(names) < 2 {
		t.Fatalf("the service started %d times, want at least two to compare", len(names))
	}

	for at := 1; at < len(names); at++ {
		if names[at] == names[at-1] {
			t.Errorf("starts %d and %d both named the consumer %q, want a fresh name each start", at, at+1, names[at])
		}
	}
}

// TestARefusedStartupRequestReachesHealth: a request the bus refused before the first delivery is the
// likeliest reason a service never consumed, so its code and description reach the report.
func TestARefusedStartupRequestReachesHealth(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := quirkySandbox(t, config, quirks{conflictingStream: true, idempotent: true}, nil,
		"ORD-REFUSED-1")

	result, err := check.Run(t.Context(), built, check.Options{
		Messages: recorded,
		Consumer: observedConsumer,
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if result.Health == nil {
		t.Fatal("Health = nil")
	}

	refused := slices.ContainsFunc(result.Health.Refusals, func(refusal effect.Refusal) bool {
		return refusal.ErrCode == 10058 && refusal.Description != ""
	})
	if !refused {
		t.Errorf("Health.Refusals = %+v, want the 10058 refusal with its description", result.Health.Refusals)
	}

	if rendered := result.String(); !strings.Contains(rendered, "10058") {
		t.Errorf("the report does not name 10058:\n%s", rendered)
	}
}
