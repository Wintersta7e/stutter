package harness

import (
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// TestAnotherConsumersTrafficIsNotTheRunsBusiness: two consumers on one stream both acknowledge the
// faulted sequence. Swallowing the bystander's acknowledgement would fault a handler nobody chose, and
// a delivery to it means the run's effects are mixed with a second handler's.
func TestAnotherConsumersTrafficIsNotTheRunsBusiness(t *testing.T) {
	t.Parallel()

	const stream = "CORPUS"

	wire, err := replay.NewWirePolicy(replay.Duplicate{Seq: 1})
	if err != nil {
		t.Fatalf("NewWirePolicy() error = %v", err)
	}

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), make([]byte, 32))
	run := newObservedRun(
		"clean-1",
		stream,
		wire,
		recorder,
		[]corpus.Staged{{Recorded: 1, Sequence: 1}},
		time.Millisecond,
	)
	run.scope("under_test")

	bystander := natsproxy.Ack{Stream: stream, Consumer: "bystander", StreamSeq: 1, Deliveries: 1}

	if run.Withhold(bystander) {
		t.Error("Withhold() swallowed another consumer's acknowledgement of the faulted message")
	}

	if err := run.unscoped(); err != nil {
		t.Fatalf("unscoped() = %v before any foreign delivery", err)
	}

	run.Delivered(natsproxy.Delivery{Ack: bystander})

	if err := run.unscoped(); err == nil {
		t.Error("unscoped() = nil after another consumer took a delivery during the run")
	}

	if !run.Withhold(natsproxy.Ack{Stream: stream, Consumer: "under_test", StreamSeq: 1, Deliveries: 1}) {
		t.Error("Withhold() let the consumer under test's acknowledgement of the faulted message through")
	}
}
