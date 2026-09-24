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
	run := newObservedRun("clean-1", stream, wire, recorder, time.Millisecond)
	run.stage([]corpus.Staged{{Recorded: 1, Sequence: 1}})
	run.scope("under_test", time.Second)

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

// TestAFedBackDeliveryIsNeverAttributedToACorpusMessage: a service can publish into the stream it
// consumes, and the bus then delivers that message too, at a sequence Stutter never staged. Passing
// that sequence off as a recorded one attributed the service's own output to a corpus message — and,
// where the numbers collided, aimed that message's fault at it.
func TestAFedBackDeliveryIsNeverAttributedToACorpusMessage(t *testing.T) {
	t.Parallel()

	const stream = "CORPUS"

	// Recorded message 2 is staged alone, at sequence 1. Sequence 2 is then the service's own output,
	// and 2 is also the recorded sequence the fault is aimed at.
	wire, err := replay.NewWirePolicy(replay.Duplicate{Seq: 2})
	if err != nil {
		t.Fatalf("NewWirePolicy() error = %v", err)
	}

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), make([]byte, 32))
	run := newObservedRun("duplicate-1", stream, wire, recorder, time.Millisecond)
	run.stage([]corpus.Staged{{Recorded: 2, Sequence: 1}})
	run.scope("under_test", time.Second)

	if got := run.sequence(2); got != 0 {
		t.Errorf("sequence(2) = %d, want the reserved identity 0 — an unstaged message was passed off as "+
			"recorded message %d", got, got)
	}

	fedBack := natsproxy.Ack{Stream: stream, Consumer: "under_test", StreamSeq: 2, Deliveries: 1}

	if run.Withhold(fedBack) {
		t.Error("Withhold() swallowed a fed-back message's acknowledgement: the fault aimed at recorded " +
			"message 2 landed on the service's own output")
	}
}
