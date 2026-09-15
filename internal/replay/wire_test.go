package replay_test

import (
	"errors"
	"testing"
	"time"

	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
)

const targetSeq = 7

// TestWirePolicyWithholdsTheSameAcksTheDriverWould is the point of the adapter: one rule decides a
// fault in both models. A second copy of it would drift, and the two drivers would then inject
// different things under the same fault name.
func TestWirePolicyWithholdsTheSameAcksTheDriverWould(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mutation replay.Mutation
		name     string
		withheld []uint64
		attempts []uint64
	}{
		{
			name:     "clean withholds nothing",
			mutation: replay.Clean{},
			attempts: []uint64{1, 2, 3},
		},
		{
			name:     "duplicate withholds the first attempt only",
			mutation: replay.Duplicate{Seq: targetSeq},
			attempts: []uint64{1, 2, 3},
			withheld: []uint64{1},
		},
		{
			name:     "crash before ack withholds every attempt it is configured for",
			mutation: replay.CrashBeforeAck{Seq: targetSeq, Times: 2},
			attempts: []uint64{1, 2, 3},
			withheld: []uint64{1, 2},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			policy, err := replay.NewWirePolicy(testCase.mutation)
			if err != nil {
				t.Fatalf("NewWirePolicy() error = %v", err)
			}

			var withheld []uint64

			for _, attempt := range testCase.attempts {
				ack := natsproxy.Ack{StreamSeq: targetSeq, Deliveries: attempt}

				if policy.Withhold(ack) {
					withheld = append(withheld, attempt)
				}

				// The same rule the driver applies: another message's acknowledgement is never the
				// target, however many times it is delivered.
				if policy.Withhold(natsproxy.Ack{StreamSeq: targetSeq + 1, Deliveries: attempt}) {
					t.Error("withheld an acknowledgement for a message that was not the target")
				}
			}

			if !equalSeqs(withheld, testCase.withheld) {
				t.Errorf("withheld attempts %v, want %v", withheld, testCase.withheld)
			}
		})
	}
}

// TestWirePolicyRefusesWhatItCannotInject keeps the gap visible. Delay and reorder act on a delivery
// before the service sees it, and by the time the proxy could act it has already been handed over.
func TestWirePolicyRefusesWhatItCannotInject(t *testing.T) {
	t.Parallel()

	unsupported := []replay.Mutation{
		replay.Delay{Seq: targetSeq, For: time.Second},
		replay.Reorder{First: targetSeq},
		replay.Concurrent{First: targetSeq, Second: targetSeq + 1},
	}

	for _, mutation := range unsupported {
		policy, err := replay.NewWirePolicy(mutation)
		if !errors.Is(err, replay.ErrUnsupported) {
			t.Errorf("NewWirePolicy(%s) error = %v, want ErrUnsupported", mutation.Fault(), err)
		}

		if policy != nil {
			t.Errorf("NewWirePolicy(%s) returned a policy alongside its refusal", mutation.Fault())
		}
	}
}

func equalSeqs(got, want []uint64) bool {
	if len(got) != len(want) {
		return false
	}

	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}

	return true
}
