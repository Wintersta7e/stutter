package policy_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/policy"
)

// sendingConsumer mirrors the shape of a real sending consumer in a production bulk-mail service:
// an explicit ack, a three-entry backoff curve sized to outlast an SMTP transaction, and a capped
// redelivery count.
func sendingConsumer() policy.Config {
	return policy.Config{
		BackOff:        []time.Duration{2 * time.Minute, 5 * time.Minute, 15 * time.Minute},
		FilterSubjects: []string{"mail.sending"},
		AckMode:        policy.AckExplicit,
		AckWait:        2 * time.Minute,
		MaxDeliver:     5,
		MaxAckPending:  1,
	}
}

func TestDeadlineFollowsTheBackoffCurve(t *testing.T) {
	t.Parallel()

	config := sendingConsumer()

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 2 * time.Minute},
		{attempt: 2, want: 5 * time.Minute},
		{attempt: 3, want: 15 * time.Minute},
		// The last entry governs every attempt through MaxDeliver.
		{attempt: 4, want: 15 * time.Minute},
		{attempt: 99, want: 15 * time.Minute},
		// Defensive: a caller counting from zero must not index out of range.
		{attempt: 0, want: 2 * time.Minute},
	}

	for _, testCase := range cases {
		if got := config.Deadline(testCase.attempt); got != testCase.want {
			t.Errorf("Deadline(%d) = %v, want %v", testCase.attempt, got, testCase.want)
		}
	}
}

// TestBackoffOverridesAckWait encodes a real production incident. A consumer declared a generous
// AckWait while a service-wide backoff curve gave every message a one-second first deadline; the
// server redelivered nearly everything and roughly one delivery in six was duplicated. Reading
// AckWait here is not a harmless approximation — it is the bug.
func TestBackoffOverridesAckWait(t *testing.T) {
	t.Parallel()

	misconfigured := policy.Config{
		BackOff:       []time.Duration{time.Second, 5 * time.Minute},
		AckMode:       policy.AckExplicit,
		AckWait:       2 * time.Minute, // what the config says
		MaxDeliver:    5,
		MaxAckPending: 1,
	}

	if got := misconfigured.Deadline(1); got != time.Second {
		t.Errorf("Deadline(1) = %v, want 1s; a delay mutation sized to AckWait would model nothing", got)
	}
}

func TestDeadlineFallsBackToAckWaitWithoutBackoff(t *testing.T) {
	t.Parallel()

	config := policy.Config{AckMode: policy.AckExplicit, AckWait: 30 * time.Second, MaxDeliver: 3}

	if got := config.Deadline(4); got != 30*time.Second {
		t.Errorf("Deadline() = %v, want the declared AckWait of 30s", got)
	}
}

func TestPermitsDerivesFaultsFromConfig(t *testing.T) {
	t.Parallel()

	ordered := sendingConsumer()

	concurrent := sendingConsumer()
	concurrent.MaxAckPending = 16

	atMostOnce := sendingConsumer()
	atMostOnce.MaxDeliver = 1

	unacknowledged := sendingConsumer()
	unacknowledged.AckMode = policy.AckNone

	cases := []struct {
		name   string
		fault  policy.Fault
		config policy.Config
		want   bool
	}{
		{name: "duplicate under explicit ack", config: ordered, fault: policy.FaultDuplicate, want: true},
		{name: "crash before ack under explicit ack", config: ordered, fault: policy.FaultCrashBeforeAck, want: true},
		{name: "delay under explicit ack", config: ordered, fault: policy.FaultDelay, want: true},
		{name: "reorder needs concurrency", config: ordered, fault: policy.FaultReorder, want: false},
		{name: "concurrent needs concurrency", config: ordered, fault: policy.FaultConcurrent, want: false},
		{name: "drop is a lie when the bus retries", config: ordered, fault: policy.FaultDrop, want: false},

		{name: "reorder with headroom", config: concurrent, fault: policy.FaultReorder, want: true},
		{name: "concurrent with headroom", config: concurrent, fault: policy.FaultConcurrent, want: true},

		{name: "no redelivery at MaxDeliver 1", config: atMostOnce, fault: policy.FaultDuplicate, want: false},
		{name: "drop is real at MaxDeliver 1", config: atMostOnce, fault: policy.FaultDrop, want: true},

		{name: "no redelivery without acks", config: unacknowledged, fault: policy.FaultDuplicate, want: false},
		{name: "drop is real without acks", config: unacknowledged, fault: policy.FaultDrop, want: true},

		{name: "unknown fault is refused", config: ordered, fault: policy.Fault("teleport"), want: false},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			verdict := testCase.config.Permits(testCase.fault)
			if verdict.Permitted != testCase.want {
				t.Errorf("Permits(%s) = %v, want %v (clause: %s)",
					testCase.fault, verdict.Permitted, testCase.want, verdict.Clause)
			}

			if verdict.Clause == "" {
				t.Error("verdict carries no clause; a finding could not say why the fault was legal")
			}
		})
	}
}

// TestDuplicateIgnoresThePublishDedupeWindow guards a correction the design needed. A stream's
// duplicate window deduplicates ingestion of the same message id, not delivery: every redelivery of
// an already-stored message carries the same stream sequence and arrives regardless. Treating that
// window as a reason to suppress duplicate findings would hide real bugs.
func TestDuplicateIgnoresThePublishDedupeWindow(t *testing.T) {
	t.Parallel()

	if verdict := sendingConsumer().Permits(policy.FaultDuplicate); !verdict.Permitted {
		t.Errorf("duplicate was refused: %s", verdict.Clause)
	}
}

func TestReorderAcrossSubjectsNamesTheSubjectCount(t *testing.T) {
	t.Parallel()

	config := sendingConsumer()
	config.MaxAckPending = 8
	config.FilterSubjects = []string{"orders.created", "orders.cancelled"}

	verdict := config.Permits(policy.FaultReorder)
	if !verdict.Permitted {
		t.Fatalf("reorder across subjects was refused: %s", verdict.Clause)
	}

	if want := "2 filter subjects"; !strings.Contains(verdict.Clause, want) {
		t.Errorf("clause = %q, want it to mention %q", verdict.Clause, want)
	}
}
