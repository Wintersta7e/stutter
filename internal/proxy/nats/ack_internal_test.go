package nats

import (
	"testing"
	"time"
)

// TestADelayedNakRefusesTheDelivery classifies acknowledgement payloads the way the server matches
// them: a NAK by prefix, because a client asking for redelivery later sends `-NAK {"delay": …}` or
// `-NAK <duration>`, and matching the bare word read that refusal as a settled delivery.
//
// Anything that is not a settle, a NAK or a work-in-progress acknowledgement is ignored, as the server
// ignores it. A work-in-progress acknowledgement settles nothing:
// swallowing one injects a redelivery the run never chose, and closing an attribution window on one
// reports the rest of a handler's own work against the next message. A negative one is the only way a
// service Stutter does not call can say a delivery failed.
func TestADelayedNakRefusesTheDelivery(t *testing.T) {
	t.Parallel()

	cases := []struct {
		payload    string
		inProgress bool
		negative   bool
	}{
		{payload: ""},
		{payload: "+ACK"},
		{payload: "+OK"},
		{payload: "+NXT"},
		{payload: `+NXT {"batch":2}`},
		{payload: "+TERM"},
		{payload: "+TERM no longer wanted"},
		{payload: "-NAK", negative: true},
		{payload: `-NAK {"delay": 200000000}`, negative: true},
		{payload: "-NAK 5s", negative: true},
		// The server treats an unparseable delay as a plain NAK, so it is still a refusal.
		{payload: "-NAK soon", negative: true},
		{payload: "+WPI", inProgress: true},
	}

	t.Logf("%d acknowledgement payloads classified", len(cases))

	if len(cases) == 0 {
		t.Fatal("no payloads to classify")
	}

	for _, testCase := range cases {
		t.Run(testCase.payload, func(t *testing.T) {
			t.Parallel()

			ack := Ack{Payload: []byte(testCase.payload)}

			if got := ack.InProgress(); got != testCase.inProgress {
				t.Errorf("InProgress() = %v, want %v", got, testCase.inProgress)
			}

			if got := ack.Negative(); got != testCase.negative {
				t.Errorf("Negative() = %v, want %v", got, testCase.negative)
			}
		})
	}
}

// TestAcknowledgementsAreClassifiedAsTheServerDoes: a run ends when every message is settled, so what
// settles a message has to be exactly what the server settles on. Anything the server ignores — a
// trailing space, an unknown verb — leaves the message owed, and a NAK's delay is when its redelivery
// will land.
func TestAcknowledgementsAreClassifiedAsTheServerDoes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		payload    string
		nakDelay   time.Duration
		settles    bool
		negative   bool
		inProgress bool
	}{
		{payload: "", settles: true},
		{payload: "+ACK", settles: true},
		{payload: "+OK", settles: true},
		{payload: "+NXT", settles: true},
		{payload: `+NXT {"batch":2}`, settles: true},
		{payload: "+TERM", settles: true},
		{payload: "+TERM gone", settles: true},
		{payload: "-NAK", negative: true},
		{payload: `-NAK {"delay":200000000}`, negative: true, nakDelay: 200 * time.Millisecond},
		{payload: "-NAK 5s", negative: true, nakDelay: 5 * time.Second},
		// The server treats an unparseable delay as a plain NAK.
		{payload: "-NAK soon", negative: true},
		{payload: "+WPI", inProgress: true},
		// The server matches these exactly or not at all, and ignores what it does not match.
		{payload: "+ACK "},
		{payload: "+WPIX"},
		{payload: "hello"},
	}

	t.Logf("%d acknowledgement payloads classified", len(cases))

	if len(cases) == 0 {
		t.Fatal("no payloads to classify")
	}

	for _, testCase := range cases {
		t.Run(testCase.payload, func(t *testing.T) {
			t.Parallel()

			ack := Ack{Payload: []byte(testCase.payload)}

			if got := ack.Settles(); got != testCase.settles {
				t.Errorf("Settles() = %v, want %v", got, testCase.settles)
			}

			if got := ack.Negative(); got != testCase.negative {
				t.Errorf("Negative() = %v, want %v", got, testCase.negative)
			}

			if got := ack.InProgress(); got != testCase.inProgress {
				t.Errorf("InProgress() = %v, want %v", got, testCase.inProgress)
			}

			if got := ack.NakDelay(); got != testCase.nakDelay {
				t.Errorf("NakDelay() = %s, want %s", got, testCase.nakDelay)
			}
		})
	}
}

// TestPullRequestsAreBookkeeping keeps the effect sequence to what the handler chose to send.
//
// A pull consumer asks the bus for its next message for as long as the run lasts, so how many of
// these it sends depends on its client library's batching and on how long the run took. Recorded as
// effects they fill the sequence with traffic that differs between two identical runs, and the
// determinism gate then fails on Stutter's own delivery machinery.
func TestPullRequestsAreBookkeeping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		subject string
		want    bool
	}{
		{subject: "$JS.ACK.ORDERS.worker.1.1.1.1700000000000000000.0", want: true},
		{subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.worker", want: true},
		// A handler that creates a stream or a bucket did something, and hiding it would report that
		// handler as having done nothing at all.
		{subject: "$JS.API.STREAM.CREATE.ORDERS"},
		{subject: "$KV.claims.ORD-1"},
		{subject: "orders.shipped"},
	}

	for _, testCase := range cases {
		t.Run(testCase.subject, func(t *testing.T) {
			t.Parallel()

			if got := isBookkeeping(testCase.subject); got != testCase.want {
				t.Errorf("isBookkeeping(%q) = %v, want %v", testCase.subject, got, testCase.want)
			}
		})
	}
}

// TestParseAckReadsBothSubjectShapes covers the shape the live test cannot reach: a server with a
// JetStream domain publishes acknowledgements with two extra leading tokens, and reading them
// positionally from the front would silently pick the domain as the stream and withhold the wrong
// message.
func TestParseAckReadsBothSubjectShapes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		subject  string
		stream   string
		consumer string
		seq      uint64
		attempts uint64
		ok       bool
	}{
		{
			name:     "without a domain",
			subject:  "$JS.ACK.ORDERS.worker.2.7.5.1700000000000000000.3",
			stream:   "ORDERS",
			consumer: "worker",
			attempts: 2,
			seq:      7,
			ok:       true,
		},
		{
			name:     "with a domain and account hash",
			subject:  "$JS.ACK.hub.ACCHASH.ORDERS.worker.2.7.5.1700000000000000000.3",
			stream:   "ORDERS",
			consumer: "worker",
			attempts: 2,
			seq:      7,
			ok:       true,
		},
		{name: "not an acknowledgement", subject: "orders.created"},
		{name: "wrong token count", subject: "$JS.ACK.ORDERS.worker.2.7"},
		{name: "unparseable sequence", subject: "$JS.ACK.ORDERS.worker.2.seven.5.1700000000000000000.3"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, ok := parseAck(testCase.subject, []byte("+ACK"))
			if ok != testCase.ok {
				t.Fatalf("parseAck() ok = %v, want %v", ok, testCase.ok)
			}

			if !testCase.ok {
				return
			}

			if got.Stream != testCase.stream || got.Consumer != testCase.consumer {
				t.Errorf("named %q/%q, want %q/%q", got.Stream, got.Consumer, testCase.stream, testCase.consumer)
			}

			if got.StreamSeq != testCase.seq || got.Deliveries != testCase.attempts {
				t.Errorf("seq/deliveries = %d/%d, want %d/%d",
					got.StreamSeq, got.Deliveries, testCase.seq, testCase.attempts)
			}
		})
	}
}
