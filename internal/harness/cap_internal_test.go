package harness

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/policy"
)

// TestTheDeliveryCapIsOnePlusTheCrashLoopPlusOneRetry: the cap is the first delivery, the crash loop's
// withheld ones, and one retry by the service — derived, never retyped, so a longer crash loop raises
// it with no second edit.
func TestTheDeliveryCapIsOnePlusTheCrashLoopPlusOneRetry(t *testing.T) {
	t.Parallel()

	if DeliveryCap != 1+policy.CrashLoopWithheld+serviceRetries {
		t.Errorf("DeliveryCap = %d, want 1 + %d + %d", DeliveryCap, policy.CrashLoopWithheld, serviceRetries)
	}

	if DeliveryCap != 4 {
		t.Errorf("DeliveryCap = %d, want 4", DeliveryCap)
	}
}

// TestTheDeliveryCapKeepsEveryDeadline: capping deliveries trims the backoff curve, and trimming it from
// the wrong end would move every redelivery. Each attempt the cap keeps lands when the untrimmed curve
// says it would, and the attempt past the cap never comes.
func TestTheDeliveryCapKeepsEveryDeadline(t *testing.T) {
	t.Parallel()

	curve := []time.Duration{
		200 * time.Millisecond, 500 * time.Millisecond, 900 * time.Millisecond, 2 * time.Second, 5 * time.Second,
	}

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	t.Cleanup(connection.Close)

	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	consumer, err := js.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
		Durable:    "curved",
		BackOff:    curve,
		MaxDeliver: 100,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	if _, err := store.Serialise(t.Context(), "curved", DeliveryCap); err != nil {
		t.Fatalf("Serialise() error = %v", err)
	}

	if _, err := store.Publish(t.Context(), corpus.SubjectPrefix+"orders", []byte("ORD-CAP-1")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	// Attempt n lands the sum of the first n-1 entries after the first.
	want := []time.Duration{0, 200 * time.Millisecond, 700 * time.Millisecond, 1600 * time.Millisecond}
	quiet := want[len(want)-1] + curve[DeliveryCap-1] + 500*time.Millisecond

	offsets := deliveries(t, consumer, quiet)
	t.Logf("deliveries at %v after the first, watched for %s", offsets, quiet)

	if len(offsets) != len(want) {
		t.Fatalf("%d deliveries, want %d: the cap let through %v", len(offsets), len(want), offsets)
	}

	for attempt, offset := range offsets {
		if gap := (offset - want[attempt]).Abs(); gap > drainPoll {
			t.Errorf("attempt %d landed at %s, want %s within %s", attempt+1, offset, want[attempt], drainPoll)
		}
	}
}

// deliveries fetches without ever acknowledging until quiet has passed since the first delivery, and
// returns each delivery's offset from the first on the monotonic clock.
func deliveries(t *testing.T, consumer jetstream.Consumer, quiet time.Duration) []time.Duration {
	t.Helper()

	var (
		first   time.Time
		offsets []time.Duration
	)

	began := time.Now()

	for first.IsZero() || time.Since(first) < quiet {
		if first.IsZero() && time.Since(began) > quiet {
			t.Fatalf("no delivery within %s", quiet)
		}

		wait := time.Second
		if !first.IsZero() {
			wait = max(quiet-time.Since(first), time.Millisecond)
		}

		batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(wait))
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}

		for range batch.Messages() {
			if first.IsZero() {
				first = time.Now()
			}

			offsets = append(offsets, time.Since(first))
		}
	}

	return offsets
}
