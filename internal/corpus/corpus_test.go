package corpus_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

const (
	ackWait   = 2 * time.Second
	fetchWait = 5 * time.Second
	subject   = corpus.SubjectPrefix + "order.created"
)

func start(t *testing.T) *corpus.Corpus {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	return store
}

func TestReplayDeliversInStreamOrder(t *testing.T) {
	t.Parallel()

	store := start(t)
	payloads := [][]byte{
		[]byte(`{"order_id":"ORD-1"}`),
		[]byte(`{"order_id":"ORD-2"}`),
		[]byte(`{"order_id":"ORD-3"}`),
	}

	for _, payload := range payloads {
		if _, err := store.Publish(t.Context(), subject, payload); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	replay, err := store.Replay(t.Context(), "order-test",
		corpus.ConsumerOptions{AckWait: ackWait, MaxDeliver: 3})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	for index, want := range payloads {
		delivery, err := replay.Next(t.Context(), fetchWait)
		if err != nil {
			t.Fatalf("Next() at index %d error = %v", index, err)
		}

		if string(delivery.Payload) != string(want) {
			t.Errorf("payload at index %d = %s, want %s", index, delivery.Payload, want)
		}

		if err := delivery.Ack(); err != nil {
			t.Fatalf("Ack() error = %v", err)
		}
	}

	if _, err := replay.Next(t.Context(), time.Second); !errors.Is(err, corpus.ErrDrained) {
		t.Errorf("Next() after the last message error = %v, want ErrDrained", err)
	}
}

// TestWithheldAckRedelivers is the mechanism the duplicate and crash-before-ack mutations are built
// on. If the bus does not redeliver a message whose ack was withheld, neither mutation exists and
// the product has nothing to test with.
func TestWithheldAckRedelivers(t *testing.T) {
	t.Parallel()

	store := start(t)

	seq, err := store.Publish(t.Context(), subject, []byte(`{"order_id":"ORD-99001","qty":3}`))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	replay, err := store.Replay(t.Context(), "redelivery-test",
		corpus.ConsumerOptions{AckWait: ackWait, MaxDeliver: 5})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	firstDelivery, err := replay.Next(t.Context(), fetchWait)
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}

	if firstDelivery.Seq != seq {
		t.Errorf("first delivery Seq = %d, want %d", firstDelivery.Seq, seq)
	}

	if firstDelivery.Deliveries != 1 {
		t.Errorf("first delivery Deliveries = %d, want 1", firstDelivery.Deliveries)
	}

	if nakErr := firstDelivery.Nak(); nakErr != nil {
		t.Fatalf("Nak() error = %v", nakErr)
	}

	redelivered, err := replay.Next(t.Context(), fetchWait)
	if err != nil {
		t.Fatalf("Next() after Nak error = %v; the bus did not redeliver", err)
	}

	if redelivered.Seq != seq {
		t.Errorf("redelivered Seq = %d, want the same message %d", redelivered.Seq, seq)
	}

	if redelivered.Deliveries != 2 {
		t.Errorf("redelivered Deliveries = %d, want 2", redelivered.Deliveries)
	}

	if string(redelivered.Payload) != `{"order_id":"ORD-99001","qty":3}` {
		t.Errorf("redelivered payload = %s, want it byte-identical to the original", redelivered.Payload)
	}

	if err := redelivered.Ack(); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	if _, err := replay.Next(t.Context(), time.Second); !errors.Is(err, corpus.ErrDrained) {
		t.Errorf("Next() after ack error = %v, want ErrDrained", err)
	}
}

func TestCorpusURLIsReachable(t *testing.T) {
	t.Parallel()

	if url := start(t).URL(); url == "" {
		t.Error("URL() is empty; a service under test could not reach the bus")
	}
}
