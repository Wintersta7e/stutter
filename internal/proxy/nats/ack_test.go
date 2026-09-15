package nats_test

import (
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// swallowFirst withholds one message's first acknowledgement and forwards everything else, which is
// the wire-level form of crash-before-ack against a service that acknowledges for itself.
type swallowFirst struct {
	seen []natsproxy.Ack
	seq  uint64
	mu   sync.Mutex
}

func (s *swallowFirst) Withhold(ack natsproxy.Ack) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seen = append(s.seen, ack)

	return ack.StreamSeq == s.seq && ack.Deliveries == 1
}

func (s *swallowFirst) observed() []natsproxy.Ack {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]natsproxy.Ack(nil), s.seen...)
}

// deliveries records what the bus handed over, which is the signal an observed run opens its
// attribution window on.
type deliveries struct {
	seen []natsproxy.Delivery
	mu   sync.Mutex
}

func (d *deliveries) Delivered(delivery natsproxy.Delivery) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.seen = append(d.seen, delivery)
}

func (d *deliveries) observed() []natsproxy.Delivery {
	d.mu.Lock()
	defer d.mu.Unlock()

	return append([]natsproxy.Delivery(nil), d.seen...)
}

// TestWithheldAckIsRedelivered is the lever for a service Stutter does not drive: it acknowledges
// for itself, so the only place left to inject a redelivery is the wire.
//
// It runs against a real embedded JetStream server with a real consumer, because a withheld
// acknowledgement is only a fault if the server genuinely redelivers.
func TestWithheldAckIsRedelivered(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	seq, err := store.Publish(t.Context(), corpus.SubjectPrefix+"orders", []byte(`{"order_id":"ORD-1"}`))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	parsed, err := url.Parse(store.URL())
	if err != nil {
		t.Fatalf("parse the bus address: %v", err)
	}

	policy := &swallowFirst{seq: seq}
	observed := &recorder{}

	watched := &deliveries{}

	proxy, err := natsproxy.ListenWith(t.Context(), "127.0.0.1:0", parsed.Host, observed, natsproxy.Options{
		Acks:       policy,
		Deliveries: watched,
	})
	if err != nil {
		t.Fatalf("ListenWith() error = %v", err)
	}

	served := make(chan error, 1)
	go func() { served <- proxy.Serve(t.Context()) }()

	connection, err := nats.Connect("nats://" + proxy.Addr())
	if err != nil {
		t.Fatalf("connect through the proxy: %v", err)
	}

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
		Name:          "ack_withheld",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Second,
		MaxDeliver:    3,
		MaxAckPending: 1,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	deliveries := fetchAndAck(t, consumer, 2)

	connection.Close()

	if closeErr := proxy.Close(); closeErr != nil {
		t.Fatalf("proxy.Close() error = %v", closeErr)
	}

	if serveErr := <-served; serveErr != nil {
		t.Fatalf("proxy.Serve() error = %v", serveErr)
	}

	if len(deliveries) != 2 || deliveries[0] != seq || deliveries[1] != seq {
		t.Fatalf("delivered sequences = %v, want message %d twice", deliveries, seq)
	}

	acks := policy.observed()
	if len(acks) < 2 {
		t.Fatalf("acknowledgements seen = %d, want the withheld one and the one that got through", len(acks))
	}

	if acks[0].StreamSeq != seq || acks[0].Deliveries != 1 {
		t.Errorf("first ack = %+v, want stream sequence %d on delivery 1", acks[0], seq)
	}

	if acks[0].Stream != corpus.StreamName || acks[0].Consumer != "ack_withheld" {
		t.Errorf("ack named %q/%q, want %q/ack_withheld", acks[0].Stream, acks[0].Consumer, corpus.StreamName)
	}

	// An acknowledgement is delivery bookkeeping. Recorded as an effect it would put Stutter's own
	// fault injection into the sequence being compared, and the withheld one would show up as a
	// difference between the clean run and the faulted one.
	for _, text := range observed.texts() {
		if strings.Contains(text, "$JS.ACK.") {
			t.Errorf("an acknowledgement was recorded as an effect: %q", text)
		}
	}

	// The same two deliveries the consumer saw, seen from the wire. This is what an observed run
	// opens its attribution window on, and it has to agree with what actually arrived.
	handed := watched.observed()
	if len(handed) != 2 {
		t.Fatalf("deliveries watched = %d, want 2", len(handed))
	}

	for at, delivery := range handed {
		if delivery.Ack.StreamSeq != seq {
			t.Errorf("delivery %d was of message %d, want %d", at, delivery.Ack.StreamSeq, seq)
		}

		if delivery.Ack.Deliveries != uint64(at+1) {
			t.Errorf("delivery %d reported attempt %d", at, delivery.Ack.Deliveries)
		}

		if !strings.Contains(string(delivery.Payload), "ORD-1") {
			t.Errorf("delivery %d carried %q, want the published payload", at, delivery.Payload)
		}
	}
}

// fetchAndAck pulls count messages, acknowledging each, and returns the stream sequences it saw.
func fetchAndAck(t *testing.T, consumer jetstream.Consumer, count int) []uint64 {
	t.Helper()

	var seen []uint64

	for range count {
		batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}

		for message := range batch.Messages() {
			meta, metaErr := message.Metadata()
			if metaErr != nil {
				t.Fatalf("Metadata() error = %v", metaErr)
			}

			seen = append(seen, meta.Sequence.Stream)

			if ackErr := message.Ack(); ackErr != nil {
				t.Fatalf("Ack() error = %v", ackErr)
			}
		}

		if err := batch.Error(); err != nil {
			t.Fatalf("batch error = %v", err)
		}
	}

	return seen
}
