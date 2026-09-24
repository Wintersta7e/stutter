package nats_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// apiRequests records the JetStream API subjects the proxy reported.
type apiRequests struct {
	subjects []string
	mu       sync.Mutex
}

func (r *apiRequests) Requested(subject string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.subjects = append(r.subjects, subject)
}

func (r *apiRequests) reported() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.subjects...)
}

// TestAJetStreamRequestIsReported: every JetStream API request a client sends is reported, pull
// requests included, and nothing else is — an acknowledgement is delivery bookkeeping and a core
// publish is the service's own traffic, neither of them a sign that it has touched JetStream.
func TestAJetStreamRequestIsReported(t *testing.T) {
	t.Parallel()

	bus := embeddedBus(t)
	seen := &apiRequests{}
	_, addr := startWatched(t, bus.Addr().String(), natsproxy.Options{Requests: seen})

	conn := connect(t, addr)

	stream, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	if _, createErr := stream.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
	}); createErr != nil {
		t.Fatalf("CreateStream() error = %v", createErr)
	}

	if _, infoErr := stream.AccountInfo(t.Context()); infoErr != nil {
		t.Fatalf("AccountInfo() error = %v", infoErr)
	}

	consumer, err := stream.CreateOrUpdateConsumer(t.Context(), "ORDERS", jetstream.ConsumerConfig{
		Durable:   "reserve",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	empty, err := consumer.Fetch(1, jetstream.FetchMaxWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}

	for range empty.Messages() {
		t.Fatal("the empty stream handed over a message")
	}

	if publishErr := conn.Publish("orders.created", []byte(`{"order_id":"ORD-1"}`)); publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	fetched, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}

	for message := range fetched.Messages() {
		if err := message.Ack(); err != nil {
			t.Fatalf("Ack() error = %v", err)
		}
	}

	flush(t, conn)

	subjects := seen.reported()
	t.Logf("requests reported: %d", len(subjects))

	if len(subjects) == 0 {
		t.Fatal("requests reported: 0")
	}

	assertOnlyAPIRequests(t, subjects)
	assertReported(t, subjects)
}

func assertOnlyAPIRequests(t *testing.T, subjects []string) {
	t.Helper()

	for _, subject := range subjects {
		if strings.HasPrefix(subject, "$JS.ACK.") || strings.HasPrefix(subject, "orders.") {
			t.Errorf("reported %q, which is no JetStream API request", subject)
		}

		if !strings.HasPrefix(subject, "$JS.API.") {
			t.Errorf("reported %q, want a $JS.API. subject", subject)
		}
	}
}

func assertReported(t *testing.T, subjects []string) {
	t.Helper()

	var created, info, pulled bool

	for _, subject := range subjects {
		created = created || subject == "$JS.API.STREAM.CREATE.ORDERS"
		info = info || subject == "$JS.API.INFO"
		pulled = pulled || strings.HasPrefix(subject, "$JS.API.CONSUMER.MSG.NEXT.")
	}

	if !created || !info || !pulled {
		t.Errorf("stream create %t, account info %t, pull request %t; want all reported (got %q)",
			created, info, pulled, subjects)
	}
}
