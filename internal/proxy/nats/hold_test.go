package nats_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// heldStream is the stream the hold tests deliver from.
const heldStream = "HELD"

// pulls records the pull requests the proxy reported.
type pulls struct {
	seen []natsproxy.Pull
	mu   sync.Mutex
}

func (p *pulls) Pulled(pull natsproxy.Pull) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.seen = append(p.seen, pull)
}

func (p *pulls) observed() []natsproxy.Pull {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]natsproxy.Pull(nil), p.seen...)
}

// startWatched puts a proxy with the given hooks in front of upstream.
func startWatched(t *testing.T, upstream string, opts natsproxy.Options) (*recorder, string) {
	t.Helper()

	sink := &recorder{}

	front, err := natsproxy.ListenWith(t.Context(), "127.0.0.1:0", upstream, sink, opts)
	if err != nil {
		t.Fatalf("ListenWith() error = %v", err)
	}

	served := make(chan error, 1)
	go func() { served <- front.Serve(t.Context()) }()

	t.Cleanup(func() {
		if err := front.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}

		if err := <-served; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	})

	return sink, front.Addr()
}

// stocked creates the held stream and a pull consumer on it, on a direct connection, holding one
// message per payload.
func stocked(t *testing.T, bus string, payloads ...string) {
	t.Helper()

	direct, err := jetstream.New(connect(t, bus))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	if _, err := direct.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     heldStream,
		Subjects: []string{"held.>"},
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}

	if _, err := direct.CreateOrUpdateConsumer(t.Context(), heldStream, jetstream.ConsumerConfig{
		Durable:   "held",
		AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	for _, message := range payloads {
		if _, err := direct.Publish(t.Context(), "held.orders", []byte(message)); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}
}

// TestAHeldConnectionForwardsInOrderOnRelease: while the corpus is published nothing reaches the
// service, yet a delivery is noted as soon as it is read; on release every frame arrives, in order,
// none dropped.
func TestAHeldConnectionForwardsInOrderOnRelease(t *testing.T) {
	t.Parallel()

	bus := embeddedBus(t).Addr().String()
	stocked(t, bus, "ORD-1", "ORD-2", "ORD-3")

	hold := &natsproxy.Hold{}
	watched := &deliveries{}
	_, addr := startWatched(t, bus, natsproxy.Options{Deliveries: watched, Hold: hold})

	proxied, err := jetstream.New(connect(t, addr))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	consumer, err := proxied.Consumer(t.Context(), heldStream, "held")
	if err != nil {
		t.Fatalf("Consumer() error = %v", err)
	}

	hold.Begin()

	arrived := make(chan string, 3)

	go func() {
		defer close(arrived)

		batch, fetchErr := consumer.Fetch(3, jetstream.FetchMaxWait(startupTimeout))
		if fetchErr != nil {
			return
		}

		for msg := range batch.Messages() {
			arrived <- string(msg.Data())
		}
	}()

	select {
	case got := <-arrived:
		t.Fatalf("%q reached the service while the connection was held", got)
	case <-time.After(100 * time.Millisecond):
	}

	// The proxy reads one frame, notes it, and waits to forward it; the next is read after release.
	if got := len(watched.observed()); got != 1 {
		t.Errorf("%d deliveries noted while held, want the one read before the hold stopped it", got)
	}

	hold.Release()

	var got []string
	for payload := range arrived {
		got = append(got, payload)
	}

	if len(got) != 3 || got[0] != "ORD-1" || got[1] != "ORD-2" || got[2] != "ORD-3" {
		t.Errorf("after release the service got %q, want ORD-1, ORD-2, ORD-3 in order", got)
	}
}

// TestPullRequestsReportTheirTimers: a pull consumer's client gives up on its own timers, which bound
// how long deliveries may be held; they are read off the pull request, which stays bookkeeping.
func TestPullRequestsReportTheirTimers(t *testing.T) {
	t.Parallel()

	bus := embeddedBus(t).Addr().String()
	stocked(t, bus)

	requests := &pulls{}
	sink, addr := startWatched(t, bus, natsproxy.Options{Pulls: requests})

	proxied, err := jetstream.New(connect(t, addr))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	consumer, err := proxied.Consumer(t.Context(), heldStream, "held")
	if err != nil {
		t.Fatalf("Consumer() error = %v", err)
	}

	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(200*time.Millisecond),
		jetstream.FetchHeartbeat(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}

	for range batch.Messages() {
		t.Error("the empty stream delivered a message")
	}

	want := natsproxy.Pull{
		Stream:    heldStream,
		Consumer:  "held",
		Expires:   200 * time.Millisecond,
		Heartbeat: 50 * time.Millisecond,
	}

	if got := requests.observed(); len(got) != 1 || got[0] != want {
		t.Errorf("pulls = %+v, want exactly %+v", got, want)
	}

	for _, text := range sink.texts() {
		if strings.Contains(text, "CONSUMER.MSG.NEXT") {
			t.Errorf("effect %q is a pull request, which is bookkeeping", text)
		}
	}
}

// TestACoreDeliveryIsReported: a message handed to a core subscription is not a consumer delivery,
// yet it is reported, so a core subscriber on a corpus subject cannot go unseen.
func TestACoreDeliveryIsReported(t *testing.T) {
	t.Parallel()

	bus := embeddedBus(t).Addr().String()
	watched := &deliveries{}
	_, addr := startWatched(t, bus, natsproxy.Options{Deliveries: watched})

	subscriber := connect(t, addr)

	sub, err := subscriber.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("SubscribeSync() error = %v", err)
	}

	flush(t, subscriber)

	if err := connect(t, bus).Publish(subject, []byte(payload)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if _, err := sub.NextMsg(startupTimeout); err != nil {
		t.Fatalf("NextMsg() error = %v", err)
	}

	if got := watched.coreSubjects(); len(got) != 1 || got[0] != subject {
		t.Errorf("core deliveries = %q, want exactly [%s]", got, subject)
	}

	if got := watched.observed(); len(got) != 0 {
		t.Errorf("consumer deliveries = %d, want none: a core message is no consumer's", len(got))
	}
}
