package harness_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// observedConsumer is the consumer the service under test creates for itself. It is a fixed name on
// purpose: a real service's consumer is part of its configuration, not something Stutter picks per
// run, and rebuilding the stream is what stops a second run resuming where the first stopped.
const observedConsumer = "reserve_stock_wire"

// observedAckWait is the deadline a withheld acknowledgement waits out before the bus redelivers.
// The drain wait is deliberately left to the sandbox to derive from it, because that derivation is
// what a real caller gets and a hand-picked number here would never exercise it.
const observedAckWait = 500 * time.Millisecond

// fetchWait bounds one pull. It is short so the service stops promptly when the run is over, and it
// is what makes the service send a pull request per message rather than one for the whole run.
const fetchWait = 100 * time.Millisecond

// pulling is a service Stutter does not dispatch to. It creates its own JetStream consumer, fetches
// its own messages and acknowledges them on its own connection — the shape a provisioned container
// has, reduced to the parts that decide whether Stutter can still fault it.
//
// Its one side effect is a line to a dependency on a protocol Stutter does not parse, so the run is
// observed entirely through the proxies and needs no database.
type pulling struct {
	connection *nats.Conn
	consumer   jetstream.Consumer
	dependency net.Conn
	replies    *bufio.Reader
	done       chan struct{}
	stopped    chan struct{}
}

// startPulling connects the service to the proxied bus and lets it start consuming.
//
// Every address it dials is a proxy's. A service that reached a real dependency directly would
// produce no observable effects at all, which reads as a handler that did nothing.
func startPulling(
	ctx context.Context,
	at harness.Addresses,
	config policy.Config,
) (*pulling, error) {
	dialer := net.Dialer{Timeout: time.Second}

	dependency, err := dialer.DialContext(ctx, "tcp", at.Opaque["cache"])
	if err != nil {
		return nil, fmt.Errorf("dial the unparsed dependency: %w", err)
	}

	service := &pulling{
		dependency: dependency,
		replies:    bufio.NewReader(dependency),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}

	service.connection, err = nats.Connect(at.NATS)
	if err != nil {
		return nil, fmt.Errorf("connect to the bus: %w", err)
	}

	stream, err := jetstream.New(service.connection)
	if err != nil {
		return nil, fmt.Errorf("open jetstream: %w", err)
	}

	service.consumer, err = stream.CreateOrUpdateConsumer(ctx, corpus.StreamName, jetstream.ConsumerConfig{
		Name:          observedConsumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       config.AckWait,
		MaxDeliver:    config.MaxDeliver,
		MaxAckPending: config.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("create the consumer: %w", err)
	}

	go service.pump()

	return service, nil
}

// Close stops pulling and waits for the pump before the connections go, so the proxies are not torn
// down underneath a delivery still being worked on.
func (p *pulling) Close(context.Context) {
	close(p.done)
	<-p.stopped

	p.connection.Close()

	_ = p.dependency.Close()
}

// pump pulls one message at a time for as long as the run lasts, which is what a real pull consumer
// does and what makes a redelivery arrive on its own rather than being handed over.
func (p *pulling) pump() {
	defer close(p.stopped)

	for {
		select {
		case <-p.done:
			return
		default:
		}

		batch, err := p.consumer.Fetch(1, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			return
		}

		for msg := range batch.Messages() {
			p.handle(msg)
		}
	}
}

// handle reserves stock once per delivery, which is the planted bug: the bus is permitted to deliver
// the same message twice, and this service reserves twice when it does.
func (p *pulling) handle(msg jetstream.Msg) {
	settle := msg.Ack
	if err := p.reserve(msg.Data()); err != nil {
		settle = msg.Nak
	}

	//nolint:errcheck // a settle that fails is the run ending underneath the service, and the proxy
	// reports that; retrying it here would add a delivery the run never asked for.
	_ = settle()
}

func (p *pulling) reserve(payload []byte) error {
	var order struct {
		OrderID string `json:"order_id"`
		Qty     int    `json:"qty"`
	}

	if err := json.Unmarshal(payload, &order); err != nil {
		return fmt.Errorf("decode the order: %w", err)
	}

	if _, err := fmt.Fprintf(p.dependency, "RESERVE %s %d\n", order.OrderID, order.Qty); err != nil {
		return fmt.Errorf("reserve stock: %w", err)
	}

	if _, err := p.replies.ReadString('\n'); err != nil {
		return fmt.Errorf("read the reservation: %w", err)
	}

	return nil
}

// observedSandbox builds a sandbox around a service that consumes for itself, and stocks the corpus
// with one message per order id.
func observedSandbox(
	t *testing.T,
	config policy.Config,
	orders ...string,
) (*harness.Sandbox, []uint64) {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	recorded := make([]uint64, 0, len(orders))

	for _, order := range orders {
		seq, publishErr := store.Publish(
			t.Context(),
			toy.SubjectOrderCreated,
			orderPayload(order, "WIDGET-WIRE"),
		)
		if publishErr != nil {
			t.Fatalf("Publish() error = %v", publishErr)
		}

		recorded = append(recorded, seq)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	upstream := startLineDependency(t)

	built, err := harness.New(harness.Config{
		Corpus:  store,
		Opaque:  map[string]string{"cache": upstream},
		HashKey: key,
		Policy:  config,
		Quiesce: toy.DefaultQuiesce,
		Start: func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			service, startErr := startPulling(ctx, at, config)
			if startErr != nil {
				return nil, startErr
			}

			return service, nil
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built, recorded
}

// observedConfig is the recorded consumer's own configuration. In this run model it is also the
// service's: Stutter no longer creates the consumer, so the two have to agree or the faults the
// legality table licenses are not the ones the bus will actually commit.
func observedConfig() policy.Config {
	return policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{filterAll},
		AckWait:        observedAckWait,
		MaxDeliver:     3,
		MaxAckPending:  1,
	}
}

// TestObservedServiceIsFaultedOnTheWire is the run model a provisioned container needs: Stutter
// dispatches nothing and has no acknowledgement of its own to withhold, so the delivery is watched
// and the fault injected by swallowing the service's own acknowledgement on the wire.
//
// It needs no database and no container — only a service that consumes for itself.
func TestObservedServiceIsFaultedOnTheWire(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := observedSandbox(t, config, "ORD-WIRE-1")

	session := &counting{inner: built}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: recorded,
		Consumer: observedConsumer,
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if session.faults == 0 {
		t.Fatal("no fault was injected, so the finding proves nothing")
	}

	if len(result.Gates) != 1 || !result.Gates[0].Result.OK() {
		t.Fatalf("the determinism gate did not hold for an undriven service:\n%s", result)
	}

	if len(result.Findings) != 1 {
		t.Fatalf("Findings = %d, want 1\n%s", len(result.Findings), result)
	}

	found := result.Findings[0]
	if found.Status != report.StatusWarn {
		t.Errorf("Status = %q, want WARN (reservations: %v)", found.Status, found.Reservations)
	}

	if !strings.Contains(result.String(), "opaque dependency=cache") {
		t.Errorf("report did not name the unparsed dependency:\n%s", result)
	}
}

// TestAWholeCorpusRunFollowsAScopedOne is the property staging would destroy if the corpus were
// re-read from the stream each run.
//
// A check interleaves whole-corpus runs with the subsets a shrink asks for, and staging republishes
// only the messages it was handed — so a caller that read the stream back would find the corpus
// already reduced to the last subset it staged, and every run after the first shrink would compare
// against a corpus that was never delivered.
func TestAWholeCorpusRunFollowsAScopedOne(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := observedSandbox(t, config, "ORD-WIRE-1", "ORD-WIRE-2")

	scoped, err := built.Run(t.Context(), "scoped", replay.Clean{}, []uint64{recorded[1]})
	if err != nil {
		t.Fatalf("Run() scoped error = %v", err)
	}

	if scoped.Delivered != 1 {
		t.Fatalf("the scoped run delivered %d messages, want 1", scoped.Delivered)
	}

	whole, err := built.Run(t.Context(), "whole", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() over the whole corpus after a scoped one: %v", err)
	}

	if whole.Delivered != len(recorded) {
		t.Fatalf("the whole-corpus run delivered %d messages, want %d — the scoped run took the rest "+
			"of the corpus with it", whole.Delivered, len(recorded))
	}
}

// TestAScopedObservedRunFaultsTheRecordedSequence covers the translation a rebuilt stream forces.
//
// Scoping an observed run means restaging the corpus with only the retained messages, and staging
// renumbers from one. A fault built against a recorded sequence would otherwise be aimed at a
// message that is no longer there — which a shrink would read as "this subset does not reproduce",
// leaving every finding without a minimal repro.
func TestAScopedObservedRunFaultsTheRecordedSequence(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := observedSandbox(t, config, "ORD-WIRE-1", "ORD-WIRE-2")

	second := recorded[1]
	if second == 1 {
		t.Fatal("the second message was recorded at sequence 1, so staging cannot renumber it")
	}

	result, err := built.Run(t.Context(), "scoped", replay.Duplicate{Seq: second}, []uint64{second})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Delivered != 2 {
		t.Fatalf("Delivered = %d, want 2 — the duplicate never landed on the retained message",
			result.Delivered)
	}

	if len(result.Effects) != 2 {
		t.Fatalf("Effects = %d, want 2 reservations\n%v", len(result.Effects), result.Effects)
	}

	// A pull consumer asks for its next message for as long as the run lasts, so anything of its own
	// delivery machinery that reached the sequence would arrive after the last window closed. A
	// non-zero count here is the sequence filling up with traffic the handler never chose to send.
	if result.Late != 0 {
		t.Errorf("Late = %d, want 0 — the run recorded delivery bookkeeping as the handler's work",
			result.Late)
	}

	for at, observed := range result.Effects {
		if observed.MessageSeq != second {
			t.Errorf("effect %d attributed to message %d, want the recorded sequence %d",
				at, observed.MessageSeq, second)
		}

		if !strings.Contains(observed.Printable, "ORD-WIRE-2") {
			t.Errorf("effect %d = %q, want the retained message's order", at, observed.Printable)
		}
	}
}
