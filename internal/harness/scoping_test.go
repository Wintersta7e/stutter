package harness_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// The crowded service's two consumers, both reading every message on the corpus stream.
const (
	consumerReserve = "reserve_stock_crowded"
	consumerAudit   = "audit_orders_crowded"
)

// crowded has the shape of the real service that stopped the first real-target run: two consumers on
// one stream in one process, sharing a bus connection and a dependency connection, plus a cleanup pass
// that runs as the service comes up rather than in answer to any message. Each consumer writes its own
// verb to the dependency, so a run's effects say which handler produced them.
type crowded struct {
	connection *nats.Conn
	dependency net.Conn
	replies    *bufio.Reader
	done       chan struct{}
	pumps      sync.WaitGroup
	mu         sync.Mutex
}

func startCrowded(ctx context.Context, at harness.Addresses, config policy.Config) (*crowded, error) {
	dialer := net.Dialer{Timeout: time.Second}

	dependency, err := dialer.DialContext(ctx, "tcp", at.Opaque[opaqueCache])
	if err != nil {
		return nil, fmt.Errorf("dial the unparsed dependency: %w", err)
	}

	service := &crowded{
		dependency: dependency,
		replies:    bufio.NewReader(dependency),
		done:       make(chan struct{}),
	}

	service.connection, err = nats.Connect(at.NATS)
	if err != nil {
		return nil, fmt.Errorf("connect to the bus: %w", err)
	}

	stream, err := jetstream.New(service.connection)
	if err != nil {
		return nil, fmt.Errorf("open jetstream: %w", err)
	}

	verbs := map[string]string{consumerReserve: "RESERVE", consumerAudit: "AUDIT"}

	for _, name := range []string{consumerReserve, consumerAudit} {
		consumer, createErr := stream.CreateOrUpdateConsumer(ctx, corpus.StreamName, jetstream.ConsumerConfig{
			Durable:       name,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       config.AckWait,
			MaxDeliver:    config.MaxDeliver,
			MaxAckPending: config.MaxAckPending,
		})
		if createErr != nil {
			return nil, fmt.Errorf("create consumer %q: %w", name, createErr)
		}

		var afterFirstPull func()
		if name == consumerReserve {
			afterFirstPull = service.cleanup
		}

		service.pumps.Go(func() { service.pump(consumer, verbs[name], afterFirstPull) })
	}

	return service, nil
}

// Close stops both consumers before the connections go.
func (c *crowded) Close(context.Context) {
	close(c.done)
	c.pumps.Wait()

	c.connection.Close()

	_ = c.dependency.Close()
}

// pump pulls for one consumer, running afterFirstPull once the first pull has finished whatever it
// brought. Against a stream already holding the corpus, that is straight after the first message's
// acknowledgement, inside its attribution window.
func (c *crowded) pump(consumer jetstream.Consumer, verb string, afterFirstPull func()) {
	for {
		select {
		case <-c.done:
			return
		default:
		}

		batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			return
		}

		for msg := range batch.Messages() {
			c.handle(msg, verb)
		}

		if afterFirstPull != nil {
			afterFirstPull()

			afterFirstPull = nil
		}
	}
}

// cleanup is the startup pass: work the service does on its own schedule, answering no message.
func (c *crowded) cleanup() {
	//nolint:errcheck // a cleanup that fails is the run ending underneath the service; the test judges
	// what reached the dependency, not whether the service heard back.
	_ = c.call("CLEANUP expired")
}

func (c *crowded) handle(msg jetstream.Msg, verb string) {
	var order struct {
		OrderID string `json:"order_id"`
	}

	settle := msg.Ack

	if err := json.Unmarshal(msg.Data(), &order); err != nil || c.call(verb+" "+order.OrderID) != nil {
		settle = msg.Nak
	}

	//nolint:errcheck // a settle that fails is the run ending underneath the service, and the proxy
	// reports that; retrying it here would add a delivery the run never asked for.
	_ = settle()
}

// call sends one line and waits for the answer. Both consumers share the connection, as a pooled
// client does, so a line and its answer are one critical section.
func (c *crowded) call(line string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := fmt.Fprintln(c.dependency, line); err != nil {
		return fmt.Errorf("write %q: %w", line, err)
	}

	if _, err := c.replies.ReadString('\n'); err != nil {
		return fmt.Errorf("read the answer to %q: %w", line, err)
	}

	return nil
}

// crowdedSandbox builds a sandbox around the crowded service, scoped to one of its consumers.
func crowdedSandbox(t *testing.T, consumer string, orders ...string) (*harness.Sandbox, []uint64) {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	recorded := make([]uint64, 0, len(orders))

	for _, order := range orders {
		seq, publishErr := store.Publish(t.Context(), toy.SubjectOrderCreated, orderPayload(order, "WIDGET-CROWDED"))
		if publishErr != nil {
			t.Fatalf("Publish() error = %v", publishErr)
		}

		recorded = append(recorded, seq)
	}

	config := observedConfig()

	built, err := harness.New(harness.Config{
		Corpus:   store,
		Opaque:   map[string]string{opaqueCache: startLineDependency(t)},
		Consumer: consumer,
		HashKey:  make([]byte, hashKeyLen),
		Policy:   config,
		Quiesce:  toy.DefaultQuiesce,
		Start: func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			service, startErr := startCrowded(ctx, at, config)
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

// verbs counts a run's effects by the verb that opens each dependency line.
func verbs(result replay.Result) map[string]int {
	counts := make(map[string]int)

	for _, observed := range result.Effects {
		_, line, _ := strings.Cut(observed.Canonical, "payload=")
		verb, _, _ := strings.Cut(line, " ")
		counts[verb]++
	}

	return counts
}

// TestACrowdedServiceIsJudgedOneConsumerAtATime is the first real-target run, reproduced: two
// consumers pulling at once opened overlapping windows until every effect was attributed to one
// message, and a startup pass landed inside the first message's window, so the determinism gate could
// never hold. Scoped to one consumer, the run holds only that consumer's work.
func TestACrowdedServiceIsJudgedOneConsumerAtATime(t *testing.T) {
	t.Parallel()

	built, recorded := crowdedSandbox(t, consumerReserve, "ORD-CROWD-1", "ORD-CROWD-2")

	runs := make([]replay.Result, 0, 2)

	for _, name := range []string{"clean-1", "clean-2"} {
		result, err := built.Run(t.Context(), name, replay.Clean{}, nil)
		if err != nil {
			t.Fatalf("Run(%s) error = %v", name, err)
		}

		want := map[string]int{"RESERVE": len(recorded)}
		if got := verbs(result); len(got) != 1 || got["RESERVE"] != want["RESERVE"] {
			t.Errorf("%s effects by verb = %v, want %v — the paused consumer or the startup pass leaked in",
				name, got, want)
		}

		runs = append(runs, result)
	}

	if held := gate.NewComparer().
		Compare(effect.Compared(runs[0].Effects), effect.Compared(runs[1].Effects)); !held.OK() {
		t.Errorf("determinism did not hold for a scoped crowded service: %s", held.Describe())
	}

	// The fault has to land on the consumer under test, not on whichever acknowledged the sequence.
	faulted, err := built.Run(t.Context(), "duplicate-3", replay.Duplicate{Seq: recorded[0]}, nil)
	if err != nil {
		t.Fatalf("Run(duplicate) error = %v", err)
	}

	if got := verbs(faulted)["RESERVE"]; got != len(recorded)+1 {
		t.Errorf("RESERVE effects under duplicate delivery = %d, want %d", got, len(recorded)+1)
	}
}

// TestALateConsumerIsNeverDeliveredToUnserialised: with no consumer named, a service that created none
// by the startup limit has the corpus published anyway, so the observation gate can say where to
// look. A consumer it creates after that was never held to one message in flight, and attributing its
// deliveries would put a batch's effects on whichever message the proxy saw last. The run stops.
func TestALateConsumerIsNeverDeliveredToUnserialised(t *testing.T) {
	t.Parallel()

	const startup = 300 * time.Millisecond

	built, _ := quirkySandbox(t, observedConfig(), quirks{lateBy: 2 * startup}, func(settings *harness.Config) {
		settings.Startup = startup
		// Long enough that the late consumer's deliveries land inside the run.
		settings.Drain = 2 * time.Second
	}, "ORD-LATE-1", "ORD-LATE-2")

	_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err == nil {
		t.Fatal("Run() error = nil — a consumer created after the corpus was published was delivered to " +
			"and its effects attributed, with nothing holding it to one message in flight")
	}

	t.Logf("the run stopped: %v", err)

	for _, want := range []string{observedConsumer, startup.String()} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Run() error = %q, want it to name %q", err, want)
		}
	}
}

// TestANamedConsumerAbsentAtStartupStopsTheRun: a named consumer that never appeared cannot be held to
// one message in flight, so the corpus is never published for it to find later.
func TestANamedConsumerAbsentAtStartupStopsTheRun(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	order := orderPayload("ORD-NAMED-1", "WIDGET-WIRE")
	if _, publishErr := store.Publish(t.Context(), toy.SubjectOrderCreated, order); publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	built, err := harness.New(harness.Config{
		Corpus:   store,
		Consumer: observedConsumer,
		HashKey:  make([]byte, hashKeyLen),
		Policy:   observedConfig(),
		Quiesce:  toy.DefaultQuiesce,
		Drain:    fetchWait,
		Startup:  fetchWait,
		Start:    func(context.Context, harness.Addresses) (harness.Consumer, error) { return absent{}, nil },
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	_, err = built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err == nil {
		t.Fatal("Run() error = nil, want the run stopped at the startup limit")
	}

	t.Logf("the run stopped: %v", err)

	for _, want := range []string{observedConsumer, fetchWait.String()} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Run() error = %q, want it to name %q", err, want)
		}
	}

	published, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	t.Logf("the stream holds %d messages after the run stopped", len(published))

	if len(published) != 0 {
		t.Errorf("the corpus was published (%d messages) for a consumer that never appeared", len(published))
	}
}

// TestACrowdedServiceNeedsItsConsumerNamed: guessing which of several consumers is under test would
// put a verdict on the wrong handler, so the run is refused and the refusal names the candidates.
func TestACrowdedServiceNeedsItsConsumerNamed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		consumer string
		want     string
	}{
		{name: "none named", consumer: "", want: "set Config.Consumer"},
		{name: "one it never created", consumer: "ship_orders", want: "not one the service created"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			built, _ := crowdedSandbox(t, testCase.consumer, "ORD-CROWD-1")

			_, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
			if err == nil {
				t.Fatal("Run() error = nil, want a refusal")
			}

			for _, want := range []string{testCase.want, consumerAudit, consumerReserve} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Run() error = %q, want it to contain %q", err, want)
				}
			}
		})
	}
}
