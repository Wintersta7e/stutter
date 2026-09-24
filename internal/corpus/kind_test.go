package corpus_test

import (
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

// Consumer names and timings the kind tests share.
const (
	// reserveConsumer is the name a service gives its consumer without making it durable.
	reserveConsumer = "reserve"
	pullConsumer    = "pull"
	pushConsumer    = "push"
	// emptyPull is how long a pull that only has to create the consumer waits.
	emptyPull = 200 * time.Millisecond
)

// TestConsumerKindsAreClassified: a consumer that acknowledges nothing admits no fault and cannot be
// held to one message in flight, so its kind is read once and a refused kind is named as such.
func TestConsumerKindsAreClassified(t *testing.T) {
	t.Parallel()

	store := start(t)
	js := direct(t, store)

	create := func(config jetstream.ConsumerConfig) {
		t.Helper()

		var err error
		if config.DeliverSubject != "" {
			_, err = js.CreateOrUpdatePushConsumer(t.Context(), corpus.StreamName, config)
		} else {
			_, err = js.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, config)
		}

		if err != nil {
			t.Fatalf("create consumer %+v: %v", config, err)
		}
	}

	create(jetstream.ConsumerConfig{Durable: pullConsumer, AckPolicy: jetstream.AckExplicitPolicy})
	create(jetstream.ConsumerConfig{
		Durable:        pushConsumer,
		DeliverSubject: "deliver.push",
		AckPolicy:      jetstream.AckExplicitPolicy,
	})
	create(jetstream.ConsumerConfig{Name: reserveConsumer, AckPolicy: jetstream.AckExplicitPolicy})
	create(jetstream.ConsumerConfig{Durable: "unacknowledged", AckPolicy: jetstream.AckNonePolicy})

	ordered := orderedConsumer(t, store, js)

	rows := []struct {
		consumer string
		text     string
		kind     corpus.ConsumerKind
		refused  bool
	}{
		{consumer: pullConsumer, kind: corpus.KindPull, text: pullConsumer},
		{consumer: pushConsumer, kind: corpus.KindPush, text: pushConsumer},
		{consumer: reserveConsumer, kind: corpus.KindPull, text: pullConsumer},
		{consumer: "unacknowledged", kind: corpus.KindAckNone, text: "AckNone", refused: true},
		{consumer: ordered, kind: corpus.KindOrdered, text: "ordered", refused: true},
	}

	t.Logf("%d kinds", len(rows))

	if len(rows) == 0 {
		t.Fatal("no consumers to classify")
	}

	for _, row := range rows {
		kind, err := store.Kind(t.Context(), row.consumer)
		if err != nil {
			t.Fatalf("Kind(%s) error = %v", row.consumer, err)
		}

		if kind != row.kind || kind.Refused() != row.refused || kind.String() != row.text {
			t.Errorf("Kind(%s) = %s (refused %t), want %s (refused %t)",
				row.consumer, kind, kind.Refused(), row.text, row.refused)
		}
	}
}

// TestAConsumerNameSurvivesAStartUnlessGenerated: a consumer the service names keeps its name across
// starts though it reads back with no durable name, and only one the client names for it changes.
func TestAConsumerNameSurvivesAStartUnlessGenerated(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := startIn(t, root)

	b0, err := store.Checkpoint(t.Context(), filepath.Join(root, "B0"))
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	first, generated := startTwoConsumers(t, store)

	if err := store.Restore(t.Context(), b0); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	second, _ := startTwoConsumers(t, store)

	if got := corpus.Unstable(first, second); !slices.Equal(got, []string{generated}) {
		t.Errorf("Unstable(%q, %q) = %q, want only the generated name %q", first, second, got, generated)
	}
}

// startTwoConsumers is one start of a service creating a named consumer and a nameless one. It
// returns every consumer on the stream and the name the client generated.
func startTwoConsumers(t *testing.T, store *corpus.Corpus) ([]string, string) {
	t.Helper()

	js := direct(t, store)

	var generated string

	for _, config := range []jetstream.ConsumerConfig{
		{Name: reserveConsumer, AckPolicy: jetstream.AckExplicitPolicy},
		{AckPolicy: jetstream.AckExplicitPolicy},
	} {
		consumer, err := js.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, config)
		if err != nil {
			t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
		}

		info := consumer.CachedInfo()
		if info.Config.Durable != "" {
			t.Errorf("consumer %s reads back Durable %q, want none", info.Name, info.Config.Durable)
		}

		if config.Name == "" {
			generated = info.Name
		}
	}

	names, err := store.Consumers(t.Context())
	if err != nil {
		t.Fatalf("Consumers() error = %v", err)
	}

	return names, generated
}

// TestDurableSaysWhetherANameIsStable: a durable consumer's name is its durable name and survives
// every start without a second look; a consumer with no durable name needs one.
func TestDurableSaysWhetherANameIsStable(t *testing.T) {
	t.Parallel()

	store := start(t)
	js := direct(t, store)

	rows := []struct {
		name   string
		config jetstream.ConsumerConfig
		stable bool
	}{
		{name: "durable", config: jetstream.ConsumerConfig{Durable: reserveConsumer}, stable: true},
		{name: "named only", config: jetstream.ConsumerConfig{Name: "named-" + reserveConsumer}},
		{name: "generated"},
	}

	t.Logf("%d rows", len(rows))

	if len(rows) == 0 {
		t.Fatal("no consumers to read")
	}

	for _, row := range rows {
		row.config.AckPolicy = jetstream.AckExplicitPolicy

		consumer, err := js.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, row.config)
		if err != nil {
			t.Fatalf("%s: CreateOrUpdateConsumer() error = %v", row.name, err)
		}

		stable, err := store.Durable(t.Context(), consumer.CachedInfo().Name)
		if err != nil {
			t.Fatalf("%s: Durable() error = %v", row.name, err)
		}

		if stable != row.stable {
			t.Errorf("%s: Durable() = %t, want %t", row.name, stable, row.stable)
		}
	}
}

// TestAnOrderedSiblingIsPausedLikeAnyOther: an ordered consumer beside the one under test is scoped
// out by a pause like any other, and does not escape it by re-creating itself under a new name.
//
// It consumes continuously, as a service does. A Fetch would prove nothing: the client re-creates an
// ordered consumer on every Fetch call, pause or no pause.
func TestAnOrderedSiblingIsPausedLikeAnyOther(t *testing.T) {
	t.Parallel()

	store := start(t)
	js := direct(t, store)

	stream, err := js.Stream(t.Context(), corpus.StreamName)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	ordered, err := stream.OrderedConsumer(t.Context(), jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("OrderedConsumer() error = %v", err)
	}

	var handed atomic.Int64

	consuming, err := ordered.Consume(func(jetstream.Msg) { handed.Add(1) },
		jetstream.PullHeartbeat(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	t.Cleanup(consuming.Stop)

	before, err := store.Consumers(t.Context())
	if err != nil || len(before) != 1 {
		t.Fatalf("Consumers() = %q, %v, want the one ordered consumer", before, err)
	}

	if pauseErr := store.Pause(t.Context(), before[0]); pauseErr != nil {
		t.Fatalf("Pause() error = %v", pauseErr)
	}

	if _, publishErr := store.Publish(t.Context(), subject, []byte(firstOrder)); publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	time.Sleep(3 * time.Second)

	if got := handed.Load(); got != 0 {
		t.Errorf("the paused ordered consumer was handed %d messages, want none", got)
	}

	after, err := store.Consumers(t.Context())
	if err != nil {
		t.Fatalf("Consumers() error = %v", err)
	}

	if !slices.Equal(after, before) {
		t.Errorf("Consumers() = %q after the pause, want %q: the ordered consumer re-created itself", after, before)
	}
}

// drain fetches once for up to wait and counts what arrived.
func drain(t *testing.T, consumer jetstream.Consumer, wait time.Duration) int {
	t.Helper()

	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(wait))
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}

	count := 0
	for range batch.Messages() {
		count++
	}

	return count
}

// orderedConsumer makes the client create an ordered consumer and returns the name it was given.
func orderedConsumer(t *testing.T, store *corpus.Corpus, js jetstream.JetStream) string {
	t.Helper()

	before, err := store.Consumers(t.Context())
	if err != nil {
		t.Fatalf("Consumers() error = %v", err)
	}

	stream, err := js.Stream(t.Context(), corpus.StreamName)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	ordered, err := stream.OrderedConsumer(t.Context(), jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("OrderedConsumer() error = %v", err)
	}

	drain(t, ordered, emptyPull)

	after, err := store.Consumers(t.Context())
	if err != nil {
		t.Fatalf("Consumers() error = %v", err)
	}

	created := corpus.Unstable(after, before)
	if len(created) != 1 {
		t.Fatalf("the ordered consumer added %q, want exactly one consumer", created)
	}

	return created[0]
}
