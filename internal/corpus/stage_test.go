package corpus_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
)

// firstOrder is the message every test here keeps, named once so the assertions and the fixture
// cannot drift apart.
const firstOrder = "ORD-1"

// pushedConsumer is the push consumer the tests that need one create.
const pushedConsumer = "pushed"

// keyValues are an Idempotency-Key header's two values, in the order they were written.
var keyValues = []string{"second", "first"}

// TestStageKeepsOnlyTheChosenMessages is how a run is scoped to part of the corpus when the service
// under test picks its own messages. Shrinking a failure to a minimal repro is exactly that.
func TestStageKeepsOnlyTheChosenMessages(t *testing.T) {
	t.Parallel()

	store := stocked(t, firstOrder, "ORD-2", "ORD-3")

	snapshot, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	keep := []corpus.Message{snapshot[0], snapshot[2]}

	staged := stage(t, store, keep)

	if len(staged) != len(keep) {
		t.Fatalf("staged %d messages, want %d", len(staged), len(keep))
	}

	for at, message := range staged {
		if message.Recorded != keep[at].Seq {
			t.Errorf("staged[%d].Recorded = %d, want %d", at, message.Recorded, keep[at].Seq)
		}
	}

	// Renumbered from one, which is why the translation is returned at all: a fault aimed at the
	// recorded sequence would otherwise land on a different message or on none.
	if staged[0].Sequence != 1 || staged[1].Sequence != 2 {
		t.Errorf("sequences = %d, %d, want 1, 2", staged[0].Sequence, staged[1].Sequence)
	}

	delivered := drainSubjects(t, store)
	if len(delivered) != len(keep) {
		t.Fatalf("the staged corpus delivered %q, want the two kept messages", delivered)
	}

	if delivered[0] != firstOrder || delivered[1] != "ORD-3" {
		t.Errorf("delivered %q, want ORD-1 then ORD-3 — the dropped message is still there", delivered)
	}
}

// TestSnapshotStagesTheWholeCorpusAgainAfterASubset is the property a check depends on: it
// interleaves whole-corpus runs with the subsets a shrink asks for, and staging destroys everything
// it does not republish. A caller that re-read the stream between runs would find the corpus already
// reduced to the last subset it staged.
func TestSnapshotStagesTheWholeCorpusAgainAfterASubset(t *testing.T) {
	t.Parallel()

	store := stocked(t, firstOrder, "ORD-2", "ORD-3")

	snapshot, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	stage(t, store, []corpus.Message{snapshot[1]})

	if delivered := drainSubjects(t, store); len(delivered) != 1 {
		t.Fatalf("the subset corpus delivered %q, want ORD-2 alone", delivered)
	}

	stage(t, store, snapshot)

	delivered := drainSubjects(t, store)
	if len(delivered) != len(snapshot) {
		t.Fatalf("the restored corpus delivered %q, want all three messages back", delivered)
	}

	if delivered[0] != firstOrder || delivered[2] != "ORD-3" {
		t.Errorf("delivered %q, want the recorded order back", delivered)
	}
}

// TestFillRefusesAStreamSomethingElsePublishedInto: the translation is computed before publishing,
// so a message that lands elsewhere would have every fault aimed by sequence hit the wrong message.
func TestFillRefusesAStreamSomethingElsePublishedInto(t *testing.T) {
	t.Parallel()

	store := stocked(t, firstOrder)

	snapshot, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if clearErr := store.Clear(t.Context()); clearErr != nil {
		t.Fatalf("Clear() error = %v", clearErr)
	}

	first := next(t, store)

	// The service under test publishing into the stream it consumes, after the numbering was read.
	_, publishErr := store.Publish(t.Context(), corpus.SubjectPrefix+"orders", []byte("FOREIGN"))
	if publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	err = store.Fill(t.Context(), snapshot, first)
	if !errors.Is(err, corpus.ErrFill) {
		t.Fatalf("Fill() error = %v, want ErrFill: the message landed at 2, not 1", err)
	}

	var refused *corpus.FillError
	if !errors.As(err, &refused) || refused.Landed != 2 {
		t.Errorf("Fill() error = %#v, want a FillError that landed at 2", err)
	}
}

// TestFillNumbersFromTheStreamsLastSequence: a stream a job or the service created may already hold
// messages, so the corpus lands after them, and the translation has to say so before publishing.
func TestFillNumbersFromTheStreamsLastSequence(t *testing.T) {
	t.Parallel()

	store := stocked(t, firstOrder, "ORD-2")

	snapshot, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	first, err := store.Next(t.Context())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	if first != 3 {
		t.Fatalf("Next() = %d, want 3 after two messages", first)
	}

	if err := store.Fill(t.Context(), snapshot, first); err != nil {
		t.Fatalf("Fill() error = %v", err)
	}

	staged := corpus.Numbering(snapshot, first)
	for at, message := range staged {
		if want := first + uint64(at); message.Sequence != want || message.Recorded != snapshot[at].Seq {
			t.Errorf("Numbering()[%d] = %+v, want sequence %d for recorded %d", at, message, want, snapshot[at].Seq)
		}
	}

	stream := directStream(t, store)

	for _, message := range staged {
		raw, getErr := stream.GetMsg(t.Context(), message.Sequence)
		if getErr != nil {
			t.Fatalf("GetMsg(%d) error = %v", message.Sequence, getErr)
		}

		if want := snapshot[message.Recorded-1].Payload; string(raw.Data) != string(want) {
			t.Errorf("sequence %d holds %s, want %s", message.Sequence, raw.Data, want)
		}
	}
}

// TestFillCarriesHeaders: a header-keyed dedupe guard reads what the sidecar supplied, so a header
// reaches the stream with every value, in the order it was written.
func TestFillCarriesHeaders(t *testing.T) {
	t.Parallel()

	store := stocked(t)

	messages := []corpus.Message{{
		Subject: corpus.SubjectPrefix + "orders",
		Payload: []byte(firstOrder),
		Header:  map[string][]string{"Idempotency-Key": keyValues},
		Seq:     1,
	}}

	first := next(t, store)
	if err := store.Fill(t.Context(), messages, first); err != nil {
		t.Fatalf("Fill() error = %v", err)
	}

	raw, err := directStream(t, store).GetMsg(t.Context(), first)
	if err != nil {
		t.Fatalf("GetMsg() error = %v", err)
	}

	if got := raw.Header.Values("Idempotency-Key"); !slices.Equal(got, keyValues) {
		t.Errorf("Idempotency-Key = %q, want both values in file order", got)
	}

	snapshot, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if got := snapshot[0].Header["Idempotency-Key"]; !slices.Equal(got, keyValues) {
		t.Errorf("Snapshot() header = %q, want the published values back", got)
	}
}

// TestFillRefusesADuplicateAcknowledgement: the stream's duplicate window drops a message whose id it
// has seen, and the corpus would then be missing one while every later sequence shifted.
func TestFillRefusesADuplicateAcknowledgement(t *testing.T) {
	t.Parallel()

	withID := func(seq uint64, id string) corpus.Message {
		return corpus.Message{
			Subject: corpus.SubjectPrefix + "orders",
			Payload: []byte(id),
			Header:  map[string][]string{"Nats-Msg-Id": {id}},
			Seq:     seq,
		}
	}

	cases := []struct {
		name     string
		already  string
		messages []corpus.Message
		other    uint64
	}{
		{name: "already in the stream", already: "dup-1", messages: []corpus.Message{withID(1, "dup-1")}},
		{
			name:     "two corpus messages",
			messages: []corpus.Message{withID(1, "dup-2"), withID(2, "dup-2")},
			other:    1,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store := stocked(t)

			if testCase.already != "" {
				msg := nats.NewMsg(corpus.SubjectPrefix + "orders")
				msg.Header.Set("Nats-Msg-Id", testCase.already)

				if _, err := directJetStream(t, store).PublishMsg(t.Context(), msg); err != nil {
					t.Fatalf("PublishMsg() error = %v", err)
				}
			}

			err := store.Fill(t.Context(), testCase.messages, next(t, store))
			if !errors.Is(err, corpus.ErrFill) {
				t.Fatalf("Fill() error = %v, want ErrFill", err)
			}

			var refused *corpus.FillError
			if !errors.As(err, &refused) {
				t.Fatalf("Fill() error = %v, want a FillError", err)
			}

			if last := testCase.messages[len(testCase.messages)-1]; refused.Seq != last.Seq {
				t.Errorf("FillError.Seq = %d, want %d", refused.Seq, last.Seq)
			}

			if refused.Other != testCase.other {
				t.Errorf("FillError.Other = %d, want %d", refused.Other, testCase.other)
			}
		})
	}
}

// TestPendingCountsWhatTheConsumerWillReceive: the staged corpus reaches the consumer only where its
// filter admits it, and the server's own count and filters are what say so.
func TestPendingCountsWhatTheConsumerWillReceive(t *testing.T) {
	t.Parallel()

	const (
		admitted = corpus.SubjectPrefix + "order.created"
		skipped  = corpus.SubjectPrefix + "order.cancelled"
	)

	store := stocked(t)

	messages := []corpus.Message{
		{Subject: admitted, Payload: []byte(firstOrder), Seq: 1},
		{Subject: skipped, Payload: []byte("ORD-2"), Seq: 2},
		{Subject: admitted, Payload: []byte("ORD-3"), Seq: 3},
	}

	if err := store.Fill(t.Context(), messages, next(t, store)); err != nil {
		t.Fatalf("Fill() error = %v", err)
	}

	stream := directJetStream(t, store)

	if _, err := stream.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
		Durable:       "filtered",
		FilterSubject: admitted,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	if _, err := stream.CreateOrUpdatePushConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
		Durable:        pushedConsumer,
		DeliverSubject: "deliver.pending",
		AckPolicy:      jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("CreateOrUpdatePushConsumer() error = %v", err)
	}

	cases := []struct {
		consumer string
		filters  []string
		count    int
	}{
		{consumer: "filtered", count: 2, filters: []string{admitted}},
		{consumer: pushedConsumer, count: len(messages)},
	}

	for _, testCase := range cases {
		count, filters, err := store.Pending(t.Context(), testCase.consumer)
		if err != nil {
			t.Fatalf("Pending(%q) error = %v", testCase.consumer, err)
		}

		if count != testCase.count || !slices.Equal(filters, testCase.filters) {
			t.Errorf("Pending(%q) = %d, %q, want %d, %q",
				testCase.consumer, count, filters, testCase.count, testCase.filters)
		}
	}
}

// TestAPausedConsumerIsHandedNothing is how a run is scoped to one consumer of a service running
// several. The pause has to hold while the corpus is published AND survive the service re-creating
// the consumer with its own configuration, which is what a real service does on startup.
func TestAPausedConsumerIsHandedNothing(t *testing.T) {
	t.Parallel()

	store := stocked(t)

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	t.Cleanup(connection.Close)

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	consumers := make(map[string]jetstream.Consumer, 2)

	create := func(name string) {
		created, createErr := stream.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
			Durable:   name,
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		if createErr != nil {
			t.Fatalf("CreateOrUpdateConsumer(%q) error = %v", name, createErr)
		}

		consumers[name] = created
	}

	create("paused")
	create("scoped")

	names, err := corpusConsumers(t.Context(), store)
	if err != nil {
		t.Fatalf("Consumers() error = %v", err)
	}

	if !slices.Equal(names, []string{"paused", "scoped"}) {
		t.Fatalf("Consumers() = %q, want both, in name order", names)
	}

	if err := store.Pause(t.Context(), "paused"); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	create("paused")

	if _, err := store.Publish(t.Context(), corpus.SubjectPrefix+"orders", []byte(firstOrder)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if got := fetched(t, consumers["scoped"]); got != 1 {
		t.Errorf("the scoped consumer was handed %d messages, want 1", got)
	}

	if got := fetched(t, consumers["paused"]); got != 0 {
		t.Errorf("the paused consumer was handed %d messages, want none", got)
	}
}

// TestASerialisedConsumerHasOneMessageInFlight: a service asking for a batch gets one message, and
// the next only once that one is acknowledged. The service's own configuration asked for more.
func TestASerialisedConsumerHasOneMessageInFlight(t *testing.T) {
	t.Parallel()

	store := stocked(t, firstOrder, "ORD-2", "ORD-3")

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	t.Cleanup(connection.Close)

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
		Durable:       "batching",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 100,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	if _, err := store.Serialise(t.Context(), "batching", harness.DeliveryCap); err != nil {
		t.Fatalf("Serialise() error = %v", err)
	}

	for want := range []string{firstOrder, "ORD-2"} {
		batch, fetchErr := consumer.Fetch(3, jetstream.FetchMaxWait(500*time.Millisecond))
		if fetchErr != nil {
			t.Fatalf("Fetch() error = %v", fetchErr)
		}

		var handed []jetstream.Msg
		for msg := range batch.Messages() {
			handed = append(handed, msg)
		}

		if len(handed) != 1 {
			t.Fatalf("pull %d handed over %d messages, want exactly 1 in flight", want, len(handed))
		}

		if ackErr := handed[0].Ack(); ackErr != nil {
			t.Fatalf("Ack() error = %v", ackErr)
		}
	}
}

// TestSerialiseFindsAPushConsumer: a service may consume through a push consumer, and one message in
// flight matters just as much there. The pull-only lookup refused it with "consumer is not a pull
// consumer", so a push target was never held to one message in flight.
func TestSerialiseFindsAPushConsumer(t *testing.T) {
	t.Parallel()

	const deliverTo = "deliver.pushed"

	store := stocked(t)

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	t.Cleanup(connection.Close)

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	_, err = stream.CreateOrUpdatePushConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
		Durable:        pushedConsumer,
		DeliverSubject: deliverTo,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  100,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdatePushConsumer() error = %v", err)
	}

	read, err := store.Serialise(t.Context(), pushedConsumer, harness.DeliveryCap)
	if err != nil {
		t.Fatalf("Serialise() error = %v", err)
	}

	// What it returns is the consumer as it was before the rewrite, push or not.
	if read.AckMode != policy.AckExplicit || read.MaxAckPending != 100 {
		t.Errorf("Serialise() read %+v, want the push consumer explicit with MaxAckPending 100", read)
	}

	pushed, err := stream.PushConsumer(t.Context(), corpus.StreamName, pushedConsumer)
	if err != nil {
		t.Fatalf("PushConsumer() error = %v — serialising turned it into something else", err)
	}

	config := pushed.CachedInfo().Config

	if config.MaxAckPending != 1 {
		t.Errorf("MaxAckPending = %d, want 1", config.MaxAckPending)
	}

	if config.DeliverSubject != deliverTo {
		t.Errorf("DeliverSubject = %q, want %q unchanged", config.DeliverSubject, deliverTo)
	}
}

// TestSerialiseCapsDeliveriesAndReturnsWhatItRead: one call holds the consumer to one message in
// flight and to the run's delivery cap, trimming the backoff curve to match — the server refuses a
// curve longer than the delivery limit — and hands back the consumer as it was, which is what
// legality reads.
func TestSerialiseCapsDeliveriesAndReturnsWhatItRead(t *testing.T) {
	t.Parallel()

	store := stocked(t)
	js := directJetStream(t, store)
	curve := []time.Duration{
		200 * time.Millisecond, 500 * time.Millisecond, 900 * time.Millisecond, 2 * time.Second, 5 * time.Second,
	}

	cases := []struct {
		created       jetstream.ConsumerConfig
		maxDeliver    int
		backOff       int
		maxAckPending int
	}{
		{
			created: jetstream.ConsumerConfig{
				Durable:       curvedConsumer,
				BackOff:       curve,
				MaxDeliver:    100,
				MaxAckPending: 1000,
			},
			maxDeliver:    4,
			backOff:       4,
			maxAckPending: 1,
		},
		{
			created:    jetstream.ConsumerConfig{Durable: "unlimited", AckWait: time.Second},
			maxDeliver: 4, maxAckPending: 1,
		},
		{
			created:    jetstream.ConsumerConfig{Durable: "short", AckWait: time.Second, MaxDeliver: 2},
			maxDeliver: 2, maxAckPending: 1,
		},
	}

	for _, testCase := range cases {
		name := testCase.created.Durable

		if _, err := js.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, testCase.created); err != nil {
			t.Fatalf("CreateOrUpdateConsumer(%s) error = %v", name, err)
		}

		read, err := store.Serialise(t.Context(), name, harness.DeliveryCap)
		if err != nil {
			t.Fatalf("Serialise(%s) error = %v", name, err)
		}

		consumer, err := js.Consumer(t.Context(), corpus.StreamName, name)
		if err != nil {
			t.Fatalf("Consumer(%s) error = %v", name, err)
		}

		held := consumer.CachedInfo().Config
		t.Logf("%s: read MaxDeliver %d, %d backoff entries, MaxAckPending %d; now MaxDeliver %d, %d entries, "+
			"MaxAckPending %d", name, read.MaxDeliver, len(read.BackOff), read.MaxAckPending, held.MaxDeliver,
			len(held.BackOff), held.MaxAckPending)

		unread := read.MaxDeliver != 100 || len(read.BackOff) != len(curve) || read.MaxAckPending != 1000
		if name == curvedConsumer && unread {
			t.Errorf("%s: Serialise() returned %+v, want the configuration as it was before the rewrite", name, read)
		}

		if held.MaxDeliver != testCase.maxDeliver || len(held.BackOff) != testCase.backOff ||
			held.MaxAckPending != testCase.maxAckPending {
			t.Errorf("%s: the server holds MaxDeliver %d, %d entries, MaxAckPending %d; want %d, %d, %d", name,
				held.MaxDeliver, len(held.BackOff), held.MaxAckPending, testCase.maxDeliver, testCase.backOff,
				testCase.maxAckPending)
		}
	}

	if len(cases) == 0 {
		t.Fatal("no consumers to serialise")
	}
}

// TestDiscoveryReadsTheConfigBeforeSerialise: the configuration legality is decided from is the one
// the service created, with the server's defaults filled in — not the one Serialise leaves behind for
// the run, which would license nothing that needs two messages in flight.
func TestDiscoveryReadsTheConfigBeforeSerialise(t *testing.T) {
	t.Parallel()

	store := stocked(t)
	js := directJetStream(t, store)

	create := func(config jetstream.ConsumerConfig) policy.Config {
		t.Helper()

		if _, err := js.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, config); err != nil {
			t.Fatalf("CreateOrUpdateConsumer(%s) error = %v", config.Durable, err)
		}

		read, err := store.Serialise(t.Context(), config.Durable, harness.DeliveryCap)
		if err != nil {
			t.Fatalf("Serialise(%s) error = %v", config.Durable, err)
		}

		return read
	}

	batching := create(jetstream.ConsumerConfig{Durable: "batching", MaxAckPending: 10})
	t.Logf("batching: read MaxAckPending %d, first deadline %s", batching.MaxAckPending, batching.Deadline(1))

	if batching.MaxAckPending != 10 || batching.Deadline(1) != 30*time.Second {
		t.Errorf("Serialise() read MaxAckPending %d and a first deadline of %s, want 10 and the server's 30s",
			batching.MaxAckPending, batching.Deadline(1))
	}

	after, err := store.Policy(t.Context(), "batching")
	if err != nil {
		t.Fatalf("Policy() error = %v", err)
	}

	if after.MaxAckPending != 1 {
		t.Errorf("Policy() after Serialise reads MaxAckPending %d, want the run's 1", after.MaxAckPending)
	}

	curved := create(jetstream.ConsumerConfig{
		Durable: curvedConsumer,
		AckWait: 2 * time.Minute,
		BackOff: []time.Duration{time.Second, 5 * time.Second},
	})

	if got := curved.Deadline(1); got != time.Second {
		t.Errorf("Serialise() read a first deadline of %s, want the curve's first entry 1s", got)
	}
}

// fetched pulls once and counts what arrived. A paused consumer must look idle, not broken, so an
// error here fails the test rather than counting as nothing.
func fetched(t *testing.T, consumer jetstream.Consumer) int {
	t.Helper()

	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}

	count := 0
	for range batch.Messages() {
		count++
	}

	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("batch error = %v, want an idle consumer to look idle", err)
	}

	return count
}

// stage scopes the corpus as a run does: clear, fill from the next sequence, and hand back the
// translation.
func stage(t *testing.T, store *corpus.Corpus, messages []corpus.Message) []corpus.Staged {
	t.Helper()

	if err := store.Clear(t.Context()); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}

	first := next(t, store)

	if err := store.Fill(t.Context(), messages, first); err != nil {
		t.Fatalf("Fill() error = %v", err)
	}

	return corpus.Numbering(messages, first)
}

// next reads the sequence the next staged message will land at.
func next(t *testing.T, store *corpus.Corpus) uint64 {
	t.Helper()

	first, err := store.Next(t.Context())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	return first
}

// directJetStream is a JetStream context on a connection of the test's own.
//
//nolint:ireturn // the client library models JetStream as an interface.
func directJetStream(t *testing.T, store *corpus.Corpus) jetstream.JetStream {
	t.Helper()

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	t.Cleanup(connection.Close)

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	return stream
}

// directStream opens the corpus stream on a connection of the test's own.
//
//nolint:ireturn // the client library models a stream as an interface.
func directStream(t *testing.T, store *corpus.Corpus) jetstream.Stream {
	t.Helper()

	stream, err := directJetStream(t, store).Stream(t.Context(), store.Topic().Stream)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	return stream
}

// stocked starts a corpus holding one message per payload, in the order given.
func stocked(t *testing.T, payloads ...string) *corpus.Corpus {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	for _, payload := range payloads {
		if _, err := store.Publish(t.Context(), corpus.SubjectPrefix+"orders", []byte(payload)); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	return store
}

// drainSubjects replays the corpus and returns each payload, which is how a test sees what a service
// under test would actually be handed.
func drainSubjects(t *testing.T, store *corpus.Corpus) []string {
	t.Helper()

	replay, err := store.Replay(t.Context(), "drain-"+t.Name(), corpus.ConsumerOptions{
		AckWait:       time.Second,
		MaxDeliver:    1,
		MaxAckPending: 1,
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	var payloads []string

	for {
		delivery, err := replay.Next(t.Context(), 500*time.Millisecond)
		if err != nil {
			return payloads
		}

		payloads = append(payloads, string(delivery.Payload))

		if ackErr := delivery.Ack(); ackErr != nil {
			t.Fatalf("Ack() error = %v", ackErr)
		}
	}
}
