package corpus_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/policy"
)

// corpusConsumers names the consumers on the bound stream.
func corpusConsumers(ctx context.Context, store *corpus.Corpus) ([]string, error) {
	listing, err := store.Consumers(ctx)

	return listing.Corpus, err
}

// TestPolicyReadsTheServersDefaults: a service rarely spells its whole consumer out, and what it left
// unset is what the server fills in — a thirty-second deadline, no delivery limit, a thousand in
// flight. Legality read from the request would see zeros and license nothing, or the wrong thing.
func TestPolicyReadsTheServersDefaults(t *testing.T) {
	t.Parallel()

	store := start(t)
	js := direct(t, store)

	create := func(config jetstream.ConsumerConfig) policy.Config {
		t.Helper()

		if _, err := js.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, config); err != nil {
			t.Fatalf("CreateOrUpdateConsumer(%s) error = %v", config.Name, err)
		}

		read, err := store.Policy(t.Context(), config.Name)
		if err != nil {
			t.Fatalf("Policy(%s) error = %v", config.Name, err)
		}

		t.Logf("%s reads back %+v", config.Name, read)

		return read
	}

	plain := create(jetstream.ConsumerConfig{Name: "plain"})

	want := policy.Config{AckMode: policy.AckExplicit, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 1000}
	if plain.AckMode != want.AckMode || plain.AckWait != want.AckWait || plain.MaxDeliver != want.MaxDeliver ||
		plain.MaxAckPending != want.MaxAckPending {
		t.Errorf("Policy(plain) = %+v, want the server's defaults %+v", plain, want)
	}

	curved := create(jetstream.ConsumerConfig{
		Name:    "curved",
		AckWait: 2 * time.Minute,
		BackOff: []time.Duration{time.Second, 5 * time.Second},
	})

	if got := curved.Deadline(1); got != time.Second {
		t.Errorf("Policy(curved).Deadline(1) = %s, want the curve's first entry 1s", got)
	}
}

// TestTheListingNamesEveryConsumerOnEveryStream: a consumer on another stream is the likeliest sign
// the check was pointed at the wrong one, so it is listed by where it is; a key/value bucket's or an
// object store's stream is the client's own machinery and is left out.
func TestTheListingNamesEveryConsumerOnEveryStream(t *testing.T) {
	t.Parallel()

	store := start(t)
	js := direct(t, store)
	ctx := t.Context()

	consume := func(stream, name string) {
		t.Helper()

		if _, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{Durable: name}); err != nil {
			t.Fatalf("CreateOrUpdateConsumer(%s/%s) error = %v", stream, name, err)
		}
	}

	consume(corpus.StreamName, "beta")
	consume(corpus.StreamName, "alpha")

	other := jetstream.StreamConfig{Name: "OTHER", Subjects: []string{"other.>"}}
	if _, err := js.CreateStream(ctx, other); err != nil {
		t.Fatalf("CreateStream(OTHER) error = %v", err)
	}

	consume("OTHER", "watcher")

	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "claims"}); err != nil {
		t.Fatalf("CreateKeyValue() error = %v", err)
	}

	consume("KV_claims", "keys")

	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "blobs"}); err != nil {
		t.Fatalf("CreateObjectStore() error = %v", err)
	}

	consume("OBJ_blobs", "objects")

	listing, err := store.Consumers(ctx)
	if err != nil {
		t.Fatalf("Consumers() error = %v", err)
	}

	t.Logf("%d on the corpus stream %q, %d elsewhere %q", len(listing.Corpus), listing.Corpus,
		len(listing.Elsewhere), listing.Elsewhere)

	if len(listing.Corpus) == 0 && len(listing.Elsewhere) == 0 {
		t.Fatal("the listing is empty, so it proves nothing")
	}

	if want := []string{"alpha", "beta"}; !slices.Equal(listing.Corpus, want) {
		t.Errorf("Corpus = %q, want %q", listing.Corpus, want)
	}

	if want := []string{"OTHER/watcher"}; !slices.Equal(listing.Elsewhere, want) {
		t.Errorf("Elsewhere = %q, want %q", listing.Elsewhere, want)
	}
}

// TestAMissingStreamListsNoConsumers: before anything creates the stream a check is pointed at, it has
// no consumers, which is an answer and not a failure.
func TestAMissingStreamListsNoConsumers(t *testing.T) {
	t.Parallel()

	store := start(t)

	if err := direct(t, store).DeleteStream(t.Context(), corpus.StreamName); err != nil {
		t.Fatalf("DeleteStream() error = %v", err)
	}

	listing, err := store.Consumers(t.Context())
	if err != nil {
		t.Fatalf("Consumers() error = %v, want an empty listing", err)
	}

	if len(listing.Corpus) != 0 || len(listing.Elsewhere) != 0 {
		t.Errorf("Consumers() = %+v, want an empty listing", listing)
	}
}
