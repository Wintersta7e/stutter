package nats_test

import (
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// claimBucket is the key/value bucket the guard in this test claims into.
const claimBucket = "reject-test-claims"

// TestARefusedClaimIsReportedAsRefused is the whole of the commonest idempotency guard.
//
// A dedupe guard claims a key atomically and does its work only if the claim succeeded. On a
// redelivery the claim is REFUSED by the bus — nothing is stored, the handler does nothing — but the
// attempt is still a publish, and recorded as an ordinary effect it lengthens the faulted run's
// sequence and reports a working guard as a divergence. The bus's answer is the only place that
// refusal is visible.
func TestARefusedClaimIsReportedAsRefused(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	parsed, err := url.Parse(store.URL())
	if err != nil {
		t.Fatalf("parse the bus address: %v", err)
	}

	// The bucket is made on a DIRECT connection: provisioning a dependency through the proxy would
	// put its setup into the effect sequence.
	direct, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("connect directly: %v", err)
	}

	t.Cleanup(direct.Close)

	directStream, err := jetstream.New(direct)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	if _, bucketErr := directStream.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{
		Bucket: claimBucket,
	}); bucketErr != nil {
		t.Fatalf("CreateKeyValue() error = %v", bucketErr)
	}

	observed := &recorder{}

	proxy, err := natsproxy.Listen(t.Context(), "127.0.0.1:0", parsed.Host, observed)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	served := make(chan error, 1)
	go func() { served <- proxy.Serve(t.Context()) }()

	connection, err := nats.Connect("nats://" + proxy.Addr())
	if err != nil {
		t.Fatalf("connect through the proxy: %v", err)
	}

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() through the proxy: %v", err)
	}

	bucket, err := stream.KeyValue(t.Context(), claimBucket)
	if err != nil {
		t.Fatalf("KeyValue() error = %v", err)
	}

	if _, claimErr := bucket.Create(t.Context(), "1", []byte("claimed")); claimErr != nil {
		t.Fatalf("the first claim should succeed: %v", claimErr)
	}

	// The second claim is what a redelivery produces, and the guard depends on it failing.
	_, secondErr := bucket.Create(t.Context(), "1", []byte("claimed"))
	if !errors.Is(secondErr, jetstream.ErrKeyExists) {
		t.Fatalf("the second claim returned %v, want ErrKeyExists — the guard is not being exercised", secondErr)
	}

	connection.Close()

	if closeErr := proxy.Close(); closeErr != nil {
		t.Fatalf("proxy.Close() error = %v", closeErr)
	}

	if serveErr := <-served; serveErr != nil {
		t.Fatalf("proxy.Serve() error = %v", serveErr)
	}

	claims := observed.matching("kv.create")
	if len(claims) != 2 {
		t.Fatalf("recorded %d claims, want 2 (the one that took and the one that was refused):\n%v",
			len(claims), observed.texts())
	}

	refused := observed.refusals()
	if len(refused) != 1 {
		t.Fatalf("the bus refused one claim but %d effects are marked refused:\n%v", len(refused), observed.texts())
	}

	if !strings.Contains(refused[0], "kv.create") {
		t.Errorf("the refused effect is %q, want the second claim", refused[0])
	}

	// A read is not a write. After the refusal the guard looks its claim up, and that lookup is a
	// direct get whose own subject EMBEDS the key/value subject it reads — so resolving the embedded
	// part reports a handler that merely checked as one that stored. Asserted on the effects that must
	// exist rather than on the subject text, because the subject is exactly what the bug renders away.
	if len(observed.matching("kv.get")) == 0 {
		t.Errorf("the guard's lookup was not recorded as a read:\n%v", observed.texts())
	}

	if stores := observed.matching("kv.put"); len(stores) != 0 {
		t.Errorf("a lookup was recorded as a store: %v", stores)
	}

	// Marked as a read, too: a guard that only looked again under redelivery must not fail on the
	// look alone.
	if reads, lookups := observed.reads(), observed.matching("kv.get"); !slices.Equal(reads, lookups) {
		t.Errorf("effects marked as reads = %v, want exactly the lookups %v", reads, lookups)
	}
}
