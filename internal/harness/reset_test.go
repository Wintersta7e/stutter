package harness_test

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// resetRuns is how many runs a reset test makes: enough that a leak from any run into the next shows.
const resetRuns = 3

// TestAServiceBucketIsGoneAtTheStartOfTheNextRun: a bucket the service made in one run must not be
// there when it starts the next, or the second run starts where the first one ended.
func TestAServiceBucketIsGoneAtTheStartOfTheNextRun(t *testing.T) {
	t.Parallel()

	seen := &startups{}
	built, _ := quirkySandbox(t, observedConfig(), quirks{bucket: true, seen: seen}, nil, "ORD-RESET-1")

	cleanRuns(t, built)

	found := seen.buckets()
	if len(found) != resetRuns {
		t.Fatalf("the service started %d times, want %d", len(found), resetRuns)
	}

	for run, present := range found {
		if present {
			t.Errorf("run %d: the service found bucket %s, which an earlier run made", run+1, serviceBucket)
		}
	}
}

// TestAJobSeededKeyReadsItsSeededRevisionEveryRun: a key a job wrote before the service first started
// reads back at the job's revision at the start of every run, not at the handler's last write.
func TestAJobSeededKeyReadsItsSeededRevisionEveryRun(t *testing.T) {
	t.Parallel()

	var seededAt uint64

	seen := &startups{}
	built, _ := quirkySandbox(t, observedConfig(), quirks{seededKey: true, seen: seen}, func(settings *harness.Config) {
		seededAt = seed(t, settings.Corpus)
	}, "ORD-RESET-1")

	cleanRuns(t, built)

	revisions := seen.seededRevisions()
	if len(revisions) != resetRuns {
		t.Fatalf("the service started %d times, want %d", len(revisions), resetRuns)
	}

	for run, revision := range revisions {
		if revision != seededAt {
			t.Errorf("run %d: the seeded key read revision %d, want the job's %d", run+1, revision, seededAt)
		}
	}
}

// TestEveryRestoreServesTheServiceAFreshPort: each run's bus is a fresh start, and the service is
// handed where it is now; an address kept from an earlier run would reach nothing.
func TestEveryRestoreServesTheServiceAFreshPort(t *testing.T) {
	t.Parallel()

	var store *corpus.Corpus

	built, _ := quirkySandbox(t, observedConfig(), quirks{}, func(settings *harness.Config) {
		store = settings.Corpus
	}, "ORD-RESET-1")

	previous := store.URL()

	for run := range resetRuns {
		result, err := built.Run(t.Context(), "clean", replay.Clean{}, nil)
		if err != nil {
			t.Fatalf("run %d: Run() error = %v", run+1, err)
		}

		if len(effect.Compared(result.Effects)) == 0 {
			t.Errorf("run %d observed no effect", run+1)
		}

		if store.URL() == previous {
			t.Errorf("run %d: the bus still serves %s, the previous start's address", run+1, previous)
		}

		previous = store.URL()
	}
}

// cleanRuns makes resetRuns clean runs.
func cleanRuns(t *testing.T, built *harness.Sandbox) {
	t.Helper()

	for run := range resetRuns {
		if _, err := built.Run(t.Context(), "clean", replay.Clean{}, nil); err != nil {
			t.Fatalf("run %d: Run() error = %v", run+1, err)
		}
	}
}

// seed is a job's work: the seeded key written on a direct connection before the service ever
// starts. It returns the key's revision.
func seed(t *testing.T, store *corpus.Corpus) uint64 {
	t.Helper()

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	defer connection.Close()

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	bucket, err := stream.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: seededBucket})
	if err != nil {
		t.Fatalf("CreateKeyValue() error = %v", err)
	}

	revision, err := bucket.Put(t.Context(), seededKey, []byte("seeded"))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	return revision
}
