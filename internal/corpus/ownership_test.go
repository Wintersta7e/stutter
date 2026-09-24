package corpus_test

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

// ownedStream is the stream the ownership tests bind, named as a user's --stream would be.
const ownedStream = "ORDERS"

// Subjects a user's corpus carries, and the filter a user's stream captures them with.
const (
	orderCreated   = "orders.created"
	orderCancelled = "orders.cancelled"
	allOrders      = "orders.>"
)

// ownedCorpus is a corpus over two subjects, one of them twice, so "distinct" means something.
func ownedCorpus() []corpus.Message {
	return []corpus.Message{
		{Subject: orderCreated, Payload: []byte(firstOrder), Seq: 1},
		{Subject: orderCancelled, Payload: []byte("ORD-2"), Seq: 2},
		{Subject: orderCreated, Payload: []byte("ORD-3"), Seq: 3},
	}
}

// openOwned opens a bus bound to ownedStream and checkpoints it empty, as B0 is taken before anything
// has created the stream.
func openOwned(t *testing.T) (*corpus.Corpus, corpus.Checkpoint) {
	t.Helper()

	root := t.TempDir()

	store, err := corpus.Open(t.Context(), filepath.Join(root, "store"), ownedStream)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	b0, err := store.Checkpoint(t.Context(), filepath.Join(root, "B0"))
	if err != nil {
		t.Fatalf("Checkpoint(B0) error = %v", err)
	}

	return store, b0
}

func survey(t *testing.T, store *corpus.Corpus) corpus.Survey {
	t.Helper()

	surveyed, err := store.Survey(t.Context())
	if err != nil {
		t.Fatalf("Survey() error = %v", err)
	}

	return surveyed
}

// readBack is the bound stream's configuration as the server holds it.
func readBack(t *testing.T, store *corpus.Corpus) jetstream.StreamConfig {
	t.Helper()

	stream, err := direct(t, store).Stream(t.Context(), ownedStream)
	if err != nil {
		t.Fatalf("Stream(%s) error = %v", ownedStream, err)
	}

	return stream.CachedInfo().Config
}

// TestStreamOwnershipFollowsWhoCreatedItFirst: whoever creates the stream first in production owns
// it — a job, then the service, then Stutter — and each finds it as it would have made it.
func TestStreamOwnershipFollowsWhoCreatedItFirst(t *testing.T) {
	t.Parallel()

	t.Run("job-owned", func(t *testing.T) {
		t.Parallel()

		store, _ := openOwned(t)
		config := jetstream.StreamConfig{Name: ownedStream, Subjects: []string{allOrders}, MaxMsgs: 10}

		if _, err := direct(t, store).CreateStream(t.Context(), config); err != nil {
			t.Fatalf("CreateStream() error = %v", err)
		}

		before := survey(t, store)
		held := readBack(t, store)

		if err := store.Establish(t.Context(), before, before, ownedCorpus()); err != nil {
			t.Fatalf("Establish() error = %v", err)
		}

		if got := readBack(t, store); got.MaxMsgs != 10 || !slices.Equal(got.Subjects, held.Subjects) {
			t.Errorf("the job's stream reads back %+v, want it left as the job made it", got)
		}
	})

	t.Run("service-owned", func(t *testing.T) {
		t.Parallel()

		store, b0 := openOwned(t)
		before := survey(t, store)
		config := jetstream.StreamConfig{Name: ownedStream, Subjects: []string{allOrders}, Duplicates: time.Minute}

		// The probe start: the service creates its stream the way it always does.
		if _, err := direct(t, store).CreateStream(t.Context(), config); err != nil {
			t.Fatalf("CreateStream() error = %v", err)
		}

		after := survey(t, store)

		if err := store.Restore(t.Context(), b0); err != nil {
			t.Fatalf("Restore(B0) error = %v", err)
		}

		if err := store.Establish(t.Context(), before, after, ownedCorpus()); err != nil {
			t.Fatalf("Establish() error = %v", err)
		}

		if got := readBack(t, store); !slices.Equal(got.Subjects, config.Subjects) {
			t.Errorf("subjects = %q, want the service's %q", got.Subjects, config.Subjects)
		}

		// Every later start the service creates it again, and must find it as it would have made it.
		if _, err := direct(t, store).CreateStream(t.Context(), config); err != nil {
			t.Errorf("the service's own CreateStream() error = %v, want it to find its stream unchanged", err)
		}
	})

	t.Run("stutter-owned", func(t *testing.T) {
		t.Parallel()

		store, _ := openOwned(t)
		before := survey(t, store)

		if err := store.Establish(t.Context(), before, before, ownedCorpus()); err != nil {
			t.Fatalf("Establish() error = %v", err)
		}

		got := readBack(t, store)
		if want := []string{orderCancelled, orderCreated}; !slices.Equal(got.Subjects, want) {
			t.Errorf("subjects = %q, want the distinct corpus subjects %q", got.Subjects, want)
		}

		if got.Storage != jetstream.FileStorage {
			t.Errorf("storage = %s, want file", got.Storage)
		}
	})

	t.Run("service-owned memory", func(t *testing.T) {
		t.Parallel()

		store, b0 := openOwned(t)
		before := survey(t, store)
		config := jetstream.StreamConfig{
			Name:     ownedStream,
			Subjects: []string{allOrders},
			Storage:  jetstream.MemoryStorage,
		}

		if _, err := direct(t, store).CreateStream(t.Context(), config); err != nil {
			t.Fatalf("CreateStream() error = %v", err)
		}

		after := survey(t, store)

		if err := store.Restore(t.Context(), b0); err != nil {
			t.Fatalf("Restore(B0) error = %v", err)
		}

		if err := store.Establish(t.Context(), before, after, ownedCorpus()); err != nil {
			t.Fatalf("Establish() error = %v", err)
		}

		if _, err := direct(t, store).Stream(t.Context(), ownedStream); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Errorf("Stream() error = %v, want a memory stream left for the service to recreate", err)
		}
	})
}

// unsafeCase is one stream configuration Establish must refuse, and the key its refusal names.
type unsafeCase struct {
	// job is the stream present at the first survey, if any.
	job *jetstream.StreamConfig
	// service is a stream the probe start creates, if any.
	service *jetstream.StreamConfig
	name    string
	names   string
}

// TestEstablishRefusesAnUnsafeStream: a stream that does not capture the corpus, or that mirrors,
// republishes or transforms it, cannot be filled and checked, so it is refused before any run.
func TestEstablishRefusesAnUnsafeStream(t *testing.T) {
	t.Parallel()

	job := func(mutate func(*jetstream.StreamConfig)) *jetstream.StreamConfig {
		config := &jetstream.StreamConfig{Name: ownedStream, Subjects: []string{allOrders}}
		mutate(config)

		return config
	}

	cases := []unsafeCase{
		{
			name:  "uncaptured subject",
			job:   job(func(c *jetstream.StreamConfig) { c.Subjects = []string{orderCreated} }),
			names: orderCancelled,
		},
		{
			name: "mirror",
			job: job(func(c *jetstream.StreamConfig) {
				c.Subjects = nil
				c.Mirror = &jetstream.StreamSource{Name: "ORIGIN"}
			}),
			names: "Mirror",
		},
		{
			name: "republish",
			job: job(func(c *jetstream.StreamConfig) {
				c.RePublish = &jetstream.RePublish{Source: ">", Destination: "copied.>"}
			}),
			names: "RePublish",
		},
		{
			name: "subject transform",
			job: job(func(c *jetstream.StreamConfig) {
				c.SubjectTransform = &jetstream.SubjectTransformConfig{Source: allOrders, Destination: "moved.>"}
			}),
			names: "SubjectTransform",
		},
		{name: "no ack", job: job(func(c *jetstream.StreamConfig) { c.NoAck = true }), names: "NoAck"},
		{
			name:    "the service consumes elsewhere",
			service: &jetstream.StreamConfig{Name: "ELSEWHERE", Subjects: []string{allOrders}},
			names:   "ELSEWHERE",
		},
		{
			name:  "another stream holds the subjects",
			job:   job(func(c *jetstream.StreamConfig) { c.Name = "OTHER" }),
			names: ownedStream,
		},
	}

	t.Logf("%d cases", len(cases))

	if len(cases) == 0 {
		t.Fatal("no unsafe streams to refuse")
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := establishUnsafe(t, testCase)
			if !errors.Is(err, corpus.ErrOwnership) {
				t.Fatalf("Establish() error = %v, want ErrOwnership", err)
			}

			if !strings.Contains(err.Error(), testCase.names) {
				t.Errorf("Establish() error = %v, want it to name %s", err, testCase.names)
			}
		})
	}
}

// establishUnsafe settles ownership in production order around one unsafe case.
func establishUnsafe(t *testing.T, testCase unsafeCase) error {
	t.Helper()

	store, b0 := openOwned(t)
	js := direct(t, store)

	if testCase.job != nil {
		if testCase.job.Mirror != nil {
			createStream(t, js, "ORIGIN", "origin.>")
		}

		if _, err := js.CreateStream(t.Context(), *testCase.job); err != nil {
			t.Fatalf("CreateStream(%s) error = %v", testCase.job.Name, err)
		}
	}

	before := survey(t, store)
	after := before

	if testCase.service != nil {
		if _, err := direct(t, store).CreateStream(t.Context(), *testCase.service); err != nil {
			t.Fatalf("CreateStream(%s) error = %v", testCase.service.Name, err)
		}

		after = survey(t, store)

		if err := store.Restore(t.Context(), b0); err != nil {
			t.Fatalf("Restore(B0) error = %v", err)
		}
	}

	return store.Establish(t.Context(), before, after, ownedCorpus())
}
