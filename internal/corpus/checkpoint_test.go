package corpus_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

// seededBucket and seededKey are what a job writes before the checkpoint.
const (
	seededBucket = "seeded"
	seededKey    = "k"
)

// TestARestoreReturnsTheStoreByteForByte: every run starts where the checkpoint was taken, so whatever
// a run created — a bucket, a stream, a key's next revision — is gone at the next start, and whatever
// it deleted is back.
func TestARestoreReturnsTheStoreByteForByte(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := startIn(t, root)

	before := seed(t, store)

	checkpoint, err := store.Checkpoint(t.Context(), filepath.Join(root, "B0"))
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	mutate(t, store)

	if restoreErr := store.Restore(t.Context(), checkpoint); restoreErr != nil {
		t.Fatalf("Restore() error = %v", restoreErr)
	}

	js := direct(t, store)

	bucket, err := js.KeyValue(t.Context(), seededBucket)
	if err != nil {
		t.Fatalf("KeyValue(%s) error = %v", seededBucket, err)
	}

	entry, err := bucket.Get(t.Context(), seededKey)
	if err != nil {
		t.Fatalf("Get(%s) error = %v", seededKey, err)
	}

	if entry.Revision() != before.revision || string(entry.Value()) != "seeded" {
		t.Errorf("key at revision %d = %q, want revision %d = seeded", entry.Revision(), entry.Value(), before.revision)
	}

	if _, bucketErr := js.KeyValue(t.Context(), "X"); !errors.Is(bucketErr, jetstream.ErrBucketNotFound) {
		t.Errorf("KeyValue(X) error = %v, want the bucket created after the checkpoint gone", bucketErr)
	}

	if _, streamErr := js.Stream(t.Context(), "S3"); !errors.Is(streamErr, jetstream.ErrStreamNotFound) {
		t.Errorf("Stream(S3) error = %v, want the stream created after the checkpoint gone", streamErr)
	}

	side, err := js.Stream(t.Context(), "SIDE")
	if err != nil {
		t.Fatalf("Stream(SIDE) error = %v", err)
	}

	if _, consumerErr := side.Consumer(t.Context(), "keeper"); consumerErr != nil {
		t.Errorf("Consumer(keeper) error = %v, want the consumer deleted after the checkpoint back", consumerErr)
	}

	info, err := side.Info(t.Context())
	if err != nil {
		t.Fatalf("Info() error = %v", err)
	}

	if info.State.LastSeq != before.lastSeq {
		t.Errorf("SIDE LastSeq = %d, want %d as seeded", info.State.LastSeq, before.lastSeq)
	}
}

// TestEveryRestartServesAFreshPort: a proxy still pointed at the previous address must reach nothing
// rather than a server that happens to have been handed the same port.
func TestEveryRestartServesAFreshPort(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := startIn(t, root)

	checkpoint, err := store.Checkpoint(t.Context(), filepath.Join(root, "B0"))
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	url, monitor := store.URL(), store.MonitorAddr()

	for restore := range 5 {
		if err := store.Restore(t.Context(), checkpoint); err != nil {
			t.Fatalf("Restore() %d error = %v", restore, err)
		}

		if store.URL() == url || store.MonitorAddr().Port() == monitor.Port() {
			t.Errorf("restore %d serves %s and monitoring %s, the previous start's", restore, store.URL(),
				store.MonitorAddr())
		}

		url, monitor = store.URL(), store.MonitorAddr()
	}
}

// TestACheckpointRefusesMemoryStorage: a memory-storage stream or consumer does not survive the
// restart, so a restore would silently drop it; the checkpoint is refused by name instead, and the
// server is never stopped.
func TestACheckpointRefusesMemoryStorage(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := startIn(t, root)
	js := direct(t, store)

	if _, err := js.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{
		Bucket:  "volatile",
		Storage: jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("CreateKeyValue() error = %v", err)
	}

	if _, err := js.CreateOrUpdateConsumer(t.Context(), corpus.StreamName, jetstream.ConsumerConfig{
		Durable:       "fleeting",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MemoryStorage: true,
	}); err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	url := store.URL()

	_, err := store.Checkpoint(t.Context(), filepath.Join(root, "B0"))
	if !errors.Is(err, corpus.ErrMemoryStorage) {
		t.Fatalf("Checkpoint() error = %v, want ErrMemoryStorage", err)
	}

	for _, name := range []string{"KV_volatile", "fleeting"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("Checkpoint() error = %v, want it to name %s", err, name)
		}
	}

	if store.URL() != url {
		t.Errorf("URL() = %s after a refused checkpoint, want %s: the server was stopped", store.URL(), url)
	}
}

// TestClearAfterACheckpointIsRefused: after a checkpoint the stream belongs to the checkpoint, and a
// rebuild would leave a restore returning something Clear had already thrown away.
func TestClearAfterACheckpointIsRefused(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := startIn(t, root)

	if err := store.Clear(t.Context()); err != nil {
		t.Fatalf("Clear() before any checkpoint error = %v", err)
	}

	if _, err := store.Checkpoint(t.Context(), filepath.Join(root, "B0")); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	if err := store.Clear(t.Context()); err == nil {
		t.Error("Clear() after a checkpoint error = nil, want a refusal")
	}
}

// TestACheckpointInsideTheStoreIsRefused: a restore replaces the store, so a checkpoint kept inside
// it would be destroyed by the first restore that needed it.
func TestACheckpointInsideTheStoreIsRefused(t *testing.T) {
	t.Parallel()

	store := startIn(t, t.TempDir())
	url := store.URL()
	inside := filepath.Join(store.StoreDir(), "B0")

	_, err := store.Checkpoint(t.Context(), inside)
	if err == nil || !strings.Contains(err.Error(), inside) {
		t.Fatalf("Checkpoint(%s) error = %v, want a refusal naming the path", inside, err)
	}

	if store.URL() != url {
		t.Errorf("URL() = %s, want %s: the server was stopped", store.URL(), url)
	}

	if _, err := direct(t, store).Stream(t.Context(), corpus.StreamName); err != nil {
		t.Errorf("the server stopped serving after a refused checkpoint: %v", err)
	}
}

// TestTheStoreStaysWhereItWasPut: the store and both checkpoints are the directories they were handed
// and nothing is written anywhere else.
func TestTheStoreStaysWhereItWasPut(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	bus := filepath.Join(root, "bus")

	if err := os.Mkdir(bus, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	store, err := corpus.Open(t.Context(), filepath.Join(bus, "store"), "ORDERS")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	js := direct(t, store)
	createStream(t, js, "ORDERS", "orders.>")

	if _, err := js.Publish(t.Context(), "orders.created", []byte(firstOrder)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	for _, name := range []string{"B0", "B1"} {
		checkpoint, checkpointErr := store.Checkpoint(t.Context(), filepath.Join(bus, name))
		if checkpointErr != nil {
			t.Fatalf("Checkpoint(%s) error = %v", name, checkpointErr)
		}

		if restoreErr := store.Restore(t.Context(), checkpoint); restoreErr != nil {
			t.Fatalf("Restore(%s) error = %v", name, restoreErr)
		}
	}

	allowed := []string{filepath.Join(bus, "store"), filepath.Join(bus, "B0"), filepath.Join(bus, "B1")}
	placed := 0

	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || path == root || path == bus {
			return err
		}

		for _, dir := range allowed {
			if path == dir || strings.HasPrefix(path, dir+string(filepath.Separator)) {
				if !entry.IsDir() {
					placed++
				}

				return nil
			}
		}

		t.Errorf("%s lies outside the store and its checkpoints", path)

		return nil
	})
	if walkErr != nil {
		t.Fatalf("WalkDir() error = %v", walkErr)
	}

	t.Logf("%d files under the store and its checkpoints", placed)

	if placed == 0 {
		t.Fatal("no files were written, so their placement proves nothing")
	}
}

// startIn starts a corpus whose store lives in root/store.
func startIn(t *testing.T, root string) *corpus.Corpus {
	t.Helper()

	store, err := corpus.Start(t.Context(), root)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	return store
}

// direct is a JetStream context on a connection of the test's own to wherever the server now serves.
// It is closed before the test's next checkpoint or restore by the caller not holding it past one.
//
//nolint:ireturn // the client library models JetStream as an interface.
func direct(t *testing.T, store *corpus.Corpus) jetstream.JetStream {
	t.Helper()

	connection, err := nats.Connect(store.URL(), nats.NoReconnect())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	t.Cleanup(connection.Close)

	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	return js
}

// seeded is what seed left: the key's revision and the side stream's last sequence.
type seeded struct {
	revision uint64
	lastSeq  uint64
}

// seed is a job's work before the checkpoint: a key written once, and a second stream holding a
// message with a durable consumer on it.
func seed(t *testing.T, store *corpus.Corpus) seeded {
	t.Helper()

	js := direct(t, store)

	bucket, err := js.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: seededBucket})
	if err != nil {
		t.Fatalf("CreateKeyValue() error = %v", err)
	}

	revision, err := bucket.Put(t.Context(), seededKey, []byte("seeded"))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	createStream(t, js, "SIDE", "side.>")

	ack, err := js.Publish(t.Context(), "side.note", []byte("note"))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if _, err := js.CreateOrUpdateConsumer(t.Context(), "SIDE", jetstream.ConsumerConfig{
		Durable:   "keeper",
		AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("CreateOrUpdateConsumer() error = %v", err)
	}

	return seeded{revision: revision, lastSeq: ack.Sequence}
}

// mutate is a run's work after the checkpoint: the key rewritten, a bucket and a stream created, the
// consumer deleted.
func mutate(t *testing.T, store *corpus.Corpus) {
	t.Helper()

	js := direct(t, store)

	bucket, err := js.KeyValue(t.Context(), seededBucket)
	if err != nil {
		t.Fatalf("KeyValue() error = %v", err)
	}

	if _, err := bucket.Put(t.Context(), seededKey, []byte("handled")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if _, err := js.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: "X"}); err != nil {
		t.Fatalf("CreateKeyValue(X) error = %v", err)
	}

	if err := js.DeleteConsumer(t.Context(), "SIDE", "keeper"); err != nil {
		t.Fatalf("DeleteConsumer() error = %v", err)
	}

	createStream(t, js, "S3", "s3.>")
}

// createStream makes a file stream capturing one subject filter.
func createStream(t *testing.T, js jetstream.JetStream, name, subjects string) {
	t.Helper()

	config := jetstream.StreamConfig{Name: name, Subjects: []string{subjects}}
	if _, err := js.CreateStream(t.Context(), config); err != nil {
		t.Fatalf("CreateStream(%s) error = %v", name, err)
	}
}
