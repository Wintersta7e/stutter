package corpus_test

import (
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

const (
	ackWait   = 2 * time.Second
	fetchWait = 5 * time.Second
	subject   = corpus.SubjectPrefix + "order.created"
)

func start(t *testing.T) *corpus.Corpus {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	return store
}

func TestReplayDeliversInStreamOrder(t *testing.T) {
	t.Parallel()

	store := start(t)
	payloads := [][]byte{
		[]byte(`{"order_id":"ORD-1"}`),
		[]byte(`{"order_id":"ORD-2"}`),
		[]byte(`{"order_id":"ORD-3"}`),
	}

	for _, payload := range payloads {
		if _, err := store.Publish(t.Context(), subject, payload); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	replay, err := store.Replay(t.Context(), "order-test",
		corpus.ConsumerOptions{AckWait: ackWait, MaxDeliver: 3})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	for index, want := range payloads {
		delivery, err := replay.Next(t.Context(), fetchWait)
		if err != nil {
			t.Fatalf("Next() at index %d error = %v", index, err)
		}

		if string(delivery.Payload) != string(want) {
			t.Errorf("payload at index %d = %s, want %s", index, delivery.Payload, want)
		}

		if err := delivery.Ack(); err != nil {
			t.Fatalf("Ack() error = %v", err)
		}
	}

	if _, err := replay.Next(t.Context(), time.Second); !errors.Is(err, corpus.ErrDrained) {
		t.Errorf("Next() after the last message error = %v, want ErrDrained", err)
	}
}

// TestWithheldAckRedelivers is the mechanism the duplicate and crash-before-ack mutations are built
// on. If the bus does not redeliver a message whose ack was withheld, neither mutation exists and
// the product has nothing to test with.
func TestWithheldAckRedelivers(t *testing.T) {
	t.Parallel()

	store := start(t)

	seq, err := store.Publish(t.Context(), subject, []byte(`{"order_id":"ORD-99001","qty":3}`))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	replay, err := store.Replay(t.Context(), "redelivery-test",
		corpus.ConsumerOptions{AckWait: ackWait, MaxDeliver: 5})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	firstDelivery, err := replay.Next(t.Context(), fetchWait)
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}

	if firstDelivery.Seq != seq {
		t.Errorf("first delivery Seq = %d, want %d", firstDelivery.Seq, seq)
	}

	if firstDelivery.Deliveries != 1 {
		t.Errorf("first delivery Deliveries = %d, want 1", firstDelivery.Deliveries)
	}

	if nakErr := firstDelivery.Nak(); nakErr != nil {
		t.Fatalf("Nak() error = %v", nakErr)
	}

	redelivered, err := replay.Next(t.Context(), fetchWait)
	if err != nil {
		t.Fatalf("Next() after Nak error = %v; the bus did not redeliver", err)
	}

	if redelivered.Seq != seq {
		t.Errorf("redelivered Seq = %d, want the same message %d", redelivered.Seq, seq)
	}

	if redelivered.Deliveries != 2 {
		t.Errorf("redelivered Deliveries = %d, want 2", redelivered.Deliveries)
	}

	if string(redelivered.Payload) != `{"order_id":"ORD-99001","qty":3}` {
		t.Errorf("redelivered payload = %s, want it byte-identical to the original", redelivered.Payload)
	}

	if err := redelivered.Ack(); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	if _, err := replay.Next(t.Context(), time.Second); !errors.Is(err, corpus.ErrDrained) {
		t.Errorf("Next() after ack error = %v, want ErrDrained", err)
	}
}

// TestOpenStartsTheServerWithNoStream: who owns the stream is learned before anything creates it, so
// opening the bus creates none, yet every stream-scoped read already names the one it was bound to.
func TestOpenStartsTheServerWithNoStream(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "bus"), 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	dir := filepath.Join(root, "bus", "store")

	store, err := corpus.Open(t.Context(), dir, "ORDERS")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	if got := store.Topic().Stream; got != "ORDERS" {
		t.Errorf("Topic().Stream = %q, want ORDERS", got)
	}

	if got := store.StoreDir(); got != dir {
		t.Errorf("StoreDir() = %q, want %q", got, dir)
	}

	if names := streamNames(t, store.URL()); len(names) != 0 {
		t.Errorf("Open() created streams %q, want none", names)
	}

	refused := refusedStoreDirs(t, root)

	seen := make(map[string]string, len(refused))

	for name, path := range refused {
		_, openErr := corpus.Open(t.Context(), path, "ORDERS")
		if openErr == nil {
			t.Errorf("Open(%s) error = nil, want a refusal", name)

			continue
		}

		if !strings.Contains(openErr.Error(), path) {
			t.Errorf("Open(%s) error = %v, want it to name %s", name, openErr, path)
		}

		if other, repeated := seen[openErr.Error()]; repeated {
			t.Errorf("Open(%s) and Open(%s) failed alike (%v), want distinct errors", name, other, openErr)
		}

		seen[openErr.Error()] = name
	}
}

// refusedStoreDirs builds one store directory Open must refuse per way of getting it wrong.
func refusedStoreDirs(t *testing.T, root string) map[string]string {
	t.Helper()

	file := filepath.Join(root, "a-file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	full := filepath.Join(root, "full")
	if err := os.Mkdir(full, 0o700); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	if err := os.WriteFile(filepath.Join(full, "left-behind"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	return map[string]string{
		"a regular file":      file,
		"a non-empty dir":     full,
		"under absent parent": filepath.Join(root, "absent", "store"),
	}
}

// TestTheServerListensOnLoopbackOnly: the bus and its monitoring answer on this host alone, each on a
// port the kernel picked, read from the running server rather than configured.
func TestTheServerListensOnLoopbackOnly(t *testing.T) {
	t.Parallel()

	store := start(t)

	parsed, err := url.Parse(store.URL())
	if err != nil {
		t.Fatalf("parse URL() %q: %v", store.URL(), err)
	}

	if parsed.Hostname() != "127.0.0.1" || parsed.Port() == "" || parsed.Port() == "0" {
		t.Errorf("URL() = %q, want 127.0.0.1 on a kernel-assigned port", store.URL())
	}

	monitor := store.MonitorAddr()
	if monitor.Addr() != netip.MustParseAddr("127.0.0.1") || monitor.Port() == 0 {
		t.Errorf("MonitorAddr() = %s, want 127.0.0.1 on a kernel-assigned port", monitor)
	}

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+monitor.String()+"/varz", nil)
	if err != nil {
		t.Fatalf("build the monitoring request: %v", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET /varz error = %v", err)
	}

	_ = response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Errorf("GET /varz status = %d, want 200", response.StatusCode)
	}
}

// streamNames lists every stream on the server, on a connection of the test's own.
func streamNames(t *testing.T, address string) []string {
	t.Helper()

	connection, err := nats.Connect(address)
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	defer connection.Close()

	stream, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	lister := stream.StreamNames(t.Context())

	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}

	if err := lister.Err(); err != nil {
		t.Fatalf("list streams: %v", err)
	}

	return names
}

// TestAnNkeyClientConnects: a service authenticating with an nkey signs a nonce the server hands it,
// and refuses to connect to a server that offers none. The embedded server has no auth, so the key
// is never checked — only the nonce's presence decides whether such a service reaches the bus.
func TestAnNkeyClientConnects(t *testing.T) {
	t.Parallel()

	const userKey = "UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

	sign := func([]byte) ([]byte, error) { return make([]byte, 64), nil }

	connection, err := nats.Connect(start(t).URL(), nats.Nkey(userKey, sign))
	if err != nil {
		t.Fatalf("nats.Connect() with an nkey error = %v", err)
	}

	connection.Close()
}

func TestCorpusURLIsReachable(t *testing.T) {
	t.Parallel()

	if address := start(t).URL(); address == "" {
		t.Error("URL() is empty; a service under test could not reach the bus")
	}
}
