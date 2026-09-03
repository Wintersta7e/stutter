package nats_test

import (
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/effect"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

const (
	subject = "orders.dispatched"
	payload = `{"order_id":"ORD-9001"}`
	bucket  = "sent-envelopes"
	key     = "ORD-9001"
	// startupTimeout bounds how long the embedded bus has to accept connections.
	startupTimeout = 10 * time.Second
	// decimal is the base a stream revision is rendered in.
	decimal = 10
)

// observed is one effect as the proxy reported it.
type observed struct {
	kind effect.Kind
	text string
}

// recorder is the Sink the proxy writes to. The proxy records from its own goroutines, so it locks.
type recorder struct {
	entries []observed
	mu      sync.Mutex
}

func (r *recorder) Record(kind effect.Kind, raw, printable string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if raw != printable {
		panic("the readable form and the comparable form were expected to match")
	}

	r.entries = append(r.entries, observed{kind: kind, text: raw})
}

func (r *recorder) texts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, 0, len(r.entries))
	for _, item := range r.entries {
		out = append(out, item.text)
	}

	return out
}

// kinds reports the distinct effect kinds recorded, so a test can assert every effect was
// classified as a publish onto the bus.
func (r *recorder) kinds() map[effect.Kind]int {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[effect.Kind]int, len(r.entries))
	for _, item := range r.entries {
		out[item.kind]++
	}

	return out
}

// matching returns the recorded effects that begin with prefix.
//
// Opening a bucket is itself a stream API request through the proxy, so a test that is about what a
// handler does with a bucket filters for the key/value lines rather than counting everything.
func (r *recorder) matching(prefix string) []string {
	var out []string

	for _, text := range r.texts() {
		if strings.HasPrefix(text, prefix) {
			out = append(out, text)
		}
	}

	return out
}

// embeddedBus starts a real NATS server with JetStream enabled. The wire protocol is the thing under
// test, and a stub server would prove nothing about it.
func embeddedBus(t *testing.T) *server.Server {
	t.Helper()

	bus, err := server.NewServer(&server.Options{
		ServerName: "stutter-proxy-test",
		Host:       "127.0.0.1",
		Port:       server.RANDOM_PORT,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		NoLog:      true,
		NoSigs:     true,
	})
	if err != nil {
		t.Fatalf("server.NewServer() error = %v", err)
	}

	bus.Start()

	t.Cleanup(func() {
		bus.Shutdown()
		bus.WaitForShutdown()
	})

	if !bus.ReadyForConnections(startupTimeout) {
		t.Fatal("the embedded bus was not ready for connections")
	}

	return bus
}

// startProxy puts a proxy in front of upstream and returns the sink it records into.
func startProxy(t *testing.T, upstream string) (*recorder, string) {
	t.Helper()

	sink := &recorder{}

	front, err := natsproxy.Listen(t.Context(), "127.0.0.1:0", upstream, sink)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	// Surfaced rather than discarded: a proxy that died mid-run would otherwise present as a handler
	// that simply stopped producing effects.
	served := make(chan error, 1)
	go func() { served <- front.Serve(t.Context()) }()

	t.Cleanup(func() {
		if err := front.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}

		if err := <-served; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	})

	return sink, front.Addr()
}

// dial opens a raw TCP connection to the proxy, for the cases that have to speak the protocol by
// hand rather than through a client library.
func dial(t *testing.T, addr string) net.Conn {
	t.Helper()

	var dialer net.Dialer

	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	if err := conn.SetDeadline(time.Now().Add(startupTimeout)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}

	return conn
}

func connect(t *testing.T, addr string) *nats.Conn {
	t.Helper()

	conn, err := nats.Connect("nats://" + addr)
	if err != nil {
		t.Fatalf("nats.Connect(%q) error = %v", addr, err)
	}

	t.Cleanup(conn.Close)

	return conn
}

// flush makes the assertions deterministic without polling: the proxy reads the client's messages in
// order, so once the round trip a flush performs has completed, everything sent before it has been
// inspected and forwarded.
func flush(t *testing.T, conn *nats.Conn) {
	t.Helper()

	if err := conn.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestPublishThroughTheProxyIsRecorded(t *testing.T) {
	t.Parallel()

	sink, addr := startProxy(t, embeddedBus(t).Addr().String())
	conn := connect(t, addr)

	if err := conn.Publish(subject, []byte(payload)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	flush(t, conn)

	want := "publish subject=" + subject + " payload=" + payload

	if got := sink.texts(); len(got) != 1 || got[0] != want {
		t.Errorf("effects = %q, want exactly [%q]", got, want)
	}

	if got := sink.kinds(); got[effect.KindNATS] != 1 || len(got) != 1 {
		t.Errorf("effect kinds = %v, want one %s", got, effect.KindNATS)
	}
}

// TestReceivedTrafficIsNotAnEffect holds the line the whole design rests on: divergence is decided
// by what the service did, not by what it was told or asked to be told.
func TestReceivedTrafficIsNotAnEffect(t *testing.T) {
	t.Parallel()

	bus := embeddedBus(t)
	sink, addr := startProxy(t, bus.Addr().String())
	conn := connect(t, addr)

	sub, err := conn.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("SubscribeSync() error = %v", err)
	}

	flush(t, conn)

	// Delivered from a second connection, so nothing the subscriber itself sent can be mistaken for
	// the message it receives.
	sender := connect(t, bus.Addr().String())
	if err := sender.Publish(subject, []byte(payload)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if _, err := sub.NextMsg(startupTimeout); err != nil {
		t.Fatalf("NextMsg() error = %v", err)
	}

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe() error = %v", err)
	}

	flush(t, conn)

	if got := sink.texts(); len(got) != 0 {
		t.Errorf("effects = %q, want none: connecting, subscribing and receiving are not effects", got)
	}
}

// TestKeyValueOperationsAreNamed is why this package exists. The commonest bus-native idempotency
// guard is a key/value claim, which on the wire is only a publish; unless the report says which key
// in which bucket was claimed, and whether the write was an atomic claim or an ordinary put, a
// finding cannot be acted on without reading Stutter's source.
func TestKeyValueOperationsAreNamed(t *testing.T) {
	t.Parallel()

	bus := embeddedBus(t)

	// The bucket is created on a direct connection: creating one is a stream-management call, and
	// this test is about what a handler does with a bucket rather than about provisioning it.
	setup, err := jetstream.New(connect(t, bus.Addr().String()))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	if _, created := setup.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: bucket}); created != nil {
		t.Fatalf("CreateKeyValue() error = %v", created)
	}

	sink, addr := startProxy(t, bus.Addr().String())

	proxied, err := jetstream.New(connect(t, addr))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	store, err := proxied.KeyValue(t.Context(), bucket)
	if err != nil {
		t.Fatalf("KeyValue() error = %v", err)
	}

	revision, err := store.Create(t.Context(), key, []byte(payload))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if _, err := store.Update(t.Context(), key, []byte(payload), revision); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	if _, err := store.Put(t.Context(), key, []byte(payload)); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if err := store.Delete(t.Context(), key); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Every JetStream publish is a request, so each line carries the flattened reply inbox: left as
	// the random subject it is on the wire, no two runs of the same handler could ever agree.
	got := sink.matching("kv.")
	want := []string{
		"kv.create bucket=" + bucket + " key=" + key + " reply=<inbox> payload=" + payload,
		"kv.update bucket=" + bucket + " key=" + key + " revision=" +
			strconv.FormatUint(revision, decimal) + " reply=<inbox> payload=" + payload,
		"kv.put bucket=" + bucket + " key=" + key + " reply=<inbox> payload=" + payload,
		"kv.delete bucket=" + bucket + " key=" + key + " reply=<inbox>",
	}

	if len(got) != len(want) {
		t.Fatalf("key/value effects = %q, want %q", got, want)
	}

	for index := range want {
		if got[index] != want[index] {
			t.Errorf("effect %d = %q, want %q", index, got[index], want[index])
		}
	}
}

// TestClaimOnATakenKeyStillShows is the case the tool is built to see. A duplicate delivery reaches
// a guarded handler, its claim is refused by the bus, and the claim must still appear: an effect the
// proxy did not record is a handler that looks like it did nothing.
func TestClaimOnATakenKeyStillShows(t *testing.T) {
	t.Parallel()

	bus := embeddedBus(t)

	setup, err := jetstream.New(connect(t, bus.Addr().String()))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	if _, created := setup.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: bucket}); created != nil {
		t.Fatalf("CreateKeyValue() error = %v", created)
	}

	sink, addr := startProxy(t, bus.Addr().String())

	proxied, err := jetstream.New(connect(t, addr))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	store, err := proxied.KeyValue(t.Context(), bucket)
	if err != nil {
		t.Fatalf("KeyValue() error = %v", err)
	}

	if _, err := store.Create(t.Context(), key, []byte(payload)); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if _, err := store.Create(t.Context(), key, []byte(payload)); err == nil {
		t.Fatal("the second claim on a taken key succeeded; the bucket is not enforcing it")
	}

	const claimed = 2

	claims := sink.matching("kv.create bucket=" + bucket + " key=" + key)
	if len(claims) != claimed {
		t.Errorf("claims = %q, want the refused one recorded alongside the accepted one", claims)
	}
}

// stubBus answers one connection with the given greeting and then echoes whatever it receives.
//
// Echoing is what makes "forwards bytes untouched" testable: the client reads back exactly what the
// proxy passed on. It also lets the refusal paths be exercised without a certificate.
func stubBus(t *testing.T, greeting string) string {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		if _, err := conn.Write([]byte(greeting)); err != nil {
			return
		}

		//nolint:errcheck // the echo ends when the peer goes away, which is the only outcome here.
		_, _ = io.Copy(conn, conn)
	}()

	return listener.Addr().String()
}

// refusal drains the client connection and returns what the proxy recorded once it hung up.
func refusal(t *testing.T, sink *recorder, conn net.Conn) []string {
	t.Helper()

	if _, err := io.ReadAll(conn); err != nil {
		t.Fatalf("read from the proxied connection: %v", err)
	}

	return sink.texts()
}

// TestServerRequiringTLSIsRefused covers the sign that arrives first: the bus tells the client to
// upgrade, so the proxy knows the rest of the connection will be unreadable before a byte of it is.
func TestServerRequiringTLSIsRefused(t *testing.T) {
	t.Parallel()

	sink, addr := startProxy(t, stubBus(t, `INFO {"tls_required":true}`+"\r\n"))

	got := refusal(t, sink, dial(t, addr))
	if len(got) != 1 || !strings.Contains(got[0], "TLS") {
		t.Errorf("effects = %q, want one refusal naming TLS", got)
	}
}

// TestClientUpgradingToTLSIsRefused covers the other sign: the bus merely offers TLS and the client
// takes it, so what arrives where a CONNECT belongs is a handshake record.
//
// Refusing loudly is the whole point. A connection the proxy cannot read reports a handler as having
// produced no side effects at all, which reads as "idempotent" — the most dangerous wrong answer
// this tool can give.
func TestClientUpgradingToTLSIsRefused(t *testing.T) {
	t.Parallel()

	sink, addr := startProxy(t, stubBus(t, `INFO {"tls_available":true}`+"\r\n"))
	conn := dial(t, addr)

	// The opening bytes of a TLS handshake record, where a CONNECT would otherwise be.
	if _, err := conn.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x01}); err != nil {
		t.Fatalf("write a handshake record: %v", err)
	}

	got := refusal(t, sink, conn)
	if len(got) != 1 || !strings.Contains(got[0], "TLS") {
		t.Errorf("effects = %q, want one refusal naming TLS", got)
	}
}

// TestUnparsableTrafficKeepsTheConnection is the promise made to a service under test: a message
// Stutter cannot frame costs an effect, never the connection.
//
// The malformed publish leaves the proxy unable to say where the next control line begins, so it
// stops looking — and every byte after it still reaches the bus unaltered, in order, including the
// well-formed publish the proxy has given up reading.
func TestUnparsableTrafficKeepsTheConnection(t *testing.T) {
	t.Parallel()

	const greeting = `INFO {"headers":true}` + "\r\n"

	sink, addr := startProxy(t, stubBus(t, greeting))
	conn := dial(t, addr)

	sent := "CONNECT {\"verbose\":false}\r\n" +
		"PUB " + subject + " notanumber\r\n" +
		"PUB " + subject + " 2\r\nhi\r\n"

	if _, err := conn.Write([]byte(sent)); err != nil {
		t.Fatalf("write to the proxied connection: %v", err)
	}

	echoed := make([]byte, len(greeting)+len(sent))
	if _, err := io.ReadFull(conn, echoed); err != nil {
		t.Fatalf("read the echo back: %v", err)
	}

	if got := string(echoed); got != greeting+sent {
		t.Errorf("the bus received %q, want %q", got, greeting+sent)
	}

	if got := sink.texts(); len(got) != 0 {
		t.Errorf("effects = %q, want none: the framing was lost before either publish was readable", got)
	}
}
