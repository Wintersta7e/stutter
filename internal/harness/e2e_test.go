package harness_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// errNoSandboxCA means the sandbox served TLS without handing back the CA to trust it with.
var errNoSandboxCA = errors.New("the sandbox produced no CA certificate")

// counting wraps a session so a test can prove a fault was actually injected.
//
// It exists because a control that passes with nothing having run is indistinguishable from a
// control that passes because the handler is safe, and only one of those is worth anything.
type counting struct {
	inner  check.Session
	faults int
}

func (c *counting) Reset(ctx context.Context) error {
	return c.inner.Reset(ctx)
}

func (c *counting) Run(
	ctx context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	if mutation.Fault() != policy.FaultNone {
		c.faults++
	}

	return c.inner.Run(ctx, name, mutation, retain)
}

const (
	envPostgres = "STUTTER_TEST_POSTGRES"
	filterAll   = "corpus.>"
	// guardHost is the stable logical name the stub answers under. The listener's port changes every
	// run; this does not, which is what makes two runs comparable.
	guardHost  = "guard.example.test"
	hashKeyLen = 32
	orderedQty = 3
)

// guarded closes both the consumer and its claim guard. The guard holds its own bus connection, and
// both must be released before the proxies are: a proxy's Close waits for in-flight connections, so
// an idle open one turns teardown into a hang rather than an error.
type guarded struct {
	*toy.Consumer

	guard *toy.Guard
}

// httpGuarded models a handler whose idempotency decision comes from an HTTP dependency. The
// synthetic dependency always says the work is unclaimed, which means a duplicate exposes the
// planted database bug but must be marked guard-dependent rather than trusted as a firm failure.
type httpGuarded struct {
	consumer *toy.Consumer
	client   *http.Client
	baseURL  string
}

func (h *httpGuarded) Handle(ctx context.Context, msg replay.Message) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.baseURL+"/claimed", nil)
	if err != nil {
		return fmt.Errorf("build guard request: %w", err)
	}

	response, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("read guard: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	var answer struct {
		Claimed bool `json:"claimed"`
	}
	if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
		return fmt.Errorf("decode guard response: %w", err)
	}

	if answer.Claimed {
		return nil
	}

	return h.consumer.Handle(ctx, msg)
}

func (h *httpGuarded) Close(ctx context.Context) {
	h.client.CloseIdleConnections()
	h.consumer.Close(ctx)
}

func (g *guarded) Close(ctx context.Context) {
	g.guard.Close()
	g.Consumer.Close(ctx)
}

func sandbox(t *testing.T, sku string, store *corpus.Corpus, config policy.Config) *harness.Sandbox {
	t.Helper()

	directDSN := os.Getenv(envPostgres)

	key := make([]byte, hashKeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate hash key: %v", err)
	}

	built, err := harness.New(harness.Config{
		Corpus:      store,
		PostgresDSN: directDSN,
		HashKey:     key,
		Policy:      config,
		Quiesce:     50 * time.Millisecond,

		// Reset uses the DIRECT addresses: fixture writes are not the service's behaviour, and
		// recording them would put noise into every effect sequence.
		Reset: func(ctx context.Context) error {
			if err := toy.Setup(ctx, directDSN, sku); err != nil {
				return err
			}

			guard, err := toy.NewGuard(ctx, store.URL())
			if err != nil {
				return err
			}

			defer guard.Close()

			return guard.Reset(ctx)
		},

		// Connect uses the PROXIED addresses. Routing the guard through the bus proxy is the whole
		// point: its claim is a publish to $KV, and unobserved it would be invisible.
		Connect: func(ctx context.Context, at harness.Addresses) (harness.Service, error) {
			consumer, err := toy.Connect(ctx, at.Postgres)
			if err != nil {
				return nil, err
			}

			guard, err := toy.NewGuard(ctx, at.NATS)
			if err != nil {
				consumer.Close(ctx)

				return nil, err
			}

			consumer.UseGuard(guard)

			return &guarded{Consumer: consumer, guard: guard}, nil
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built
}

func httpGuardSandbox(t *testing.T, sku string, store *corpus.Corpus, config policy.Config) *harness.Sandbox {
	t.Helper()

	directDSN := os.Getenv(envPostgres)

	key := make([]byte, hashKeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate hash key: %v", err)
	}

	built, err := harness.New(harness.Config{
		Corpus:      store,
		PostgresDSN: directDSN,
		HTTPHost:    guardHost,
		HTTPDefault: httpproxy.Response{
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       []byte(`{"claimed":true}`),
			StatusCode: http.StatusOK,
		},
		HTTPRoutes: []httpproxy.Route{{
			Method: http.MethodGet,
			Host:   guardHost,
			Path:   "/claimed",
			Response: httpproxy.Response{
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       []byte(`{"claimed":false}`),
				StatusCode: http.StatusOK,
			},
		}},
		HashKey: key,
		Policy:  config,
		Quiesce: 50 * time.Millisecond,
		Reset: func(ctx context.Context) error {
			return toy.Setup(ctx, directDSN, sku)
		},
		Connect: func(ctx context.Context, at harness.Addresses) (harness.Service, error) {
			consumer, err := toy.Connect(ctx, at.Postgres)
			if err != nil {
				return nil, err
			}

			return &httpGuarded{
				consumer: consumer,
				client:   &http.Client{Timeout: time.Second},
				baseURL:  at.HTTP,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built
}

func orderPayload(orderID, sku string) []byte {
	return fmt.Appendf(nil, `{"order_id":%q,"sku":%q,"qty":%d}`, orderID, sku, orderedQty)
}

// TestCheckReportsThePlantedBugEndToEnd is the whole tool in one test: record, replay through both
// proxies, gate, inject every permitted fault, shrink, and rule. Nothing here is stubbed — a real
// Postgres, a real embedded bus, and a consumer that genuinely loses stock.
func TestCheckReportsThePlantedBugEndToEnd(t *testing.T) {
	t.Parallel()

	if os.Getenv(envPostgres) == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	sku := "WIDGET-" + t.Name()

	seq, err := store.Publish(t.Context(), toy.SubjectOrderCreated, orderPayload("ORD-1", sku))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	// One message and a serial consumer: reorder and concurrency are forbidden by the config, so
	// the run stays short while still exercising every fault that is legal here.
	config := policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{filterAll},
		AckWait:        time.Second,
		MaxDeliver:     6,
		MaxAckPending:  1,
	}

	result, err := check.Run(t.Context(), sandbox(t, sku, store, config), check.Options{
		Messages: []uint64{seq},
		Consumer: "reserve_stock",
		Config:   config,
		MaxRuns:  4,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	rendered := result.String()
	t.Logf("report:\n%s", rendered)

	if got := result.ExitCode(); got != 1 {
		t.Fatalf("ExitCode() = %d, want 1 — the planted bug was not reported\n%s", got, rendered)
	}

	if len(result.Findings) == 0 {
		t.Fatal("no findings")
	}

	found := result.Findings[0]
	if found.Status != report.StatusFail {
		t.Errorf("Status = %q, want FAIL (reservations: %v)", found.Status, found.Reservations)
	}

	if found.Clause == "" {
		t.Error("the finding carries no configuration clause, so a user cannot trace why the fault was legal")
	}

	if found.Repro == "" {
		t.Error("the finding carries no minimal repro")
	}

	if !strings.Contains(rendered, "FAIL") {
		t.Errorf("rendered report has no FAIL line:\n%s", rendered)
	}
}

// TestGuardedHandlerIsNotReported is the false-positive control, now running through the full check
// rather than by inspecting stock directly. A handler made safe by a claim must produce a clean
// report; if it does not, the tool is telling someone their working guard is broken.
func TestGuardedHandlerIsNotReported(t *testing.T) {
	t.Parallel()

	if os.Getenv(envPostgres) == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	sku := "WIDGET-" + t.Name()

	seq, err := store.Publish(t.Context(), toy.SubjectOrderGuarded, orderPayload("ORD-2", sku))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	config := policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{filterAll},
		AckWait:        time.Second,
		MaxDeliver:     6,
		MaxAckPending:  1,
	}

	session := &counting{inner: sandbox(t, sku, store, config)}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: []uint64{seq},
		Consumer: "reserve_stock_guarded",
		Config:   config,
		MaxRuns:  2,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	t.Logf("report:\n%s", result)

	if session.faults == 0 {
		t.Fatal("no fault was injected, so a clean report proves nothing about the guard")
	}

	for _, found := range result.Findings {
		if found.Status == report.StatusFail {
			t.Errorf("a correctly guarded handler was reported as failing:\n%s", result)
		}
	}
}

// TestHTTPGuardIsMarkedGuardDependent exercises the whole metadata path: HTTP observation, frozen
// response replay, off-script fallback, effect metadata, per-message indexing, and report ruling.
func TestHTTPGuardIsMarkedGuardDependent(t *testing.T) {
	t.Parallel()

	if os.Getenv(envPostgres) == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	sku := "WIDGET-" + t.Name()

	seq, err := store.Publish(t.Context(), toy.SubjectOrderCreated, orderPayload("ORD-HTTP", sku))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	config := policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{filterAll},
		AckWait:        time.Second,
		MaxDeliver:     6,
		MaxAckPending:  1,
	}
	session := &counting{inner: httpGuardSandbox(t, sku, store, config)}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: []uint64{seq},
		Consumer: "reserve_stock_http_guarded",
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if session.faults == 0 {
		t.Fatal("no fault was injected, so the guard-dependent finding proves nothing")
	}

	if len(result.Findings) != 1 {
		t.Fatalf("Findings = %d, want 1\n%s", len(result.Findings), result)
	}

	found := result.Findings[0]
	if found.Confidence != report.ConfidenceGuardDependent {
		t.Errorf("Confidence = %q, want %q", found.Confidence, report.ConfidenceGuardDependent)
	}

	if found.Status != report.StatusWarn {
		t.Errorf("Status = %q, want WARN (reservations: %v)", found.Status, found.Reservations)
	}

	if !strings.Contains(result.String(), "off-script") {
		t.Errorf("report omitted the off-script call:\n%s", result)
	}
}

// opaqueClient is a handler whose datastore speaks a protocol Stutter does not parse, and which has
// no database at all. It is the case the opaque fallback exists for: without it this handler
// produces no effects whatsoever, and a handler with no effects reads as idempotent.
type opaqueClient struct {
	connection net.Conn
	replies    *bufio.Reader
}

func (o *opaqueClient) Handle(_ context.Context, msg replay.Message) error {
	var order struct {
		OrderID string `json:"order_id"`
		Qty     int    `json:"qty"`
	}

	if err := json.Unmarshal(msg.Payload, &order); err != nil {
		return fmt.Errorf("decode the order: %w", err)
	}

	if _, err := fmt.Fprintf(o.connection, "RESERVE %s %d\n", order.OrderID, order.Qty); err != nil {
		return fmt.Errorf("reserve stock: %w", err)
	}

	if _, err := o.replies.ReadString('\n'); err != nil {
		return fmt.Errorf("read the reservation: %w", err)
	}

	return nil
}

func (o *opaqueClient) Close(context.Context) { _ = o.connection.Close() }

// startLineDependency stands in for a datastore Stutter has no parser for. It answers every line
// identically, so the only thing that can differ between two runs is what the handler asked for.
func startLineDependency(t *testing.T) string {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the unparsed dependency: %v", err)
	}

	var served sync.WaitGroup

	served.Go(func() {
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			served.Go(func() {
				defer func() { _ = connection.Close() }()

				reader := bufio.NewReader(connection)

				for {
					if _, readErr := reader.ReadString('\n'); readErr != nil {
						return
					}

					if _, writeErr := io.WriteString(connection, "+OK\n"); writeErr != nil {
						return
					}
				}
			})
		}
	})

	t.Cleanup(func() {
		_ = listener.Close()

		served.Wait()
	})

	return listener.Addr().String()
}

// tlsClient is a handler whose dependency will only speak TLS, which is what most real ones do. It
// trusts the sandbox CA and nothing else, so a stub serving the wrong certificate is a connection
// error rather than a quietly unobserved call.
type tlsClient struct {
	client  *http.Client
	baseURL string
}

func (c *tlsClient) Handle(ctx context.Context, _ replay.Message) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/claimed", nil)
	if err != nil {
		return fmt.Errorf("build guard request: %w", err)
	}

	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("read guard: %w", err)
	}

	defer func() { _ = response.Body.Close() }()

	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		return fmt.Errorf("drain guard response: %w", err)
	}

	return nil
}

func (c *tlsClient) Close(context.Context) { c.client.CloseIdleConnections() }

// trustBuilder is the certificate pool the service under test will use to reach the stub.
type trustBuilder func(caPEM []byte) (*x509.CertPool, error)

// trustSandboxCA is what a cooperating service does: trust the CA the sandbox minted.
func trustSandboxCA(caPEM []byte) (*x509.CertPool, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errNoSandboxCA
	}

	return roots, nil
}

// trustNothing is what certificate pinning and an embedded certificate pool look like from the
// stub's side: the sandbox CA is never consulted and every handshake is rejected.
func trustNothing([]byte) (*x509.CertPool, error) {
	return x509.NewCertPool(), nil
}

// tlsSandbox builds a sandbox whose HTTP stub speaks TLS.
func tlsSandbox(
	t *testing.T,
	store *corpus.Corpus,
	config policy.Config,
	key []byte,
	trust trustBuilder,
) *harness.Sandbox {
	t.Helper()

	built, err := harness.New(harness.Config{
		Corpus:   store,
		HTTPHost: guardHost,
		HTTPTLS:  true,
		HashKey:  key,
		Policy:   config,
		Quiesce:  50 * time.Millisecond,
		Connect: func(_ context.Context, at harness.Addresses) (harness.Service, error) {
			roots, trustErr := trust(at.HTTPCACert)
			if trustErr != nil {
				return nil, trustErr
			}

			return &tlsClient{
				baseURL: at.HTTP,
				client: &http.Client{
					Timeout: time.Second,
					Transport: &http.Transport{
						TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
					},
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built
}

// TestTLSDependencyIsObserved closes the gap that a real service hits immediately: a dependency
// reached over HTTPS used to stop the run, because the stub only spoke cleartext. An unobserved
// dependency reads as a handler that did nothing, so this has to work before the tool can be
// pointed at anything real.
func TestTLSDependencyIsObserved(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	seq, err := store.Publish(
		t.Context(),
		toy.SubjectOrderCreated,
		orderPayload("ORD-TLS", "WIDGET-TLS"),
	)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	config := policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{filterAll},
		AckWait:        time.Second,
		MaxDeliver:     6,
		MaxAckPending:  1,
	}

	session := &counting{inner: tlsSandbox(t, store, config, key, trustSandboxCA)}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: []uint64{seq},
		Consumer: "reserve_stock_tls",
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if session.faults == 0 {
		t.Fatal("no fault was injected, so this proves nothing")
	}

	// A handler that never reached its dependency produces no effects, and no effects means no
	// divergence — so the finding IS the proof that TLS egress was observed.
	if len(result.Findings) != 1 {
		t.Fatalf("Findings = %d, want the duplicated HTTPS call to be seen\n%s", len(result.Findings), result)
	}

	if !strings.Contains(result.String(), "guard.example.test/claimed") {
		t.Errorf("report did not name the TLS dependency:\n%s", result)
	}
}

// TestUntrustedTLSDependencyFailsLoudly is the false-positive guard the spec demands.
//
// A handshake the service rejects — certificate pinning, an embedded certificate pool — was being
// recorded as an effect, so duplicate delivery reported a divergence made entirely of connection
// errors. It has to stop the run instead: a finding built out of a broken connection is worse than
// no finding at all.
func TestUntrustedTLSDependencyFailsLoudly(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	seq, err := store.Publish(
		t.Context(),
		toy.SubjectOrderCreated,
		orderPayload("ORD-PINNED", "WIDGET-PINNED"),
	)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	config := policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{filterAll},
		AckWait:        time.Second,
		MaxDeliver:     6,
		MaxAckPending:  1,
	}

	result, err := check.Run(t.Context(), tlsSandbox(t, store, config, key, trustNothing), check.Options{
		Messages: []uint64{seq},
		Consumer: "reserve_stock_pinned",
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if result.Setup == nil {
		t.Fatalf("a rejected handshake did not stop the run:\n%s", result)
	}

	if len(result.Findings) != 0 {
		t.Errorf("a rejected handshake produced %d findings:\n%s", len(result.Findings), result)
	}

	const setupExit = 3
	if got := result.ExitCode(); got != setupExit {
		t.Errorf("ExitCode() = %d, want %d for a setup failure", got, setupExit)
	}
}

// TestOpaqueDependencyIsObservedWithoutADatabase is the whole point of the fallback: a service on
// something other than Postgres used to produce no effects at all. It deliberately needs no
// database, so unlike its siblings it never skips.
func TestOpaqueDependencyIsObservedWithoutADatabase(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	seq, err := store.Publish(
		t.Context(),
		toy.SubjectOrderCreated,
		orderPayload("ORD-OPAQUE", "WIDGET-OPAQUE"),
	)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	config := policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{filterAll},
		AckWait:        time.Second,
		MaxDeliver:     6,
		MaxAckPending:  1,
	}
	dependency := startLineDependency(t)

	built, err := harness.New(harness.Config{
		Corpus:  store,
		Opaque:  map[string]string{"cache": dependency},
		HashKey: key,
		Policy:  config,
		Quiesce: 50 * time.Millisecond,
		Connect: func(ctx context.Context, at harness.Addresses) (harness.Service, error) {
			dialer := net.Dialer{Timeout: time.Second}

			connection, dialErr := dialer.DialContext(ctx, "tcp", at.Opaque["cache"])
			if dialErr != nil {
				return nil, fmt.Errorf("dial the unparsed dependency: %w", dialErr)
			}

			return &opaqueClient{connection: connection, replies: bufio.NewReader(connection)}, nil
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	session := &counting{inner: built}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: []uint64{seq},
		Consumer: "reserve_stock_opaque",
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if session.faults == 0 {
		t.Fatal("no fault was injected, so the finding proves nothing")
	}

	if len(result.Findings) != 1 {
		t.Fatalf("Findings = %d, want 1\n%s", len(result.Findings), result)
	}

	found := result.Findings[0]
	if found.Status != report.StatusWarn {
		t.Errorf("Status = %q, want WARN (reservations: %v)", found.Status, found.Reservations)
	}

	if !strings.Contains(result.String(), "protocol Stutter does not parse") {
		t.Errorf("report did not say the divergence is unactionable:\n%s", result)
	}

	if !strings.Contains(result.String(), "opaque dependency=cache") {
		t.Errorf("report did not name the unparsed dependency:\n%s", result)
	}
}
