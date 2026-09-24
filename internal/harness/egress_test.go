package harness_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/harness"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
	"github.com/Wintersta7e/stutter/internal/relay"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// notHTTPStop is the stop the cleartext stub raises on a request it cannot parse.
const notHTTPStop = "egress stop not-http on port 80: not parseable HTTP/1.1"

// sendToStub writes payload to the stub at base and waits for the stub to hang up, so the stop it
// raises has been raised by the time it returns.
func sendToStub(ctx context.Context, t *testing.T, base, payload string) {
	t.Helper()

	address, err := url.Parse(base)
	if err != nil {
		t.Errorf("parse the stub address: %v", err)

		return
	}

	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address.Host)
	if err != nil {
		t.Errorf("dial the stub: %v", err)

		return
	}

	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Errorf("write to the stub: %v", err)

		return
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Errorf("bound the read: %v", err)

		return
	}

	//nolint:errcheck // any end of the connection will do: the stub stops however it hangs up.
	_, _ = io.Copy(io.Discard, conn)
}

// TestAnEgressStopEndsTheRunAsItsLastEffect: a stop on the service's egress ends the run at once, and
// the run carries it — as the text a report quotes and as its last effect, in the stop's own words, so
// a shrink reproduces it — instead of failing the run as a setup error.
func TestAnEgressStopEndsTheRunAsItsLastEffect(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	// Long enough that the owed-silence limit is ten seconds or more.
	config.AckWait = 5 * time.Second

	built, _ := quirkySandbox(t, config, quirks{}, func(settings *harness.Config) {
		settings.Start = func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			var once sync.Once

			garbage := func() {
				once.Do(func() { sendToStub(ctx, t, at.HTTP, "NOT HTTP\r\n\r\n") })
			}

			service, err := startPulling(ctx, at, config, quirks{onHandled: garbage})
			if err != nil {
				return nil, err
			}

			return service, nil
		}
	}, "ORD-STOP-1")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	began := time.Now()
	result, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)
	elapsed := time.Since(began)

	t.Logf("the run returned after %s (drain %s): %v", elapsed, built.Timings().Drain, err)

	if err != nil {
		t.Fatalf("Run() error = %v, want the stop carried by the result", err)
	}

	if result.Stopped != notHTTPStop {
		t.Errorf("Stopped = %q, want %q", result.Stopped, notHTTPStop)
	}

	if len(result.Effects) == 0 {
		t.Fatal("the run recorded no effects, want the stop as its last")
	}

	last := result.Effects[len(result.Effects)-1]
	if last.Kind != effect.KindOpaque || last.Printable != result.Stopped {
		t.Errorf("last effect = %s %q, want %s %q", last.Kind, last.Printable, effect.KindOpaque, result.Stopped)
	}

	if elapsed >= built.Timings().Drain {
		t.Errorf("the run returned after %s, want before the drain of %s", elapsed, built.Timings().Drain)
	}
}

// localIPv4 is the first non-loopback IPv4 address of this host: an address a proxy bound on every
// interface is reachable at, and one no run can predict.
func localIPv4(t *testing.T) string {
	t.Helper()

	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("list the host's addresses: %v", err)
	}

	for _, address := range addresses {
		network, isNetwork := address.(*net.IPNet)
		if isNetwork && !network.IP.IsLoopback() && network.IP.To4() != nil {
			return network.IP.String()
		}
	}

	t.Fatal("precondition: this host has no non-loopback IPv4 address to advertise")

	return ""
}

// TestAdvertisedAddressRendersLogicalHost: a service told to dial the stub at an advertised address
// sends that address as its Host. Rendering it would put a bind and advertise choice — and a
// kernel-assigned port — into the effect, and two runs would never agree.
func TestAdvertisedAddressRendersLogicalHost(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	if _, publishErr := store.Publish(
		t.Context(),
		toy.SubjectOrderCreated,
		orderPayload("ORD-HOST", "W"),
	); publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	built, err := harness.New(harness.Config{
		Corpus:        store,
		BindHost:      "0.0.0.0",
		AdvertiseHost: localIPv4(t),
		HashKey:       key,
		Policy:        observedConfig(),
		Quiesce:       toy.DefaultQuiesce,
		Connect: func(_ context.Context, at harness.Addresses) (harness.Service, error) {
			return &tlsClient{baseURL: at.HTTP, client: &http.Client{Timeout: time.Second}}, nil
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	result, err := built.Run(t.Context(), "clean", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	const want = "GET dependency.invalid/claimed"

	for _, observed := range result.Effects {
		if observed.Kind == effect.KindHTTP {
			if !strings.HasPrefix(observed.Printable, want) {
				t.Errorf("effect = %q, want it to begin %q", observed.Printable, want)
			}

			return
		}
	}

	t.Fatalf("no HTTP effect among %d", len(result.Effects))
}

// TestSandboxTallyIsItsScripts: a sandbox's tally is what its stub answered across its runs, which a
// report lists per external host.
func TestSandboxTallyIsItsScripts(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	if _, publishErr := store.Publish(
		t.Context(),
		toy.SubjectOrderCreated,
		orderPayload("ORD-TALLY", "W"),
	); publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	built, err := harness.New(harness.Config{
		Corpus:     store,
		HTTPRoutes: []httpproxy.Route{{Path: "/claimed", Response: httpproxy.Response{StatusCode: http.StatusOK}}},
		HashKey:    key,
		Policy:     observedConfig(),
		Quiesce:    toy.DefaultQuiesce,
		Connect: func(_ context.Context, at harness.Addresses) (harness.Service, error) {
			return &tlsClient{baseURL: at.HTTP, client: &http.Client{Timeout: time.Second}}, nil
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	if _, err := built.Run(t.Context(), "clean", replay.Clean{}, nil); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := []httpproxy.HostTally{{Host: "dependency.invalid", Calls: 1, Routed: 1}}
	if got := built.Tally(); !reflect.DeepEqual(got, want) {
		t.Errorf("Tally() = %+v, want %+v", got, want)
	}
}

// driven builds a sandbox Stutter dispatches to, over a corpus of one message, with the service connect
// builds and the rest of the configuration tune sets.
func driven(t *testing.T, connect harness.Connect, tune func(*harness.Config)) *harness.Sandbox {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	if _, publishErr := store.Publish(
		t.Context(),
		toy.SubjectOrderCreated,
		orderPayload("ORD-EGRESS", "W"),
	); publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	settings := harness.Config{
		Corpus:  store,
		HashKey: key,
		Policy:  observedConfig(),
		Quiesce: toy.DefaultQuiesce,
		Connect: connect,
	}

	if tune != nil {
		tune(&settings)
	}

	built, err := harness.New(settings)
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built
}

// trusting is a TLS configuration that trusts only the stub's CA.
func trusting(t *testing.T, at harness.Addresses) *tls.Config {
	t.Helper()

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(at.HTTPCACert) {
		t.Error("Addresses.HTTPCACert holds no certificate")
	}

	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}

// namesClient is a service that calls each of its names over HTTPS, through the stub's TLS entry
// whatever the name resolves to, and keeps the leaf each name was presented.
type namesClient struct {
	client *http.Client
	leaves map[string][]byte
	names  []string
	mu     sync.Mutex
}

func (c *namesClient) Handle(ctx context.Context, _ replay.Message) error {
	for index, name := range c.names {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			"https://"+name+"/"+string(rune('a'+index)), nil)
		if err != nil {
			return err
		}

		response, err := c.client.Do(request)
		if err != nil {
			return fmt.Errorf("call %s: %w", name, err)
		}

		_ = response.Body.Close()

		c.mu.Lock()
		c.leaves[name] = response.TLS.PeerCertificates[0].Raw
		c.mu.Unlock()
	}

	return nil
}

func (c *namesClient) Close(context.Context) { c.client.CloseIdleConnections() }

// namesSandbox is a driven sandbox whose service is a namesClient with a clock skewed by skew, and
// the clients each start built.
func namesSandbox(t *testing.T, skew time.Duration, names ...string) (*harness.Sandbox, *[]*namesClient) {
	t.Helper()

	var started []*namesClient

	built := driven(t, func(_ context.Context, at harness.Addresses) (harness.Service, error) {
		stub, err := url.Parse(at.HTTPS)
		if err != nil {
			return nil, err
		}

		config := trusting(t, at)
		config.Time = func() time.Time { return time.Now().Add(skew) }

		client := &namesClient{
			names:  names,
			leaves: make(map[string][]byte),
			client: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
				TLSClientConfig: config,
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, stub.Host)
				},
			}},
		}
		started = append(started, client)

		return client, nil
	}, nil)

	return built, &started
}

// TestEveryServerNameGetsItsOwnLeaf: a service reaches the stub under the names it already dials, so
// every one of them verifies against the one CA it was given, run after run, even on a clock half an
// hour behind.
func TestEveryServerNameGetsItsOwnLeaf(t *testing.T) {
	t.Parallel()

	built, clients := namesSandbox(t, 0, "api.example.test", "other.example.test")

	first, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if _, err := built.Run(t.Context(), "clean-2", replay.Clean{}, nil); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	rendered := make([]string, 0, len(first.Effects))
	for _, observed := range first.Effects {
		rendered = append(rendered, observed.Printable)
	}

	for _, want := range []string{"GET api.example.test/a", "GET other.example.test/b"} {
		if !slices.ContainsFunc(rendered, func(effect string) bool { return strings.HasPrefix(effect, want) }) {
			t.Errorf("effects = %q, want one beginning %q", rendered, want)
		}
	}

	if runs := *clients; len(runs) != 2 ||
		!bytes.Equal(runs[0].leaves["api.example.test"], runs[1].leaves["api.example.test"]) {
		t.Error("two runs were presented different leaves for api.example.test")
	}

	skewed, _ := namesSandbox(t, -30*time.Minute, "api.example.test")
	if _, err := skewed.Run(t.Context(), "skewed", replay.Clean{}, nil); err != nil {
		t.Errorf("a client 30 minutes behind: Run() error = %v", err)
	}
}

// TestANoSNIClientVerifiesTheAdvertisedAddress: a client told to dial an address sends no server
// name and verifies that address, although its connection arrives on the address the stub bound.
func TestANoSNIClientVerifiesTheAdvertisedAddress(t *testing.T) {
	t.Parallel()

	const advertised = "192.0.2.10"

	built := driven(t, func(ctx context.Context, at harness.Addresses) (harness.Service, error) {
		stub, err := url.Parse(at.HTTPS)
		if err != nil {
			return nil, err
		}

		config := trusting(t, at)
		config.ServerName = advertised

		conn, err := (&tls.Dialer{Config: config}).DialContext(ctx, "tcp", net.JoinHostPort(loopbackHost, stub.Port()))
		if err != nil {
			return nil, fmt.Errorf("handshake for %s: %w", advertised, err)
		}

		return &idleTLS{conn: conn}, nil
	}, func(settings *harness.Config) {
		settings.BindHost = loopbackHost
		settings.AdvertiseHost = advertised
	})

	if _, err := built.Run(t.Context(), "clean", replay.Clean{}, nil); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

// loopbackHost is where the stub binds when a test names the interface.
const loopbackHost = "127.0.0.1"

// idleTLS is a service holding a TLS connection to the stub that it never sends a request on, and
// closes when it is closed, taking a moment to shut down afterwards.
type idleTLS struct {
	conn net.Conn
}

func (*idleTLS) Handle(context.Context, replay.Message) error { return nil }

func (s *idleTLS) Close(context.Context) {
	_ = s.conn.Close()

	// Long enough for the stub to judge the close before its own teardown begins.
	time.Sleep(200 * time.Millisecond)
}

// TestAServiceLeavingAnIdleTLSConnectionIsNotStopped: a connection a service opened and never used is
// closed as the service goes away. It hid nothing, and the stub must not read it as a client that
// could not talk to it.
func TestAServiceLeavingAnIdleTLSConnectionIsNotStopped(t *testing.T) {
	t.Parallel()

	built := driven(t, func(ctx context.Context, at harness.Addresses) (harness.Service, error) {
		stub, err := url.Parse(at.HTTPS)
		if err != nil {
			return nil, err
		}

		config := trusting(t, at)
		config.ServerName = "api.example.test"

		conn, err := (&tls.Dialer{Config: config}).DialContext(ctx, "tcp", stub.Host)
		if err != nil {
			return nil, fmt.Errorf("handshake: %w", err)
		}

		return &idleTLS{conn: conn}, nil
	}, nil)

	if _, err := built.Run(t.Context(), "clean", replay.Clean{}, nil); err != nil {
		t.Fatalf("Run() error = %v, want an idle connection closed at teardown to stop nothing", err)
	}
}

// TestHTTPTLSIsGone: the stub always serves TLS, so the switch that once turned it on is gone from the
// tree.
func TestHTTPTLSIsGone(t *testing.T) {
	t.Parallel()

	// Built in pieces, so this file matches only through its own name, which is discounted.
	needle, self := []byte("HTTP"+"TLS"), []byte("Test"+"HTTP"+"TLS"+"IsGone")
	searched, matches := 0, 0

	module, err := os.OpenRoot(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = module.Close() }()

	err = fs.WalkDir(module.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}

		content, readErr := fs.ReadFile(module.FS(), path)
		if readErr != nil {
			return readErr
		}

		searched++

		if bytes.Count(content, needle) > bytes.Count(content, self) {
			matches++

			t.Logf("found in %s", path)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("files searched: %d matches: %d", searched, matches)

	if searched == 0 || matches > 0 {
		t.Errorf("files searched: %d matches: %d, want files searched and no match", searched, matches)
	}
}

// throughRelay is a service's call to an external name, as the stub relay delivers it: a connection to
// the listener set's key, opened with the check's preamble for the port the service dialled, carrying
// request — over TLS when secure is set — and its answer read.
func (r relayedRig) throughRelay(
	ctx context.Context,
	key string,
	port uint16,
	secure *tls.Config,
	request string,
) error {
	listener, open := r.set.Port(key)
	if !open {
		return fmt.Errorf("the set has no %s listener", key)
	}

	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp4",
		netip.AddrPortFrom(localhost, listener).String())
	if err != nil {
		return fmt.Errorf("dial the %s listener: %w", key, err)
	}

	defer func() { _ = conn.Close() }()

	if preambleErr := relay.WritePreamble(conn, r.token, port); preambleErr != nil {
		return fmt.Errorf("write the preamble: %w", preambleErr)
	}

	if secure != nil {
		conn = tls.Client(conn, secure)
	}

	if deadlineErr := conn.SetDeadline(time.Now().Add(5 * time.Second)); deadlineErr != nil {
		return fmt.Errorf("bound the call: %w", deadlineErr)
	}

	if _, writeErr := io.WriteString(conn, request); writeErr != nil {
		return fmt.Errorf("write the request: %w", writeErr)
	}

	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return fmt.Errorf("read the answer: %w", err)
	}

	return response.Body.Close()
}

// stubbedSandbox is a sandbox on the rig's listener set around a pulling service that, while handling
// the first message of each start, runs calls. tune, when set, adjusts the configuration.
func (r relayedRig) stubbedSandbox(
	t *testing.T,
	calls func(ctx context.Context, start int) error,
	tune func(*harness.Config),
) *harness.Sandbox {
	t.Helper()

	key := make([]byte, hashKeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate hash key: %v", err)
	}

	config := observedConfig()
	config.FilterSubjects = []string{toy.SubjectOrderCreated}

	var starts atomic.Int32

	settings := harness.Config{
		Corpus:    r.store,
		Listeners: r.set,
		HashKey:   key,
		Policy:    config,
		Quiesce:   toy.DefaultQuiesce,
		Start: func(ctx context.Context, _ harness.Addresses) (harness.Consumer, error) {
			start := int(starts.Add(1))

			var once sync.Once

			handled := func() {
				once.Do(func() {
					if err := calls(ctx, start); err != nil {
						t.Errorf("start %d: %v", start, err)
					}
				})
			}

			return startPulling(ctx, harness.Addresses{
				Opaque: map[string]string{opaqueCache: r.cacheAt.String()},
				NATS:   "nats://" + r.busAt.String(),
			}, config, quirks{onHandled: handled})
		},
	}

	if tune != nil {
		tune(&settings)
	}

	built, err := harness.New(settings)
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built
}

// shared takes the rig's corpus out of its stream and checkpoints the bus once, so several sandboxes on
// the one corpus restore the same starting point.
func (r relayedRig) shared(t *testing.T) func(*harness.Config) {
	t.Helper()

	recorded, err := r.store.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if clearErr := r.store.Clear(t.Context()); clearErr != nil {
		t.Fatalf("Clear() error = %v", clearErr)
	}

	baseline, err := r.store.Checkpoint(t.Context(), filepath.Join(filepath.Dir(r.store.StoreDir()), "shared"))
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}

	return func(settings *harness.Config) {
		settings.Recorded, settings.Baseline = recorded, &baseline
	}
}

// trustingSet is a TLS configuration that trusts only the set's CA, asking for name.
func trustingSet(t *testing.T, set *harness.ListenerSet, name string) *tls.Config {
	t.Helper()

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(set.CAPEM()) {
		t.Fatal("the set's CA PEM holds no certificate")
	}

	return &tls.Config{RootCAs: roots, ServerName: name, MinVersion: tls.VersionTLS12}
}

// httpEffects are a run's HTTP effects, as rendered.
func httpEffects(result replay.Result) []string {
	var rendered []string

	for _, observed := range result.Effects {
		if observed.Kind == effect.KindHTTP {
			rendered = append(rendered, observed.Printable)
		}
	}

	return rendered
}

// TestTheStubServesTheListenerSetsEntries: a provisioned service reaches the stub through the stub
// relay on 80, 443 and any other port, and the run's stub serves all three from the check's listeners.
func TestTheStubServesTheListenerSetsEntries(t *testing.T) {
	t.Parallel()

	rig := startRig(t)
	secure := trustingSet(t, rig.set, "rates.example.test")

	built := rig.stubbedSandbox(t, func(ctx context.Context, _ int) error {
		return errors.Join(
			rig.throughRelay(ctx, harness.KeyHTTP, harness.HTTPPort, nil,
				"GET /plain HTTP/1.1\r\nHost: 172.18.0.5\r\n\r\n"),
			rig.throughRelay(ctx, harness.KeyHTTPS, harness.HTTPSPort, secure,
				"GET /secure HTTP/1.1\r\nHost: rates.example.test\r\n\r\n"),
			rig.throughRelay(ctx, harness.KeyCatchAll, 8080, nil,
				"GET /other HTTP/1.1\r\nHost: api.example.test:8080\r\n\r\n"),
		)
	}, nil)

	rendered := httpEffects(cleanRun(t, built, "clean"))

	for _, want := range []string{
		"GET dependency.invalid/plain", "GET rates.example.test/secure", "GET api.example.test:8080/other",
	} {
		if !slices.ContainsFunc(rendered, func(effect string) bool { return strings.HasPrefix(effect, want) }) {
			t.Errorf("HTTP effects = %q, want one beginning %q", rendered, want)
		}
	}
}

// TestEveryConsumerCheckPresentsTheChecksCA: the CA file is written once per check, so every consumer
// check's stub presents leaves from the one CA in it.
func TestEveryConsumerCheckPresentsTheChecksCA(t *testing.T) {
	t.Parallel()

	rig := startRig(t)
	secure := trustingSet(t, rig.set, "rates.example.test")
	call := func(ctx context.Context, _ int) error {
		return rig.throughRelay(ctx, harness.KeyHTTPS, harness.HTTPSPort, secure,
			"GET /secure HTTP/1.1\r\nHost: rates.example.test\r\n\r\n")
	}

	shared := rig.shared(t)

	checks := []*harness.Sandbox{rig.stubbedSandbox(t, call, shared), rig.stubbedSandbox(t, call, shared)}

	for check, built := range checks {
		result, err := built.Run(t.Context(), "clean", replay.Clean{}, nil)
		if err != nil || result.Stopped != "" {
			t.Errorf("consumer check %d: Run() = stopped %q, %v; want its stub to present the check's CA",
				check, result.Stopped, err)
		}
	}
}

// TestAStopClosesOnlyTheRunsStub: a stop ends the run it happened in and closes that run's stub, never
// the check's listeners, so the next run is served on the same ports.
func TestAStopClosesOnlyTheRunsStub(t *testing.T) {
	t.Parallel()

	rig := startRig(t)

	built := rig.stubbedSandbox(t, func(ctx context.Context, start int) error {
		if start == 1 {
			//nolint:errcheck // the stub hangs up on a CONNECT; the stop is what the test reads.
			_ = rig.throughRelay(ctx, harness.KeyHTTP, harness.HTTPPort, nil,
				"CONNECT api.example.test:443 HTTP/1.1\r\nHost: api.example.test:443\r\n\r\n")

			return nil
		}

		return rig.throughRelay(ctx, harness.KeyHTTP, harness.HTTPPort, nil,
			"GET /after HTTP/1.1\r\nHost: 172.18.0.5\r\n\r\n")
	}, nil)

	stopped := cleanRun(t, built, "stopped")
	if !strings.Contains(stopped.Stopped, "egress stop connect") {
		t.Fatalf("Stopped = %q, want the CONNECT's stop", stopped.Stopped)
	}

	after := httpEffects(cleanRun(t, built, "after"))
	if len(after) != 1 || !strings.HasPrefix(after[0], "GET dependency.invalid/after") {
		t.Errorf("HTTP effects after the stop = %q, want the one call served", after)
	}
}
