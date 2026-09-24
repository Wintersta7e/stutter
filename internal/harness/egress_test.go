package harness_test

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/harness"
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
