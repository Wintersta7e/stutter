package harness_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/replay"
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
