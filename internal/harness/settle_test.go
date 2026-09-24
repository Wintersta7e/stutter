package harness_test

import (
	"context"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// pollSlack is the watch loop's tick: a run's end is noticed on the tick after it happens.
const pollSlack = 25 * time.Millisecond

// settleMargin is how many quiesces of quiet end a run whose messages are all done.
const settleMargin = 5

// timedSandbox builds a sandbox around a pulling service that notes its deliveries, its last
// acknowledgement and its close on a timeline.
func timedSandbox(
	t *testing.T,
	config policy.Config,
	behaviour quirks,
	orders ...string,
) (*harness.Sandbox, []uint64, *timeline) {
	t.Helper()

	timings := &timeline{}
	behaviour.timeline = timings

	built, recorded := quirkySandbox(t, config, behaviour, nil, orders...)

	return built, recorded, timings
}

// TestAPoisonMessageStopsAtTheDeliveryCap: a handler that refuses a message every time, under a consumer
// that never gives up, was measured redelivered tens of thousands of times a second — its run could
// never end. The cap ends it after four deliveries, and the message is recorded as exhausted.
func TestAPoisonMessageStopsAtTheDeliveryCap(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	config.MaxDeliver = -1

	built, _, timings := timedSandbox(t, config, quirks{nakAll: true}, "ORD-POISON-1")

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	result, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	seen := timings.read().deliveries
	t.Logf("the service was handed the message %d times; Exhausted = %d", seen, result.Exhausted)

	if seen != harness.DeliveryCap {
		t.Errorf("the service was handed the message %d times, want the cap of %d", seen, harness.DeliveryCap)
	}

	if result.Exhausted != 1 {
		t.Errorf("Exhausted = %d, want 1", result.Exhausted)
	}
}

// TestACleanRunOnALongCurveEndsOnSettlement: a run that waited for silence had to outlast the longest
// redelivery its curve allowed — ten minutes on this one — after every message had long been settled.
// Settled, it ends once the service has been quiet for the settle period. A fault on the same curve
// still waits for its redelivery, however long that is.
func TestACleanRunOnALongCurveEndsOnSettlement(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	config.BackOff = []time.Duration{
		time.Second, 5 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute,
	}
	config.AckWait = config.BackOff[0]
	config.MaxDeliver = -1

	built, recorded, timings := timedSandbox(t, config, quirks{}, "ORD-CURVE-1", "ORD-CURVE-2")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	if _, err := built.Run(ctx, "clean-1", replay.Clean{}, nil); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	lastAck, closed := timings.read().lastAck, timings.read().closed
	settle := settleMargin * toy.DefaultQuiesce
	bound := settle + pollSlack + 50*time.Millisecond

	t.Logf("closed %s after the last acknowledgement, within %s (settle %s, one tick %s, 50ms for the "+
		"after-run read)", closed.Sub(lastAck), bound, settle, pollSlack)

	if closed.Sub(lastAck) > bound {
		t.Errorf("closed %s after the last acknowledgement, want within %s", closed.Sub(lastAck), bound)
	}

	// The last message: with one in flight, an earlier one's redelivery holds every later message back,
	// and the run would wait for it even if a withheld acknowledgement were read as settling anything.
	last := recorded[len(recorded)-1]

	faulted, err := built.Run(ctx, "duplicate-2", replay.Duplicate{Seq: last}, nil)
	if err != nil {
		t.Fatalf("Run(duplicate) error = %v", err)
	}

	t.Logf("the duplicate run delivered %d messages", faulted.Delivered)

	if faulted.Delivered != len(recorded)+1 {
		t.Errorf("the duplicate run delivered %d messages, want %d — it ended before the redelivery",
			faulted.Delivered, len(recorded)+1)
	}
}

// slowCurve is a consumer whose owed-silence limit is well past its first deadline: a 300ms first
// attempt, a second-long one after it. With the fixture's quiesce the limit is 2 × 1s + 50ms, where
// twice the first deadline is 650ms. The first entry is no shorter because the Fill hold's bound is a
// tenth of it, and 10ms was measured too short to publish three messages on a loaded host.
func slowCurve() policy.Config {
	config := observedConfig()
	config.BackOff = []time.Duration{300 * time.Millisecond, time.Second}
	config.AckWait = config.BackOff[0]
	config.MaxDeliver = -1

	return config
}

// slowCurveLimit is slowCurve's owed-silence limit.
const slowCurveLimit = 2*time.Second + toy.DefaultQuiesce

// TestASlowFetcherIsWaitedFor: a service that pauses between fetches for longer than the old drain's
// silence, but well inside the limit, has every message still owed to it waited for.
func TestASlowFetcherIsWaitedFor(t *testing.T) {
	t.Parallel()

	built, recorded, _ := timedSandbox(t, slowCurve(), quirks{pause: time.Second},
		"ORD-SLOW-1", "ORD-SLOW-2", "ORD-SLOW-3")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	result, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	t.Logf("delivered %d of %d, owed %d", result.Delivered, len(recorded), result.Owed)

	if result.Delivered != len(recorded) || result.Owed != 0 {
		t.Errorf("delivered %d of %d with %d owed, want every message delivered", result.Delivered,
			len(recorded), result.Owed)
	}
}

// TestAConsumerThatStopsLeavesItsMessagesOwed: a service that stops consuming part way leaves the rest
// owed, and the run waits the owed-silence limit for them before ending — the same cut in every run —
// and says how many were still owed.
func TestAConsumerThatStopsLeavesItsMessagesOwed(t *testing.T) {
	t.Parallel()

	built, _, timings := timedSandbox(t, slowCurve(), quirks{stopAfter: 1},
		"ORD-STOP-1", "ORD-STOP-2", "ORD-STOP-3")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	result, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	lastAck, closed := timings.read().lastAck, timings.read().closed
	t.Logf("closed %s after the last acknowledgement (limit %s); owed %d", closed.Sub(lastAck),
		slowCurveLimit, result.Owed)

	if closed.Sub(lastAck) < slowCurveLimit {
		t.Errorf("the run ended %s after the service fell silent, before the limit of %s",
			closed.Sub(lastAck), slowCurveLimit)
	}

	if result.Owed != 2 {
		t.Errorf("Owed = %d, want 2", result.Owed)
	}
}

// TestANakDelayLengthensTheOwedLimit: a NAK asking for redelivery later than the owed-silence limit
// would have the run give up before the redelivery it asked for. The limit stretches to cover it.
func TestANakDelayLengthensTheOwedLimit(t *testing.T) {
	t.Parallel()

	// The static limit is 2 × 200ms + 50ms; the NAK asks for a second.
	config := observedConfig()
	config.AckWait = 200 * time.Millisecond

	built, _ := quirkySandbox(t, config, quirks{nakFor: time.Second}, nil, "ORD-NAK-1")

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	result, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	t.Logf("delivered %d, owed %d", result.Delivered, result.Owed)

	if result.Delivered != 2 || result.Owed != 0 {
		t.Errorf("delivered %d with %d owed, want the redelivery the NAK asked for and nothing owed",
			result.Delivered, result.Owed)
	}
}

// encryptedStart writes the opening bytes of a TLS handshake to the cleartext HTTP stub and hangs up,
// as a client configured for TLS does: the stub cannot read it, and stops.
func encryptedStart(ctx context.Context, t *testing.T, stub string) {
	t.Helper()

	address, err := url.Parse(stub)
	if err != nil {
		t.Errorf("parse the stub address: %v", err)

		return
	}

	dialer := net.Dialer{Timeout: time.Second}

	conn, err := dialer.DialContext(ctx, "tcp", address.Host)
	if err != nil {
		t.Errorf("dial the stub: %v", err)

		return
	}

	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x00}); err != nil {
		t.Errorf("start a TLS handshake: %v", err)
	}
}

// TestTheRunEndsAtOnceOnAProxyError: a proxy that stops mid-run leaves the service's traffic unobserved,
// and waiting out the owed-silence limit before reading its error only reports the run late — as a
// service that stopped. The run ends the moment the proxy does, with its error.
func TestTheRunEndsAtOnceOnAProxyError(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	// Long enough that the owed-silence limit is ten seconds or more.
	config.AckWait = 5 * time.Second

	var stopped time.Time

	built, _ := quirkySandbox(t, config, quirks{}, func(settings *harness.Config) {
		settings.Start = func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			stop := func() {
				stopped = time.Now()

				encryptedStart(ctx, t, at.HTTP)
			}

			service, err := startPulling(ctx, at, config, quirks{stopAfter: 1, onStop: stop})
			if err != nil {
				return nil, err
			}

			return service, nil
		}
	}, "ORD-PROXY-1", "ORD-PROXY-2")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)

	elapsed := time.Since(stopped)
	t.Logf("the run returned %s after the stub was sent TLS: %v", elapsed, err)

	if err == nil || !strings.Contains(err.Error(), "used TLS") {
		t.Fatalf("Run() error = %v, want the stub's own error", err)
	}

	if elapsed > 2*time.Second {
		t.Errorf("the run returned %s after the proxy stopped, want within 2s", elapsed)
	}
}

// TestAProxyErrorDuringStartupEndsTheWait: the startup wait ends on a proxy's error as the run's end
// does, instead of sitting out the startup limit with the service's traffic unobserved.
func TestAProxyErrorDuringStartupEndsTheWait(t *testing.T) {
	t.Parallel()

	const startup = 5 * time.Second

	built, _ := quirkySandbox(t, observedConfig(), quirks{}, func(settings *harness.Config) {
		settings.Startup = startup
		settings.Start = func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			encryptedStart(ctx, t, at.HTTP)

			return absent{}, nil
		}
	}, "ORD-PROXY-1")

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	began := time.Now()

	_, err := built.Run(ctx, "clean-1", replay.Clean{}, nil)

	elapsed := time.Since(began)
	t.Logf("the run returned after %s (startup limit %s): %v", elapsed, startup, err)

	if err == nil || !strings.Contains(err.Error(), "used TLS") {
		t.Fatalf("Run() error = %v, want the stub's own error", err)
	}

	if elapsed >= startup {
		t.Errorf("the run returned after %s, want before the startup limit of %s", elapsed, startup)
	}
}
