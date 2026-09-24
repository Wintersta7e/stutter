package provision

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/proxy/opaque"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// awaitPoll is the pause between two dials of a port not yet dialable. It bounds the dial rate against
// a forwarder that closes a dead port in well under a millisecond, and is far below any readiness a
// dependency was measured to take.
const awaitPoll = 50 * time.Millisecond

// errNotReady means a Postgres endpoint gave no answer before its wait ended.
var errNotReady = errors.New("the Postgres endpoint did not answer before its wait ended")

// Readiness uses direct connections to containers this check created: fixture setup, never passed
// through a proxy and never recorded.

// dialable reports a port that stays open for the dial hold, or sends a byte within it. A bare connect
// proves nothing: the engine's forwarder accepts for a port nothing serves and closes that connection
// with zero bytes a fraction of a millisecond later. Only a done context is an error.
func dialable(ctx context.Context, addr netip.AddrPort) (bool, error) {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp4", addr.String())
	if err != nil {
		return false, done(ctx)
	}

	defer func() { _ = conn.Close() }()

	// A context that ends during the hold ends the read with it.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if deadlineErr := conn.SetReadDeadline(time.Now().Add(opaque.DialHold)); deadlineErr != nil {
		return false, done(ctx)
	}

	read, err := conn.Read(make([]byte, 1))

	var timeout net.Error

	switch {
	case read > 0:
		return true, nil
	case ctx.Err() != nil:
		return false, done(ctx)
	default:
		return errors.As(err, &timeout) && timeout.Timeout(), nil
	}
}

// done is a context's end as an error, or nil while it lasts.
func done(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("wait ended: %w", err)
	}

	return nil
}

// probePostgres waits, within ctx, for a Postgres endpoint to answer the handshake. An endpoint that
// answers as something else contradicts the classification that made it Postgres.
func probePostgres(ctx context.Context, addr netip.AddrPort) error {
	answer, err := pg.Handshake(ctx, addr.String())
	if err != nil {
		return fmt.Errorf("probe %s: %w", addr, err)
	}

	switch answer {
	case pg.AnswerPostgres:
		return nil
	case pg.AnswerOther:
		return fmt.Errorf("%w: %s answered, but not as Postgres", compose.ErrHandshakeContradiction, addr)
	case pg.AnswerNone:
		return fmt.Errorf("%w: %s", errNotReady, addr)
	default:
		return fmt.Errorf("%w: %s gave the answer %q", errNotReady, addr, answer)
	}
}

// portProbe is one port a readiness wait polls.
type portProbe struct {
	addr netip.AddrPort
	// port is the container port it is reported as.
	port uint16
	// postgres probes it with the handshake instead of a dial.
	postgres bool
}

// awaited is the outcome of a readiness wait: the ports that answered, and the ports that had not when
// it ended. Both are sorted.
type awaited struct {
	ready   []uint16
	pending []uint16
}

// awaitPorts polls every port concurrently until each is ready or the monotonic deadline passes. A
// Postgres port answering as something else ends the wait with that contradiction; a context the
// caller ended is an error; a port still silent at the deadline is pending, not an error.
func awaitPorts(ctx context.Context, deadline time.Time, probes []portProbe) (awaited, error) {
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	errs := make([]error, len(probes))
	ready := make([]bool, len(probes))

	var wg sync.WaitGroup

	for index, probe := range probes {
		wg.Go(func() {
			ready[index], errs[index] = awaitPort(waitCtx, probe)
			if errs[index] != nil {
				cancel()
			}
		})
	}

	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return awaited{}, err
	}

	if err := ctx.Err(); err != nil {
		return awaited{}, fmt.Errorf("await ports: %w", err)
	}

	var out awaited

	for index, probe := range probes {
		if ready[index] {
			out.ready = append(out.ready, probe.port)
		} else {
			out.pending = append(out.pending, probe.port)
		}
	}

	slices.Sort(out.ready)
	slices.Sort(out.pending)

	return out, nil
}

// awaitPort polls one port until it is ready or ctx ends. Only a contradiction is an error.
func awaitPort(ctx context.Context, probe portProbe) (bool, error) {
	if probe.postgres {
		err := probePostgres(ctx, probe.addr)
		if errors.Is(err, compose.ErrHandshakeContradiction) {
			return false, err
		}

		return err == nil, nil
	}

	for {
		// A dial error is the wait ending: the port is then pending, not failed.
		if ok, err := dialable(ctx, probe.addr); ok || err != nil {
			return ok, nil //nolint:nilerr // see above.
		}

		timer := time.NewTimer(awaitPoll)

		select {
		case <-ctx.Done():
			timer.Stop()

			return false, nil
		case <-timer.C:
		}
	}
}
