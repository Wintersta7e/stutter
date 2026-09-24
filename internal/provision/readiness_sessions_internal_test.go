package provision

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// sslRequestCode is the request code of the 8-byte SSLRequest.
	sslRequestCode = 80877103
	// startingUp is the SQLSTATE of a server refusing every session while it starts.
	startingUp = "57P03"
	// maxStartup bounds the StartupMessage the fake server reads.
	maxStartup = 1024
)

// answerAsPostgres answers one connection as a Postgres server does: an SSLRequest with 'N', and a
// StartupMessage with an ErrorResponse carrying the SQLSTATE refusal returns, or with AuthenticationOk
// when it returns "". refusal is asked only for a StartupMessage.
func answerAsPostgres(conn *net.TCPConn, refusal func() string) {
	head := make([]byte, 8)
	if _, err := io.ReadFull(conn, head); err != nil {
		return
	}

	if binary.BigEndian.Uint32(head[4:]) == sslRequestCode {
		_, _ = conn.Write([]byte{'N'}) //nolint:errcheck // the probe's answer is what the test checks.

		return
	}

	length := binary.BigEndian.Uint32(head[:4])
	if length < 8 || length > maxStartup {
		return
	}

	if _, err := io.ReadFull(conn, make([]byte, length-8)); err != nil {
		return
	}

	answer := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0} // AuthenticationOk.
	if code := refusal(); code != "" {
		answer = errorResponse(code)
	}

	_, _ = conn.Write(answer) //nolint:errcheck // the answer is what the test checks.
}

// errorResponse is a FATAL ErrorResponse carrying code.
func errorResponse(code string) []byte {
	body := []byte("SFATAL\x00C" + code + "\x00Mrefused by the test server\x00\x00")
	message := make([]byte, 5, 5+len(body))
	message[0] = 'E'
	binary.BigEndian.PutUint32(message[1:], uint32(4+len(body))) //nolint:gosec // a few dozen bytes.

	return append(message, body...)
}

// postgresRefusing serves a Postgres that answers its first refusals StartupMessages with code and every
// later one with AuthenticationOk. It counts the StartupMessages.
func postgresRefusing(t *testing.T, refusals int64, code string) (netip.AddrPort, *atomic.Int64) {
	t.Helper()

	var startups atomic.Int64

	addr, _ := serveEach(t, func(_ int, conn *net.TCPConn) {
		answerAsPostgres(conn, func() string {
			if startups.Add(1) <= refusals {
				return code
			}

			return ""
		})
	})

	return addr, &startups
}

// TestTheProbeWaitsUntilPostgresAcceptsSessions is a restored cluster still starting: it answers the
// handshake at once and refuses every session with 57P03 until it is done. A probe that stopped at the
// handshake handed the service a database that refused it — measured on CI's native engine, where a
// restore's first connection read "the database system is starting up".
func TestTheProbeWaitsUntilPostgresAcceptsSessions(t *testing.T) {
	t.Parallel()

	const refusals = 3

	addr, startups := postgresRefusing(t, refusals, startingUp)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := probePostgres(ctx, addr); err != nil {
		t.Fatalf("probePostgres() = %v, want nil", err)
	}

	if got := startups.Load(); got != refusals+1 {
		t.Errorf("the server was sent %d startups, want %d: the probe stopped before a session was accepted",
			got, refusals+1)
	}
}

// TestAPostgresThatRefusesEverySessionIsNotReady keeps refusing: the wait ends not ready, never ready.
func TestAPostgresThatRefusesEverySessionIsNotReady(t *testing.T) {
	t.Parallel()

	addr, startups := postgresRefusing(t, math.MaxInt64, startingUp)

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	if err := probePostgres(ctx, addr); !errors.Is(err, errNotReady) {
		t.Fatalf("probePostgres() = %v, want errNotReady", err)
	}

	t.Logf("startups refused: %d", startups.Load())
}

// TestAnotherStartupErrorIsAServerThatServes is a role the server does not know: a backend it forked
// for the session refused it, so the server is serving sessions.
func TestAnotherStartupErrorIsAServerThatServes(t *testing.T) {
	t.Parallel()

	addr, startups := postgresRefusing(t, math.MaxInt64, "28000")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := probePostgres(ctx, addr); err != nil {
		t.Fatalf("probePostgres() = %v, want nil", err)
	}

	if got := startups.Load(); got != 1 {
		t.Errorf("the server was sent %d startups, want 1", got)
	}
}
