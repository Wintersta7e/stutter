package pg_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// script answers one accepted connection; accept is its 1-based ordinal.
type script func(conn net.Conn, accept int)

// listen opens a loopback listener on a free port.
func listen(t *testing.T) net.Listener {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	return listener
}

// serve runs a scripted server on a loopback port until the test ends, counting accepts.
func serve(t *testing.T, handle script) (string, *atomic.Int32) {
	t.Helper()

	listener := listen(t)

	var (
		accepts  atomic.Int32
		handlers sync.WaitGroup
	)

	acceptDone := make(chan struct{})

	go func() {
		defer close(acceptDone)

		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			accept := int(accepts.Add(1))

			handlers.Go(func() {
				defer conn.Close()

				handle(conn, accept)
			})
		}
	}()

	t.Cleanup(func() {
		_ = listener.Close()

		<-acceptDone
		handlers.Wait()
	})

	return listener.Addr().String(), &accepts
}

// drain reads whatever arrives until the client closes.
func drain(conn net.Conn) {
	buffer := make([]byte, 64)

	for {
		if _, err := conn.Read(buffer); err != nil {
			return
		}
	}
}

// reply reads the 8-byte request, writes answer, and holds the connection until the client closes.
func reply(answer string) script {
	return func(conn net.Conn, _ int) {
		if _, err := io.ReadFull(conn, make([]byte, 8)); err != nil {
			return
		}

		if _, err := conn.Write([]byte(answer)); err != nil {
			return
		}

		drain(conn)
	}
}

// greet writes first, before reading anything, as a server-first protocol does.
func greet(greeting string) script {
	return func(conn net.Conn, _ int) {
		if _, err := conn.Write([]byte(greeting)); err != nil {
			return
		}

		drain(conn)
	}
}

// silent reads whatever arrives and never answers.
func silent(conn net.Conn, _ int) {
	drain(conn)
}

// hangUp closes every connection without a byte, as a forwarder does while nothing listens behind it.
func hangUp(net.Conn, int) {}

func TestHandshakeAnswerTable(t *testing.T) {
	t.Parallel()

	const (
		wait    = 5 * time.Second
		silence = time.Second
	)

	rows := []struct {
		script     script
		name       string
		want       pg.Answer
		wait       time.Duration
		minElapsed time.Duration
		maxElapsed time.Duration
		minAccepts int32
		closed     bool
	}{
		{name: "1 answers N", script: reply("N"), want: pg.AnswerPostgres, wait: wait},
		{name: "2 answers S", script: reply("S"), want: pg.AnswerPostgres, wait: wait},
		{name: "3 greets first", script: greet("INFO {\"server_id\":\"x\"}\r\n"), want: pg.AnswerOther, wait: wait},
		{name: "4 answers NOPE in one write", script: reply("NOPE"), want: pg.AnswerOther, wait: wait},
		{
			name: "5 stays silent", script: silent, want: pg.AnswerOther, wait: wait,
			minElapsed: silence, maxElapsed: 3 * time.Second,
		},
		{
			name: "6 closes three times then answers N",
			script: func(conn net.Conn, accept int) {
				if accept > 3 {
					reply("N")(conn, accept)
				}
			},
			want: pg.AnswerPostgres, wait: wait, minAccepts: 4,
		},
		{name: "7 always closes", script: hangUp, want: pg.AnswerNone, wait: 500 * time.Millisecond},
		{name: "8 closed port", want: pg.AnswerNone, wait: 300 * time.Millisecond, closed: true},
	}

	ran := 0

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			var (
				addr    string
				accepts *atomic.Int32
			)

			if row.closed {
				listener := listen(t)
				addr = listener.Addr().String()
				_ = listener.Close()
			} else {
				addr, accepts = serve(t, row.script)
			}

			ctx, cancel := context.WithTimeout(t.Context(), row.wait)
			defer cancel()

			start := time.Now()
			got, err := pg.Handshake(ctx, addr)
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("Handshake: %v", err)
			}

			if got != row.want {
				t.Errorf("answer %q, want %q (after %v)", got, row.want, elapsed)
			}

			if row.minElapsed > 0 && (elapsed < row.minElapsed || elapsed >= row.maxElapsed) {
				t.Errorf("answered after %v, want [%v, %v)", elapsed, row.minElapsed, row.maxElapsed)
			}

			if row.minAccepts > 0 && accepts.Load() < row.minAccepts {
				t.Errorf("%d accepts, want at least %d", accepts.Load(), row.minAccepts)
			}
		})

		ran++
	}

	t.Logf("rows=%d", ran)

	if ran != len(rows) || ran == 0 {
		t.Fatalf("rows=%d, want %d", ran, len(rows))
	}
}

func TestHandshakeRefusesAnUnusableAddress(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{"", "no-port", "db:notaport", "db:70000"} {
		if _, err := pg.Handshake(t.Context(), addr); err == nil {
			t.Errorf("Handshake(%q) = nil error, want the address refused", addr)
		} else if errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Handshake(%q) waited for its context: %v", addr, err)
		}
	}
}
