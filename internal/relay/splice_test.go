package relay_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"

	"github.com/Wintersta7e/stutter/internal/relay"
)

// spliced is a client and a server with Splice between them, every leg a real loopback TCP connection.
type spliced struct {
	client *net.TCPConn
	server *net.TCPConn
}

func splice(t *testing.T) spliced {
	t.Helper()

	near, far := tcpPair(t), tcpPair(t)
	done := make(chan struct{})

	go func() {
		defer close(done)

		relay.Splice(near.accepted, far.dialled)
	}()

	t.Cleanup(func() {
		_ = near.dialled.Close()
		_ = far.accepted.Close()

		<-done
	})

	bound(t, near.dialled)
	bound(t, far.accepted)

	return spliced{client: near.dialled, server: far.accepted}
}

// abort closes a connection so that its peer reads a reset, never EOF.
func abort(t *testing.T, conn *net.TCPConn) {
	t.Helper()

	if err := conn.SetLinger(0); err != nil {
		t.Fatalf("SetLinger(0) error = %v", err)
	}

	_ = conn.Close()
}

func send(t *testing.T, conn *net.TCPConn, text string) {
	t.Helper()

	if _, err := io.WriteString(conn, text); err != nil {
		t.Fatalf("write %q: %v", text, err)
	}
}

func closeWrite(t *testing.T, conn *net.TCPConn) {
	t.Helper()

	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite() error = %v", err)
	}
}

// drain reads until the peer's EOF. A read error is logged rather than fatal: what arrived before it
// is what the test judges.
func drain(t *testing.T, conn net.Conn) string {
	t.Helper()

	read, err := io.ReadAll(conn)
	if err != nil {
		t.Logf("read ended with %v", err)
	}

	return string(read)
}

func TestSplice(t *testing.T) {
	t.Parallel()

	t.Run("client-half-close-gets-the-reply", func(t *testing.T) {
		t.Parallel()

		pair := splice(t)

		send(t, pair.client, "ping")
		closeWrite(t, pair.client)

		if request := drain(t, pair.server); request != "ping" {
			t.Fatalf("server read = %q, want %q", request, "ping")
		}

		send(t, pair.server, "pong")
		_ = pair.server.Close()

		if reply := drain(t, pair.client); reply != "pong" {
			t.Errorf("reply = %q, want %q", reply, "pong")
		}
	})

	t.Run("server-half-close-keeps-the-client-writing", func(t *testing.T) {
		t.Parallel()

		pair := splice(t)

		send(t, pair.server, hello)
		closeWrite(t, pair.server)

		if greeting := drain(t, pair.client); greeting != hello {
			t.Fatalf("client read = %q, want %q", greeting, hello)
		}

		send(t, pair.client, "more")
		closeWrite(t, pair.client)

		if rest := drain(t, pair.server); rest != "more" {
			t.Errorf("server read after its own half-close = %q, want %q", rest, "more")
		}
	})

	t.Run("reset-crosses-as-reset", func(t *testing.T) {
		t.Parallel()

		for _, direction := range []string{"client aborts", "server aborts"} {
			pair := splice(t)
			aborting, peer := pair.client, pair.server

			if direction == "server aborts" {
				aborting, peer = pair.server, pair.client
			}

			abort(t, aborting)

			_, err := peer.Read(make([]byte, 1))
			if !errors.Is(err, syscall.ECONNRESET) {
				t.Errorf("%s: read err = %v, want ECONNRESET", direction, err)
			}
		}
	})

	t.Run("mebibyte-each-way", func(t *testing.T) {
		t.Parallel()

		pair := splice(t)
		up, down := randomBytes(t), randomBytes(t)

		received, err := exchange(pair, up, down)
		if err != nil {
			t.Fatalf("exchange: %v", err)
		}

		if !bytes.Equal(received.byServer, up) {
			t.Errorf("server received %d bytes, not the client's 1 MiB", len(received.byServer))
		}

		if !bytes.Equal(received.byClient, down) {
			t.Errorf("client received %d bytes, not the server's 1 MiB", len(received.byClient))
		}
	})
}

// exchanged is what each end of a splice read in an exchange.
type exchanged struct {
	byClient []byte
	byServer []byte
}

// exchange sends up from the client and down from the server at once, each followed by a half-close,
// and reads both ends to EOF.
func exchange(pair spliced, up, down []byte) (exchanged, error) {
	var (
		received exchanged
		errs     [4]error
		wg       sync.WaitGroup
	)

	wg.Go(func() {
		_, errs[0] = pair.client.Write(up)
		errs[0] = errors.Join(errs[0], pair.client.CloseWrite())
	})
	wg.Go(func() {
		_, errs[1] = pair.server.Write(down)
		errs[1] = errors.Join(errs[1], pair.server.CloseWrite())
	})
	wg.Go(func() { received.byClient, errs[2] = io.ReadAll(pair.client) })
	wg.Go(func() { received.byServer, errs[3] = io.ReadAll(pair.server) })
	wg.Wait()

	return received, errors.Join(errs[:]...)
}

func randomBytes(t *testing.T) []byte {
	t.Helper()

	buf := make([]byte, 1<<20)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand: %v", err)
	}

	return buf
}
