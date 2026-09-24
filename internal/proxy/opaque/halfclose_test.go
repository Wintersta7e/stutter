package opaque_test

import (
	"errors"
	"io"
	"net"
	"slices"
	"syscall"
	"testing"
	"time"
)

// tcp is the TCP connection under a test's net.Conn: half-close and reset need it.
func tcp(t *testing.T, conn net.Conn) *net.TCPConn {
	t.Helper()

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("%T is not a TCP connection", conn)
	}

	return tcpConn
}

// resetConn closes a connection so its peer reads a reset.
func resetConn(conn *net.TCPConn) {
	_ = conn.SetLinger(0) //nolint:errcheck // without it the close is clean, which the test then catches.
	_ = conn.Close()
}

// TestHalfCloseReachesTheUpstreamAndBack sends a request and shuts the write side, as a client that
// waits for the whole reply does: the upstream must see the end of the request and still reach the
// client with its answer.
func TestHalfCloseReachesTheUpstreamAndBack(t *testing.T) {
	t.Parallel()

	current := &sink{}
	upstream := startUpstream(t, func(conn net.Conn) {
		request, err := io.ReadAll(conn)
		if err != nil {
			return
		}

		//nolint:errcheck // the client's read is what the test checks.
		_, _ = io.WriteString(conn, "GOT "+string(request))
	})
	proxy, done := startProxy(t, upstream, current)

	client := tcp(t, dial(t, proxy.Addr()))

	if _, err := io.WriteString(client, "PING\n"); err != nil {
		t.Fatal(err)
	}

	if err := client.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite() error = %v", err)
	}

	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	reply, err := io.ReadAll(client)
	if err != nil || string(reply) != "GOT PING\n" {
		t.Errorf("client got %q, %v; want the whole reply %q", reply, err, "GOT PING\n")
	}

	_ = client.Close()

	closeProxy(t, proxy, done)

	if got, want := current.texts(), []string{"dependency=cache payload=PING"}; !slices.Equal(got, want) {
		t.Errorf("effects = %q, want %q", got, want)
	}
}

// TestUpstreamHalfCloseKeepsTheClientWriting has the dependency shut its write side after a greeting:
// the client reads to the end and can still send.
func TestUpstreamHalfCloseKeepsTheClientWriting(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	upstream := startUpstream(t, func(conn net.Conn) {
		if _, err := io.WriteString(conn, "HELLO\n"); err != nil {
			return
		}

		if err := tcp(t, conn).CloseWrite(); err != nil {
			return
		}

		rest, _ := io.ReadAll(conn) //nolint:errcheck // what arrived is what the test checks.
		received <- string(rest)
	})
	proxy, done := startProxy(t, upstream, &sink{})

	client := dial(t, proxy.Addr())

	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	if greeting, err := io.ReadAll(client); err != nil || string(greeting) != "HELLO\n" {
		t.Fatalf("client read %q, %v; want the greeting, then EOF", greeting, err)
	}

	if _, err := io.WriteString(client, "QUIT\n"); err != nil {
		t.Fatalf("write after the dependency's half-close: %v", err)
	}

	_ = client.Close()

	select {
	case got := <-received:
		if got != "QUIT\n" {
			t.Errorf("the dependency received %q, want %q", got, "QUIT\n")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the dependency received nothing within 2s")
	}

	closeProxy(t, proxy, done)
}

// TestResetIsPropagatedAsReset keeps a reset a reset across the proxy. A clean close in its place tells
// the far end the peer finished, which is not what happened.
func TestResetIsPropagatedAsReset(t *testing.T) {
	t.Parallel()

	t.Run("upstream resets", func(t *testing.T) {
		t.Parallel()

		upstream := startUpstream(t, func(conn net.Conn) {
			if _, err := conn.Read(make([]byte, 64)); err != nil {
				return
			}

			resetConn(tcp(t, conn))
		})
		proxy, done := startProxy(t, upstream, &sink{})

		client := dial(t, proxy.Addr())

		if _, err := io.WriteString(client, "GET counter\n"); err != nil {
			t.Fatal(err)
		}

		if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}

		if _, err := client.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
			t.Errorf("client read = %v, want ECONNRESET", err)
		}

		_ = client.Close()

		closeProxy(t, proxy, done)
	})

	t.Run("client resets", func(t *testing.T) {
		t.Parallel()

		readErr := make(chan error, 1)
		upstream := startUpstream(t, func(conn net.Conn) {
			if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				readErr <- err

				return
			}

			_, err := io.ReadAll(conn)
			readErr <- err
		})
		proxy, done := startProxy(t, upstream, &sink{})

		client := tcp(t, dial(t, proxy.Addr()))

		if _, err := io.WriteString(client, "GET counter\n"); err != nil {
			t.Fatal(err)
		}

		// The request must be through before the reset, or the reset only cuts it short.
		time.Sleep(100 * time.Millisecond)
		resetConn(client)

		select {
		case err := <-readErr:
			if !errors.Is(err, syscall.ECONNRESET) {
				t.Errorf("the dependency's read = %v, want ECONNRESET", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("the dependency's read did not end within 3s")
		}

		closeProxy(t, proxy, done)
	})
}
