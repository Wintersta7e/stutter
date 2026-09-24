package pg_test

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// fakeDatabase accepts connections and hands each to handle, which owns it.
func fakeDatabase(t *testing.T, handle func(conn *net.TCPConn)) string {
	t.Helper()

	var (
		config net.ListenConfig
		wg     sync.WaitGroup
	)

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the database: %v", err)
	}

	wg.Go(func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			tcp, ok := conn.(*net.TCPConn)
			if !ok {
				_ = conn.Close()

				continue
			}

			wg.Go(func() {
				defer func() { _ = tcp.Close() }()

				handle(tcp)
			})
		}
	})

	t.Cleanup(func() {
		_ = listener.Close()

		wg.Wait()
	})

	return listener.Addr().String()
}

// errorResponse is a minimal backend ErrorResponse: its length, a severity field and the terminator.
func errorResponse() []byte {
	const fields = "SFATAL\x00\x00"

	message := binary.BigEndian.AppendUint32([]byte{'E'}, uint32(len(fields)+4))

	return append(message, fields...)
}

// startupClient connects to the proxy and sends a startup message.
func startupClient(t *testing.T, addr string) net.Conn {
	t.Helper()

	var dialer net.Dialer

	client, err := dialer.DialContext(t.Context(), "tcp4", addr)
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.Write(startup()); err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("write the startup message: %v", err)
	}

	return client
}

// TestAnUpstreamClosedBeforeAnyByteIsADialFailure stops the run when the database ends a connection
// without a byte: a Postgres server always answers a startup message, even with an error, so silence
// then a close is the engine's forwarder accepting for a port nothing serves.
func TestAnUpstreamClosedBeforeAnyByteIsADialFailure(t *testing.T) {
	t.Parallel()

	for _, hangUp := range []struct {
		upstream func(conn *net.TCPConn)
		name     string
	}{
		{name: "closes", upstream: func(*net.TCPConn) {}},
		// The reset waits for the startup message: one before the dial completes fails the dial itself,
		// which the eager-dial rule already covers.
		{name: "resets", upstream: func(conn *net.TCPConn) {
			_, _ = io.ReadFull(conn, make([]byte, len(startup()))) //nolint:errcheck // the reset follows either way.
			_ = conn.SetLinger(0)                                  //nolint:errcheck // without it, a clean close.
		}},
	} {
		t.Run(hangUp.name, func(t *testing.T) {
			t.Parallel()

			sink := &countingSink{}
			running := serveProxy(t, fakeDatabase(t, hangUp.upstream), sink)
			client := startupClient(t, running.proxy.Addr())

			select {
			case serveErr := <-running.done:
				if !errors.Is(serveErr, pg.ErrUpstreamClosed) {
					t.Errorf("Serve() = %v, want ErrUpstreamClosed", serveErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Serve() = <still running>, want ErrUpstreamClosed within 2s")
			}

			if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}

			_, readErr := client.Read(make([]byte, 1))
			if !errors.Is(readErr, syscall.ECONNRESET) && !errors.Is(readErr, io.EOF) {
				t.Errorf("client read = %v, want a reset or EOF: the proxy left the service hanging", readErr)
			}

			if count := sink.records.Load(); count != 0 {
				t.Errorf("the sink holds %d observations, want 0", count)
			}
		})
	}

	t.Run("answers then closes", func(t *testing.T) {
		t.Parallel()

		running := serveProxy(t, fakeDatabase(t, func(conn *net.TCPConn) {
			_, _ = io.ReadFull(conn, make([]byte, len(startup()))) //nolint:errcheck // the reply is the test.
			_, _ = conn.Write(errorResponse())                     //nolint:errcheck // as above.
		}), &countingSink{})

		client := startupClient(t, running.proxy.Addr())

		if _, err := io.ReadFull(client, make([]byte, len(errorResponse()))); err != nil {
			t.Fatalf("read the database's answer: %v", err)
		}

		_ = client.Close()

		if err := closeServed(t, running); err != nil {
			t.Errorf("Serve() = %v, want nil: a database that answered, then closed, is not a dial failure", err)
		}
	})

	t.Run("client leaves first", func(t *testing.T) {
		t.Parallel()

		running := serveProxy(t, fakeDatabase(t, func(conn *net.TCPConn) {
			time.Sleep(200 * time.Millisecond)

			_, _ = conn.Write(errorResponse()) //nolint:errcheck // the proxy may have closed already.
		}), &countingSink{})

		_ = startupClient(t, running.proxy.Addr()).Close()

		time.Sleep(400 * time.Millisecond)

		if err := closeServed(t, running); err != nil {
			t.Errorf("Serve() = %v, want nil: the proxy closing a connection its client left is no failure", err)
		}
	})
}
