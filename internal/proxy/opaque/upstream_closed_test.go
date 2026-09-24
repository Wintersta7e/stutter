package opaque_test

import (
	"errors"
	"io"
	"net"
	"slices"
	"syscall"
	"testing"
	"time"

	opaqueproxy "github.com/Wintersta7e/stutter/internal/proxy/opaque"
)

// TestAnUpstreamClosedBeforeAnyByteIsADialFailure stops the run when the dependency ends a connection
// within the dial hold without sending a byte: that is the engine's forwarder accepting for a port
// nothing serves, and handing the service an empty connection would be a refusal Stutter never sees.
func TestAnUpstreamClosedBeforeAnyByteIsADialFailure(t *testing.T) {
	t.Parallel()

	for _, hangUp := range []struct {
		upstream func(conn net.Conn)
		name     string
	}{
		{name: "closes at once, client first", upstream: func(net.Conn) {}},
		// The reset waits for the request: one before the dial completes fails the dial itself, which
		// the refused-dial rule already covers.
		{name: "resets before answering", upstream: func(conn net.Conn) {
			_, _ = conn.Read(make([]byte, 64)) //nolint:errcheck // the reset follows either way.
			resetConn(tcp(t, conn))
		}},
	} {
		t.Run(hangUp.name, func(t *testing.T) {
			t.Parallel()

			current := &sink{}
			proxy, done := startProxy(t, startUpstream(t, hangUp.upstream), current)
			t.Cleanup(func() { _ = proxy.Close() })

			// The proxy may see the dependency hang up, and reset the client, before the request is sent —
			// or, on a loaded host, before the dial reads back its own connect. Either is the same refusal.
			dialer := net.Dialer{Timeout: time.Second}

			client, err := dialer.DialContext(t.Context(), "tcp", proxy.Addr())
			if err != nil && !errors.Is(err, syscall.ECONNRESET) {
				t.Fatal(err)
			}

			if client != nil {
				defer func() { _ = client.Close() }()

				_, err := io.WriteString(client, "GET counter\n")
				if err != nil && !errors.Is(err, syscall.ECONNRESET) {
					t.Fatal(err)
				}
			}

			select {
			case serveErr := <-done:
				if !errors.Is(serveErr, opaqueproxy.ErrUpstreamClosed) {
					t.Errorf("Serve() = %v, want ErrUpstreamClosed", serveErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Serve() = <still running>, want ErrUpstreamClosed within 2s")
			}

			if client != nil {
				if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}

				if reply, _ := io.ReadAll(client); len(reply) != 0 { //nolint:errcheck // the bytes are the check.
					t.Errorf("client received %q, want no reply", reply)
				}
			}

			if texts := current.texts(); len(texts) != 0 {
				t.Errorf("effects = %q, want none: a request that never reached a dependency is no effect", texts)
			}
		})
	}

	t.Run("client finished first", func(t *testing.T) {
		t.Parallel()

		current := &sink{}
		proxy, done := startProxy(t, startUpstream(t, drain), current)

		client := dial(t, proxy.Addr())
		if _, err := io.WriteString(client, "METRIC stock.written 1\n"); err != nil {
			t.Fatal(err)
		}

		_ = client.Close()

		waitForEffects(t, current, 1)
		closeProxy(t, proxy, done)
	})

	t.Run("silent past the hold", func(t *testing.T) {
		t.Parallel()

		current := &sink{}
		proxy, done := startProxy(t, startUpstream(t, func(conn net.Conn) {
			_, _ = conn.Read(make([]byte, 64)) //nolint:errcheck // the close follows either way.

			time.Sleep(2 * opaqueproxy.DialHold)
		}), current)

		client := dial(t, proxy.Addr())
		if _, err := io.WriteString(client, "PUBLISH tick\n"); err != nil {
			t.Fatal(err)
		}

		if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}

		// The dependency's close past the hold arrives as the end of its side, nothing more.
		if _, err := io.ReadAll(client); err != nil {
			t.Fatalf("client read = %v, want EOF", err)
		}

		_ = client.Close()

		waitForEffects(t, current, 1)
		closeProxy(t, proxy, done)

		if got, want := current.texts(), []string{"dependency=cache payload=PUBLISH tick"}; !slices.Equal(got, want) {
			t.Errorf("effects = %q, want %q", got, want)
		}
	})

	t.Run("greets then closes", func(t *testing.T) {
		t.Parallel()

		current := &sink{}
		proxy, done := startProxy(t, startUpstream(t, func(conn net.Conn) {
			_, _ = io.WriteString(conn, "HELLO\n") //nolint:errcheck // the close follows either way.
		}), current)

		client := dial(t, proxy.Addr())

		if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}

		if greeting, err := io.ReadAll(client); err != nil || string(greeting) != "HELLO\n" {
			t.Fatalf("client read %q, %v; want the greeting, then EOF", greeting, err)
		}

		_ = client.Close()

		closeProxy(t, proxy, done)

		if texts := current.texts(); len(texts) != 0 {
			t.Errorf("effects = %q, want none", texts)
		}
	})
}
