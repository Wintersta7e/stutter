package opaque_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	opaqueproxy "github.com/Wintersta7e/stutter/internal/proxy/opaque"
)

// closedPorts hands out this package's closed ports, each once, from [65200, 65300). The range sits
// above the kernel's source-port range, so no socket another test opens can take one before the proxy
// dials it, and the other proxy packages' tests use ranges of their own.
var closedPorts atomic.Uint32

// closedPort is an address nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()

	var config net.ListenConfig

	for offset := closedPorts.Add(1); offset < 100; offset = closedPorts.Add(1) {
		addr := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(65200+offset)).String()

		listener, err := config.Listen(t.Context(), "tcp4", addr)
		if err == nil {
			_ = listener.Close()

			return addr
		}
	}

	t.Fatal("no free port in [65200, 65300)")

	return ""
}

// refusedClient connects to a proxy that is about to refuse it and sends first. The proxy resets the
// client once its own dial fails, and that reset can land before the connect or the write returns: it
// is then the refusal arriving early, and the client is nil.
func refusedClient(t *testing.T, addr string, first []byte) net.Conn {
	t.Helper()

	var dialer net.Dialer

	client, err := dialer.DialContext(t.Context(), "tcp4", addr)
	if errors.Is(err, syscall.ECONNRESET) {
		return nil
	}

	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.Write(first); err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("write: %v", err)
	}

	return client
}

// expectRefused requires the client's next read to end in a reset or EOF: the proxy let it go without
// a byte from the dependency.
func expectRefused(t *testing.T, client net.Conn) {
	t.Helper()

	if client == nil {
		return
	}

	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, io.EOF) {
		t.Errorf("client read = %v, want a reset or EOF", err)
	}
}

// TestUpstreamDialFailureFailsTheRun stops the run when the proxy cannot reach its dependency: a proxy
// that reset the client and carried on would report a run in which the service did nothing.
func TestUpstreamDialFailureFailsTheRun(t *testing.T) {
	t.Parallel()

	t.Run("closed-port", func(t *testing.T) {
		t.Parallel()

		upstream := closedPort(t)
		observed := &sink{}

		proxy, err := opaqueproxy.Listen(t.Context(), "127.0.0.1:0", dependencyName, upstream, observed)
		if err != nil {
			t.Fatalf("Listen() error = %v", err)
		}

		t.Cleanup(func() { _ = proxy.Close() })

		served := make(chan error, 1)

		go func() { served <- proxy.Serve(t.Context()) }()

		client := refusedClient(t, proxy.Addr(), []byte("ping"))

		select {
		case serveErr := <-served:
			if serveErr == nil || !strings.Contains(serveErr.Error(), upstream) {
				t.Errorf("Serve() = %v, want an error naming %s", serveErr, upstream)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Serve did not return within 2s after the upstream dial failed")
		}

		if texts := observed.texts(); len(texts) != 0 {
			t.Errorf("the sink holds %d observations, want 0: %q", len(texts), texts)
		}

		expectRefused(t, client)
	})

	t.Run("cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())

		proxy, err := opaqueproxy.Listen(ctx, "127.0.0.1:0", dependencyName, closedPort(t), &sink{})
		if err != nil {
			t.Fatalf("Listen() error = %v", err)
		}

		served := make(chan error, 1)

		go func() { served <- proxy.Serve(ctx) }()

		cancel()

		// The proxy lets the client go once its dial is abandoned; the read waits for that.
		expectRefused(t, refusedClient(t, proxy.Addr(), []byte("ping")))

		if err := proxy.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}

		if serveErr := <-served; serveErr != nil {
			t.Errorf("Serve() = %v after its context was cancelled, want nil", serveErr)
		}
	})
}
