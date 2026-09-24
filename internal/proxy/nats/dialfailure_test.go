package nats_test

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

	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// closedPorts hands out this package's closed ports, each once, from [65300, 65400). The range sits
// above the kernel's source-port range, so no socket another test opens can take one before the proxy
// dials it, and the other proxy packages' tests use ranges of their own.
var closedPorts atomic.Uint32

// closedPort is an address nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()

	var config net.ListenConfig

	for offset := closedPorts.Add(1); offset < 100; offset = closedPorts.Add(1) {
		addr := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(65300+offset)).String()

		listener, err := config.Listen(t.Context(), "tcp4", addr)
		if err == nil {
			_ = listener.Close()

			return addr
		}
	}

	t.Fatal("no free port in [65300, 65400)")

	return ""
}

// refusedClient connects to a proxy that is about to refuse it. The bus speaks first, so the client
// sends nothing. The proxy resets the client once its own dial fails, and that reset can land before
// the connect returns: it is then the refusal arriving early, and the client is nil.
func refusedClient(t *testing.T, addr string) net.Conn {
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

	return client
}

// expectRefused requires the client's next read to end in a reset or EOF: the proxy let it go without
// the bus's greeting.
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

// TestUpstreamDialFailureFailsTheRun stops the run when the proxy cannot reach the bus. The bus speaks
// first, so the client sends nothing: the dial happens on accept, and its failure alone must end Serve.
func TestUpstreamDialFailureFailsTheRun(t *testing.T) {
	t.Parallel()

	t.Run("closed-port", func(t *testing.T) {
		t.Parallel()

		upstream := closedPort(t)
		observations := &recorder{}

		proxy, err := natsproxy.Listen(t.Context(), "127.0.0.1:0", upstream, observations)
		if err != nil {
			t.Fatalf("Listen() error = %v", err)
		}

		t.Cleanup(func() { _ = proxy.Close() })

		served := make(chan error, 1)

		go func() { served <- proxy.Serve(t.Context()) }()

		client := refusedClient(t, proxy.Addr())

		select {
		case serveErr := <-served:
			if serveErr == nil || !strings.Contains(serveErr.Error(), upstream) {
				t.Errorf("Serve() = %v, want an error naming %s", serveErr, upstream)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Serve did not return within 2s after the upstream dial failed")
		}

		observations.mu.Lock()
		count := len(observations.entries)
		observations.mu.Unlock()

		if count != 0 {
			t.Errorf("the sink holds %d observations, want 0", count)
		}

		expectRefused(t, client)
	})

	t.Run("cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())

		proxy, err := natsproxy.Listen(ctx, "127.0.0.1:0", closedPort(t), &recorder{})
		if err != nil {
			t.Fatalf("Listen() error = %v", err)
		}

		served := make(chan error, 1)

		go func() { served <- proxy.Serve(ctx) }()

		cancel()

		// The proxy lets the client go once its dial is abandoned; the read waits for that.
		expectRefused(t, refusedClient(t, proxy.Addr()))

		if err := proxy.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}

		if serveErr := <-served; serveErr != nil {
			t.Errorf("Serve() = %v after its context was cancelled, want nil", serveErr)
		}
	})
}
