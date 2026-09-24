package provision

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// closedPorts hands out this package's closed ports, each once, from [65500, 65535). The range sits
// above the kernel's source-port range, and the other packages' tests use ranges of their own.
var closedPorts atomic.Uint32

// closedPort is an address nothing listens on.
func closedPort(t *testing.T) netip.AddrPort {
	t.Helper()

	var config net.ListenConfig

	for offset := closedPorts.Add(1); offset < 35; offset = closedPorts.Add(1) {
		addr := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(65500+offset))

		listener, err := config.Listen(t.Context(), "tcp4", addr.String())
		if err == nil {
			_ = listener.Close()

			return addr
		}
	}

	t.Fatal("no free port in [65500, 65535)")

	return netip.AddrPort{}
}

// serveEach listens on loopback and hands the nth accepted connection to handle, which owns it.
func serveEach(t *testing.T, handle func(nth int, conn *net.TCPConn)) (netip.AddrPort, *atomic.Int64) {
	t.Helper()

	var (
		config   net.ListenConfig
		accepted atomic.Int64
		wg       sync.WaitGroup
	)

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	wg.Go(func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			nth := int(accepted.Add(1))

			wg.Go(func() {
				tcp, ok := conn.(*net.TCPConn)
				if !ok {
					_ = conn.Close()

					return
				}

				defer func() { _ = tcp.Close() }()

				handle(nth, tcp)
			})
		}
	})

	t.Cleanup(func() {
		_ = listener.Close()

		wg.Wait()
	})

	return netip.MustParseAddrPort(listener.Addr().String()), &accepted
}

// resetTCP closes a connection so its peer reads a reset.
func resetTCP(conn *net.TCPConn) {
	_ = conn.SetLinger(0) //nolint:errcheck // without it the close is clean, still no answer.
	_ = conn.Close()
}

// holdOpen keeps a connection open, sending nothing, until the peer leaves.
func holdOpen(_ int, conn *net.TCPConn) {
	_, _ = io.Copy(io.Discard, conn) //nolint:errcheck // the hold ends when the peer leaves.
}

// TestTheProbeWaitsPastAcceptedResets is the engine's forwarder before Postgres listens: it accepts
// and resets, again and again, and a probe that took a connect for readiness would stop at the first.
func TestTheProbeWaitsPastAcceptedResets(t *testing.T) {
	t.Parallel()

	const resets = 5

	addr, accepted := serveEach(t, func(nth int, conn *net.TCPConn) {
		if nth <= resets {
			resetTCP(conn)

			return
		}

		if _, err := io.ReadFull(conn, make([]byte, 8)); err == nil {
			_, _ = conn.Write([]byte{'N'}) //nolint:errcheck // the probe's answer is what the test checks.
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := probePostgres(ctx, addr); err != nil {
		t.Fatalf("probePostgres() = %v, want nil", err)
	}

	t.Logf("k=%d accepts=%d", resets, accepted.Load())

	if got := accepted.Load(); got != resets+1 {
		t.Errorf("the listener accepted %d connections, want %d: the probe stopped before the answer", got, resets+1)
	}
}

// TestAPostgresEndpointThatAnswersOtherIsAContradiction is a port the model called Postgres answering
// as something else: the classification was wrong, which is a setup failure, never a retry.
func TestAPostgresEndpointThatAnswersOtherIsAContradiction(t *testing.T) {
	t.Parallel()

	addr, _ := serveEach(t, func(_ int, conn *net.TCPConn) {
		_, _ = conn.Write([]byte("+OK\r\n")) //nolint:errcheck // the greeting is what the test checks.
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := probePostgres(ctx, addr)
	if !errors.Is(err, compose.ErrHandshakeContradiction) {
		t.Fatalf("probePostgres() = %v, want ErrHandshakeContradiction", err)
	}

	if !strings.Contains(err.Error(), addr.String()) {
		t.Errorf("probePostgres() = %q, want it to name %s", err, addr)
	}
}

// TestDialableNeedsTheConnectionHeldOrAByte separates a port something serves from the forwarder's
// accept-then-close for one nothing serves.
func TestDialableNeedsTheConnectionHeldOrAByte(t *testing.T) {
	t.Parallel()

	for _, listening := range []struct {
		addr func(t *testing.T) netip.AddrPort
		name string
		want bool
	}{
		{name: "accept then close", want: false, addr: func(t *testing.T) netip.AddrPort {
			t.Helper()

			addr, _ := serveEach(t, func(int, *net.TCPConn) {})

			return addr
		}},
		{name: "accept then reset", want: false, addr: func(t *testing.T) netip.AddrPort {
			t.Helper()

			addr, _ := serveEach(t, func(_ int, conn *net.TCPConn) { resetTCP(conn) })

			return addr
		}},
		{name: "accept and hold", want: true, addr: func(t *testing.T) netip.AddrPort {
			t.Helper()

			addr, _ := serveEach(t, holdOpen)

			return addr
		}},
		{name: "accept and greet", want: true, addr: func(t *testing.T) netip.AddrPort {
			t.Helper()

			addr, _ := serveEach(t, func(nth int, conn *net.TCPConn) {
				_, _ = conn.Write([]byte("HELLO\n")) //nolint:errcheck // the greeting is what the test checks.
				holdOpen(nth, conn)
			})

			return addr
		}},
		{name: "nothing listening", want: false, addr: closedPort},
	} {
		t.Run(listening.name, func(t *testing.T) {
			t.Parallel()

			got, err := dialable(t.Context(), listening.addr(t))
			if err != nil {
				t.Fatalf("dialable() error = %v", err)
			}

			t.Logf("%s: dialable=%v", listening.name, got)

			if got != listening.want {
				t.Errorf("dialable() = %v, want %v", got, listening.want)
			}
		})
	}
}

// TestAwaitPortsReturnsThePortsThatNeverAnswered waits for a port that starts listening late, and
// gives up on one that never does at the deadline, naming it.
func TestAwaitPortsReturnsThePortsThatNeverAnswered(t *testing.T) {
	t.Parallel()

	late := closedPort(t)
	never := closedPort(t)

	go func() {
		time.Sleep(300 * time.Millisecond)

		var config net.ListenConfig

		listener, err := config.Listen(t.Context(), "tcp4", late.String())
		if err != nil {
			return
		}

		go func() {
			<-t.Context().Done()

			_ = listener.Close()
		}()

		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			go func() { _, _ = io.Copy(io.Discard, conn) }() //nolint:errcheck // held until the peer leaves.
		}
	}()

	const wait = 2 * time.Second

	started := time.Now()
	deadline := started.Add(wait)

	got, err := awaitPorts(t.Context(), deadline, []portProbe{
		{addr: late, port: late.Port()},
		{addr: never, port: never.Port()},
	})
	elapsed := time.Since(started)

	t.Logf("elapsed=%v ready=%v pending=%v", elapsed, got.ready, got.pending)

	if err != nil {
		t.Fatalf("awaitPorts() error = %v", err)
	}

	if !slices.Equal(got.ready, []uint16{late.Port()}) || !slices.Equal(got.pending, []uint16{never.Port()}) {
		t.Errorf("ready %v, pending %v; want ready [%d], pending [%d]", got.ready, got.pending, late.Port(),
			never.Port())
	}

	if elapsed > wait+200*time.Millisecond {
		t.Errorf("awaitPorts took %v, past its deadline of %v by more than 200ms", elapsed, wait)
	}
}
