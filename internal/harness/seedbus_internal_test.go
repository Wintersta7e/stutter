package harness

import (
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// TestTheSeedBusRoutesByPort gives jobs the bus without recording them: a client port reaches the
// bus's client address, the monitoring port its monitoring address, and anything else is a stranger.
func TestTheSeedBusRoutesByPort(t *testing.T) {
	t.Parallel()

	cfg := testListenerConfig(t)
	set := openTestSet(t, cfg)

	client := startLineServer(t, "client\n")
	monitor := startLineServer(t, "monitor\n")

	port, err := set.OpenSeedBus(t.Context(), client.address, monitor.address)
	if err != nil {
		t.Fatalf("OpenSeedBus() error = %v", err)
	}

	if listed, open := set.Port(KeySeedBus); !open || listed != port {
		t.Errorf("Port(seed-bus) = %d, %v; want %d", listed, open, port)
	}

	for destination, want := range map[uint16]string{cfg.Bus[0]: "client\n", MonitorPort: "monitor\n"} {
		conn := dialKey(t, set, KeySeedBus, cfg.Token, destination)

		if _, err := io.WriteString(conn, "ask\n"); err != nil {
			t.Fatalf("write: %v", err)
		}

		if err := conn.SetReadDeadline(time.Now().Add(setWait)); err != nil {
			t.Fatalf("SetReadDeadline() error = %v", err)
		}

		answer := make([]byte, len(want))
		if _, err := io.ReadFull(conn, answer); err != nil || string(answer) != want {
			t.Errorf("port %d answered %q, %v; want %q", destination, answer, err, want)
		}

		_ = conn.Close()
	}

	dialKey(t, set, KeySeedBus, cfg.Token, 5432)
	awaitCounts(t, set, ListenerCounts{Foreign: 1})

	if err := set.CloseSeedBus(); err != nil {
		t.Fatalf("CloseSeedBus() error = %v", err)
	}

	var dialer net.Dialer

	if conn, err := dialer.DialContext(
		t.Context(),
		"tcp4",
		netip.AddrPortFrom(loopbackAddr, port).String(),
	); err == nil {
		_ = conn.Close()

		t.Error("the seed-bus listener still accepts after CloseSeedBus")
	}
}

// TestASeedBusDialFailureIsNamed reports a bus the jobs could not reach, naming it: a job that failed
// to reach the bus would otherwise read as a job that failed on its own.
func TestASeedBusDialFailureIsNamed(t *testing.T) {
	t.Parallel()

	cfg := testListenerConfig(t)
	set := openTestSet(t, cfg)
	closed := netip.AddrPortFrom(loopbackAddr, freeSpanPort(t))

	if _, err := set.OpenSeedBus(t.Context(), closed, closed); err != nil {
		t.Fatalf("OpenSeedBus() error = %v", err)
	}

	conn := dialKey(t, set, KeySeedBus, cfg.Token, cfg.Bus[0])

	if err := conn.SetReadDeadline(time.Now().Add(setWait)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	_, _ = conn.Read(make([]byte, 1)) //nolint:errcheck // the refusal is judged by CloseSeedBus.

	if err := set.CloseSeedBus(); err == nil || !strings.Contains(err.Error(), closed.String()) {
		t.Errorf("CloseSeedBus() = %v, want an error naming %s", err, closed)
	}
}

// freeSpanPort is a port nothing listens on, above the kernel's source-port range, handed to one test.
func freeSpanPort(t *testing.T) uint16 {
	t.Helper()

	var config net.ListenConfig

	for port := uint16(64900); port < 65000; port++ {
		listener, err := config.Listen(t.Context(), "tcp4", netip.AddrPortFrom(loopbackAddr, port).String())
		if err == nil {
			_ = listener.Close()

			return port
		}
	}

	t.Fatal("no free port in [64900, 65000)")

	return 0
}
