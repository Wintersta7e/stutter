package relay

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// spans hands each catch-all test its own run of ports.
var spans struct {
	next uint16
	mu   sync.Mutex
}

// freeSpan returns the first port of count consecutive ports that are free on 127.0.0.1 right now.
//
// The search runs above the kernel's default source-port range (32768–60999), so no test's outbound
// connection takes a port from under it, and skips any port something on the host already listens on:
// a fixed span collided with an unrelated local listener once. Spans are never handed out twice.
func freeSpan(t *testing.T, count uint16) uint16 {
	t.Helper()

	spans.mu.Lock()
	defer spans.mu.Unlock()

	if spans.next == 0 {
		spans.next = 61000
	}

	for base := spans.next; base < 65000; base++ {
		if spanIsFree(t, base, count) {
			spans.next = base + count

			return base
		}
	}

	t.Fatalf("no %d consecutive free ports above 61000", count)

	return 0
}

func spanIsFree(t *testing.T, base, count uint16) bool {
	t.Helper()

	var config net.ListenConfig

	held := make([]net.Listener, 0, count)
	defer func() {
		for _, listener := range held {
			_ = listener.Close()
		}
	}()

	for port := base; port < base+count; port++ {
		listener, err := config.Listen(t.Context(), "tcp4", loopback(port).String())
		if err != nil {
			return false
		}

		held = append(held, listener)
	}

	return true
}

// portRangeFile writes a stand-in for /proc/sys/net/ipv4/ip_local_port_range, in the kernel's format.
func portRangeFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "ip_local_port_range")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write the port range: %v", err)
	}

	return path
}

func loopback(port uint16) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)
}

// TestTheCatchAllTagsEachConnectionWithItsPort is what lets the host tell one catch-all connection
// from another: each arrives with the port the service dialled, and the ports something else owns —
// a pipe, the kernel's own source ports — are left to their owner.
func TestTheCatchAllTagsEachConnectionWithItsPort(t *testing.T) {
	t.Parallel()

	token := testToken(t)
	pipeHost, catchHost := listenForRelay(t, token), listenForRelay(t, token)

	// A span of 61 ports: a pipe on the 11th, the kernel's source range over the last 11.
	base := freeSpan(t, 61)
	pipePort := base + 10

	sys := system{
		addresses:     fixedAddresses("127.0.0.1"),
		catchAllFirst: base,
		catchAllLast:  base + 60,
		portRangeFile: portRangeFile(t, fmt.Sprintf("%d\t%d\n", base+50, base+60)),
	}

	spec := Spec{
		Bind: netip.MustParsePrefix("127.0.0.1/32"),
		Listeners: []Listener{
			{Upstream: pipeHost.address, Port: pipePort},
			{Upstream: catchHost.address, Kind: CatchAll},
		},
		Token: token,
	}

	run := runSystem(t, sys, spec.Args())
	run.ready(t)

	tagged := 0

	for _, port := range []uint16{base + 1, base + 37, base + 49} {
		if err := dialFrom(t, loopback(port)); err != nil {
			t.Fatalf("dial catch-all port %d: %v", port, err)
		}

		if got := catchHost.next(t).DestinationPort(); got != port {
			t.Errorf("dialled %d, the host read port %d", port, got)
		}

		tagged++
	}

	if err := dialFrom(t, loopback(pipePort)); err != nil {
		t.Fatalf("dial the pipe port: %v", err)
	}

	if got := pipeHost.next(t).DestinationPort(); got != pipePort {
		t.Errorf("the pipe port reached its upstream as port %d, want %d", got, pipePort)
	}

	for _, port := range []uint16{base + 50, base + 60} {
		if err := dialFrom(t, loopback(port)); err == nil {
			t.Errorf("port %d is in the kernel's source range, yet a dial connected", port)
		}
	}

	select {
	case stray := <-catchHost.conns:
		t.Errorf("the catch-all's upstream got a connection for port %d", stray.DestinationPort())
	case <-time.After(100 * time.Millisecond):
	}

	t.Logf("catch-all connections tagged: %d", tagged)
}

// TestTheCatchAllIsOneLoop keeps the catch-all affordable: it binds tens of thousands of sockets, and a
// goroutine per socket would cost a stub relay hundreds of megabytes before any service connects.
//
//nolint:paralleltest // it counts the process's goroutines, which a relay test running beside it moves.
func TestTheCatchAllIsOneLoop(t *testing.T) {
	token := testToken(t)
	host := listenForRelay(t, token)
	base := freeSpan(t, 60)
	last := base + 59

	sys := system{
		addresses:     fixedAddresses("127.0.0.1"),
		catchAllFirst: base,
		catchAllLast:  last,
		portRangeFile: portRangeFile(t, fmt.Sprintf("%d %d\n", last+1, last+10)),
	}

	spec := Spec{
		Bind:      netip.MustParsePrefix("127.0.0.1/32"),
		Listeners: []Listener{{Upstream: host.address, Kind: CatchAll}},
		Token:     token,
	}

	before := runtime.NumGoroutine()

	run := runSystem(t, sys, spec.Args())
	run.ready(t)

	grown := runtime.NumGoroutine() - before
	t.Logf("60 idle catch-all sockets: goroutines grew by %d", grown)

	if grown >= 10 {
		t.Errorf("goroutines grew by %d for 60 idle sockets, want fewer than 10: one loop, not one per socket", grown)
	}

	if err := dialFrom(t, loopback(last)); err != nil {
		t.Fatalf("dial the last catch-all port: %v", err)
	}

	if got := host.next(t).DestinationPort(); got != last {
		t.Errorf("the host read port %d, want %d", got, last)
	}
}

// TestAnUnreadablePortRangeStopsTheRelay refuses to guess which ports the kernel will use for the
// relay's own dials: binding one of them would starve those dials.
func TestAnUnreadablePortRangeStopsTheRelay(t *testing.T) {
	t.Parallel()

	token := testToken(t)
	host := listenForRelay(t, token)

	for name, file := range map[string]string{
		"missing":     filepath.Join(t.TempDir(), "absent"),
		"unparseable": portRangeFile(t, "sixty thousand\n"),
	} {
		sys := system{
			addresses:     fixedAddresses("127.0.0.1"),
			catchAllFirst: 61400,
			catchAllLast:  61409,
			portRangeFile: file,
		}

		spec := Spec{
			Bind:      netip.MustParsePrefix("127.0.0.1/32"),
			Listeners: []Listener{{Upstream: host.address, Kind: CatchAll}},
			Token:     token,
		}

		run := runSystem(t, sys, spec.Args())

		if line, printed := run.line(t); printed {
			t.Errorf("%s: printed %q, want no ready line", name, line)
		}

		if code := run.wait(t); code != exitFailure {
			t.Errorf("%s: exit = %d, want %d", name, code, exitFailure)
		}

		if stderr := run.stderr.String(); !strings.Contains(stderr, file) {
			t.Errorf("%s: stderr %q does not name %s", name, stderr, file)
		}
	}
}
