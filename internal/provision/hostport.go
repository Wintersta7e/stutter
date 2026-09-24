package provision

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math/big"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// Where a published container port's host port comes from.
//
// Left to choose, Docker Desktop publishes on a port from Windows' dynamic range while the WSL kernel
// hands its own sockets ports from its ephemeral range, and the two overlap. Measured: a container
// published on a port a WSL socket held, even one in TIME-WAIT, was ready and refused every connect —
// 1,194 of 1,194 on one such port, 3 of 45 probes under port churn, every failed port in 49152–60999.
// Neither kernel hands out a port below both ranges on its own.
const (
	// loopbackHost is the only address a container port is ever published on.
	loopbackHost = "127.0.0.1"
	// windowsDynamicStart is the first port of Windows' dynamic range, 49152–65535.
	windowsDynamicStart = 49152
	// hostPortFloor is the lowest port chosen while the ephemeral range starts above it.
	hostPortFloor = 20000
	// unprivilegedFloor is the lowest port chosen at all: the first an unprivileged process may bind.
	unprivilegedFloor = 1024
	// highestPort is the highest TCP port.
	highestPort = 65535
	// hostPortCandidates bounds how many ports one selection tries.
	hostPortCandidates = 64
	// localPortRange is where the kernel publishes its ephemeral range.
	localPortRange = "/proc/sys/net/ipv4/ip_local_port_range"
)

// ErrNoHostPort means no host port could be chosen to publish a container port on.
var ErrNoHostPort = errors.New("no host port is free to publish on")

// hostPorts are the host ports selected in this process and not yet released. A port is free on the
// host from its selection until its container starts, so two creates choosing meanwhile could both
// pick it, and the engine would refuse the second one's start.
//
//nolint:gochecknoglobals // one reservation table per process, shared by every engine and the test helper.
var hostPorts = struct {
	taken map[uint16]bool
	mu    sync.Mutex
}{taken: map[uint16]bool{}}

// ReserveHostPort chooses a host port to publish a container port on, and reserves it in this
// process until ReleaseHostPort: below both the kernel's ephemeral range and Windows' dynamic range,
// not avoided, reserved by nothing else here, and free on the host's loopback when checked. The first
// candidate is random, so two processes choosing at once rarely meet.
func ReserveHostPort(ctx context.Context, avoid ...uint16) (uint16, error) {
	start, err := ephemeralStart()
	if err != nil {
		return 0, err
	}

	band, err := hostPortBand(start)
	if err != nil {
		return 0, err
	}

	width := band.hi - band.lo + 1

	offset, err := rand.Int(rand.Reader, big.NewInt(int64(width)))
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrNoHostPort, err)
	}

	hostPorts.mu.Lock()
	defer hostPorts.mu.Unlock()

	tries := min(hostPortCandidates, width)

	for n := range tries {
		port := uint16(band.lo + (int(offset.Int64())+n)%width) //nolint:gosec // the band lies inside 1024–65535.

		if hostPorts.taken[port] || slices.Contains(avoid, port) || !freeOnLoopback(ctx, port) {
			continue
		}

		hostPorts.taken[port] = true

		return port, nil
	}

	return 0, fmt.Errorf("%w: %d candidates from %d in %d–%d were all taken", ErrNoHostPort, tries,
		band.lo+int(offset.Int64()), band.lo, band.hi)
}

// ReleaseHostPort ends a reservation ReserveHostPort made.
func ReleaseHostPort(port uint16) {
	hostPorts.mu.Lock()
	defer hostPorts.mu.Unlock()

	delete(hostPorts.taken, port)
}

// reservedHostPort reports whether port is reserved in this process.
func reservedHostPort(port uint16) bool {
	hostPorts.mu.Lock()
	defer hostPorts.mu.Unlock()

	return hostPorts.taken[port]
}

// portBand is a range of ports, lo to hi inclusive.
type portBand struct {
	lo, hi int
}

// hostPortBand is the band host ports are chosen from, given where the kernel's ephemeral range
// starts: below it and below Windows' dynamic range, from hostPortFloor while that leaves room, else
// from the first unprivileged port.
func hostPortBand(ephemeral int) (portBand, error) {
	band := portBand{lo: hostPortFloor, hi: min(ephemeral, windowsDynamicStart) - 1}
	if band.hi < band.lo {
		band.lo = unprivilegedFloor
	}

	if band.hi < band.lo {
		return portBand{}, fmt.Errorf("%w: the local port range starts at %d, leaving no port from %d below it",
			ErrNoHostPort, ephemeral, unprivilegedFloor)
	}

	return band, nil
}

// ephemeralStart is where the kernel's ephemeral range starts. A host without the file, which is not
// Linux, is taken to start where Windows' and macOS's ranges do.
func ephemeralStart() (int, error) {
	data, err := os.ReadFile(localPortRange)
	if errors.Is(err, fs.ErrNotExist) {
		return windowsDynamicStart, nil
	}

	if err != nil {
		return 0, fmt.Errorf("%w: read %s: %w", ErrNoHostPort, localPortRange, err)
	}

	return parsePortRange(string(data))
}

// parsePortRange reads the start of the range the kernel writes as two numbers.
func parsePortRange(text string) (int, error) {
	fields := strings.Fields(text)
	if len(fields) != 2 { //nolint:mnd // the kernel writes the range's first and last port.
		return 0, fmt.Errorf("%w: %s reads %q, not a range", ErrNoHostPort, localPortRange, text)
	}

	start, err := strconv.Atoi(fields[0])
	if err != nil || start < 1 || start > highestPort {
		return 0, fmt.Errorf("%w: %s reads %q, not a range", ErrNoHostPort, localPortRange, text)
	}

	return start, nil
}

// startAttempts bounds how many containers one Start tries when the engine keeps refusing their host
// ports.
const startAttempts = 4

// portRefusals are the engine's words for a start whose host port is taken: its own when another
// container holds the port, Docker Desktop's when a host process does, and the userland proxy's.
func portRefusals() []string {
	return []string{"port is already allocated", "ports are not available", "address already in use"}
}

// portRefused reports whether a failed start is the engine refusing a taken host port.
func portRefused(err error) bool {
	var call *CallError
	if !errors.As(err, &call) {
		return false
	}

	return slices.ContainsFunc(portRefusals(), func(words string) bool { return strings.Contains(call.Stderr, words) })
}

// reserveHostPorts reserves a host port for each published container port, none of them in avoid. On
// an error nothing stays reserved.
func reserveHostPorts(ctx context.Context, publish, avoid []uint16) (map[uint16]uint16, error) {
	hostPorts := make(map[uint16]uint16, len(publish))

	for _, port := range publish {
		host, err := ReserveHostPort(ctx, avoid...)
		if err != nil {
			releaseHostPorts(hostPorts)

			return nil, fmt.Errorf("publish container port %d: %w", port, err)
		}

		hostPorts[port] = host
	}

	return hostPorts, nil
}

// releaseHostPorts releases every host port in hostPorts.
func releaseHostPorts(hostPorts map[uint16]uint16) {
	for _, host := range hostPorts {
		ReleaseHostPort(host)
	}
}

// hostPortList names a container's host ports, in order.
func hostPortList(hostPorts map[uint16]uint16) string {
	ports := make([]string, 0, len(hostPorts))
	for _, host := range slices.Sorted(maps.Values(hostPorts)) {
		ports = append(ports, strconv.Itoa(int(host)))
	}

	return strings.Join(ports, ",")
}

// holdHostPorts keeps a created container's host ports reserved until the container is removed.
func (e *Engine) holdHostPorts(seq int, hostPorts map[uint16]uint16) {
	if len(hostPorts) == 0 {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.hostPorts == nil {
		e.hostPorts = map[int][]uint16{}
	}

	e.hostPorts[seq] = slices.Collect(maps.Values(hostPorts))
}

// releaseHeldHostPorts releases a removed container's host ports.
func (e *Engine) releaseHeldHostPorts(seq int) {
	e.mu.Lock()
	held := e.hostPorts[seq]
	delete(e.hostPorts, seq)
	e.mu.Unlock()

	for _, host := range held {
		ReleaseHostPort(host)
	}
}

// freeOnLoopback reports whether nothing on the host holds port on the loopback address now.
func freeOnLoopback(ctx context.Context, port uint16) bool {
	var config net.ListenConfig

	listener, err := config.Listen(ctx, "tcp4", net.JoinHostPort(loopbackHost, strconv.Itoa(int(port))))
	if err != nil {
		return false
	}

	_ = listener.Close()

	return true
}
