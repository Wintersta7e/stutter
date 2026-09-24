package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
)

// localPortRange is where the kernel says which ports it picks outbound source ports from.
const localPortRange = "/proc/sys/net/ipv4/ip_local_port_range"

var (
	// errBind means the relay could not find exactly one of its own addresses to listen on.
	errBind = errors.New("cannot choose an address to listen on")
	// errPortRange means the kernel's source-port range could not be read.
	errPortRange = errors.New("unreadable local port range")
)

// system is what the relay reads from the machine it runs on. Production reads the machine; internal
// tests replace a part of it.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type system struct {
	// addresses lists the IP addresses on the relay's own interfaces.
	addresses func() ([]netip.Addr, error)
	// lookup resolves a name to IPv4 addresses, for the verifier's host alias.
	lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	// portRangeFile holds the kernel's source-port range, which the catch-all leaves free.
	portRangeFile string
	// catchAllFirst and catchAllLast bound the ports a catch-all covers before any exclusion: every TCP
	// port, in production. A test process cannot bind the privileged ones.
	catchAllFirst uint16
	catchAllLast  uint16
	// dnsPort is where the responder listens: DNSPort in production. A test process cannot bind it.
	dnsPort uint16
}

// hostSystem reads the machine the relay runs on.
func hostSystem() system {
	return system{
		addresses:     interfaceAddresses,
		lookup:        lookupIPv4,
		portRangeFile: localPortRange,
		catchAllFirst: 1,
		catchAllLast:  math.MaxUint16,
		dnsPort:       DNSPort,
	}
}

// catchAllPorts is every port a catch-all binds: its whole span, except the DNS port, every pipe port,
// and the range the kernel is using for the relay's own dials — binding one of those would starve the
// dials the catch-all itself makes.
func (s system) catchAllPorts(spec Spec) ([]uint16, error) {
	sources, err := readPortRange(s.portRangeFile)
	if err != nil {
		return nil, err
	}

	excluded := map[uint16]bool{DNSPort: true}

	for _, listener := range spec.Listeners {
		if listener.Kind == Pipe {
			excluded[listener.Port] = true
		}
	}

	var ports []uint16

	for port := s.catchAllFirst; ; port++ {
		if !excluded[port] && (port < sources.first || port > sources.last) {
			ports = append(ports, port)
		}

		if port == s.catchAllLast {
			return ports, nil
		}
	}
}

// portSpan is an inclusive range of ports.
type portSpan struct {
	first uint16
	last  uint16
}

// readPortRange reads the kernel's source-port range: two numbers separated by whitespace.
func readPortRange(path string) (portSpan, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return portSpan{}, fmt.Errorf("%w: %w", errPortRange, err)
	}

	// The kernel writes the first port, whitespace, the last port.
	const fieldCount = 2

	fields := strings.Fields(string(content))
	if len(fields) != fieldCount {
		return portSpan{}, fmt.Errorf("%w: %s holds %q", errPortRange, path, content)
	}

	first, firstErr := strconv.ParseUint(fields[0], 10, 16)
	last, lastErr := strconv.ParseUint(fields[1], 10, 16)

	if firstErr != nil || lastErr != nil || first > last {
		return portSpan{}, fmt.Errorf("%w: %s holds %q", errPortRange, path, content)
	}

	return portSpan{first: uint16(first), last: uint16(last)}, nil
}

// interfaceAddresses lists the addresses on this machine's interfaces.
func interfaceAddresses() ([]netip.Addr, error) {
	found, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("list the interface addresses: %w", err)
	}

	addrs := make([]netip.Addr, 0, len(found))

	for _, entry := range found {
		network, isNetwork := entry.(*net.IPNet)
		if !isNetwork {
			continue
		}

		if addr, parsed := netip.AddrFromSlice(network.IP); parsed {
			addrs = append(addrs, addr.Unmap())
		}
	}

	return addrs, nil
}

// bindAddress is the one IPv4 address of the relay's own that lies in prefix.
//
// The engine assigns a container its address when it starts, so no address can be written into the
// relay's argv; the network's prefix can, and exactly one interface lies in it. Zero or several is a
// relay placed on the wrong networks, and binding a wildcard instead would answer on every network
// the relay is on.
func (s system) bindAddress(prefix netip.Prefix) (netip.Addr, error) {
	addrs, err := s.addresses()
	if err != nil {
		return netip.Addr{}, err
	}

	var inside []string

	var self netip.Addr

	for _, addr := range addrs {
		if addr.Is4() && prefix.Contains(addr) {
			self = addr
			inside = append(inside, addr.String())
		}
	}

	if len(inside) != 1 {
		return netip.Addr{}, fmt.Errorf("%w: bind prefix %s holds %d local IPv4 addresses [%s], want exactly 1",
			errBind, prefix, len(inside), strings.Join(inside, " "))
	}

	return self, nil
}

// serve runs one relay until ctx ends or something fails, and returns the exit code.
func (s system) serve(ctx context.Context, spec Spec, stdout, stderr io.Writer) int {
	self, err := s.bindAddress(spec.Bind)
	if err != nil {
		return report(stderr, exitFailure, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	relayed := &server{spec: spec, self: self, cancel: cancel, conns: make(map[net.Conn]struct{})}

	if err := relayed.bind(ctx, s); err != nil {
		relayed.shutdown()

		return report(stderr, exitFailure, err)
	}

	if _, err := fmt.Fprintf(stdout, "%s %s\n", Ready, self); err != nil {
		relayed.shutdown()

		return report(stderr, exitFailure, fmt.Errorf("print the ready line: %w", err))
	}

	relayed.start(ctx)

	<-ctx.Done()

	relayed.shutdown()

	if err := relayed.failure(); err != nil {
		return report(stderr, exitFailure, err)
	}

	return 0
}

// pipeListener is one bound pipe listener.
type pipeListener struct {
	listener net.Listener
	upstream netip.AddrPort
	port     uint16
}

// server is one serving relay's sockets and connections.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type server struct {
	// failed is the first failure, which ends the relay.
	failed error
	cancel context.CancelFunc
	// conns are every open connection, closed at shutdown so none outlives the relay.
	conns map[net.Conn]struct{}
	// catchAll is nil unless the spec has a catch-all listener.
	catchAll *catchAll
	// dnsUDP, dnsTCP, signalConn and signal are nil unless the relay answers DNS.
	dnsUDP     net.PacketConn
	dnsTCP     net.Listener
	signalConn net.Conn
	signal     *signalWriter
	pipes      []pipeListener
	self       netip.Addr
	// catchAllUpstream is where every catch-all connection is piped.
	catchAllUpstream netip.AddrPort
	spec             Spec
	wg               sync.WaitGroup
	mu               sync.Mutex
	once             sync.Once
	// closed refuses new connections once shutdown has begun.
	closed bool
}

// bind opens every socket the spec asks for, on the relay's own address and nowhere else, and the
// signal connection last.
func (s *server) bind(ctx context.Context, sys system) error {
	if err := s.bindListeners(ctx, sys); err != nil {
		return err
	}

	if !s.spec.Signal.IsValid() {
		return nil
	}

	return s.bindDNS(ctx, sys.dnsPort)
}

// bindListeners opens the pipe listeners and the catch-all.
func (s *server) bindListeners(ctx context.Context, sys system) error {
	var config net.ListenConfig

	for _, wanted := range s.spec.Listeners {
		if wanted.Kind == CatchAll {
			s.catchAllUpstream = wanted.Upstream

			continue
		}

		addr := netip.AddrPortFrom(s.self, wanted.Port)

		listener, err := config.Listen(ctx, "tcp4", addr.String())
		if err != nil {
			return fmt.Errorf("listen on %s: %w", addr, err)
		}

		s.pipes = append(s.pipes, pipeListener{listener: listener, upstream: wanted.Upstream, port: wanted.Port})
	}

	if !s.catchAllUpstream.IsValid() {
		return nil
	}

	ports, err := sys.catchAllPorts(s.spec)
	if err != nil {
		return err
	}

	s.catchAll, err = openCatchAll(s.self, ports)

	return err
}

// start accepts on every bound socket.
func (s *server) start(ctx context.Context) {
	for _, pipe := range s.pipes {
		s.wg.Go(func() { s.accept(ctx, pipe) })
	}

	if s.dnsUDP != nil {
		s.wg.Go(func() { s.serveUDP(ctx) })
		s.wg.Go(func() { s.serveTCP(ctx) })
	}

	if s.catchAll == nil {
		return
	}

	s.wg.Go(func() {
		err := s.catchAll.serve(func(client net.Conn, port uint16) {
			s.wg.Go(func() { s.relay(ctx, client, s.catchAllUpstream, port) })
		})
		if err != nil && ctx.Err() == nil {
			s.fail(err)
		}
	})
}

// accept hands each connection on one pipe listener to its own goroutine.
func (s *server) accept(ctx context.Context, pipe pipeListener) {
	for {
		client, err := pipe.listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				s.fail(fmt.Errorf("accept on %s: %w", pipe.listener.Addr(), err))
			}

			return
		}

		s.wg.Go(func() { s.relay(ctx, client, pipe.upstream, pipe.port) })
	}
}

// relay dials the upstream at once, before the client sends anything, and splices the two.
//
// The dial is eager because a server-first protocol never gets a first byte from its client: a relay
// that waited for one would deadlock the service's connection. A dial that fails resets the client
// and ends the relay — a dependency the service cannot reach is a run that cannot be judged, and
// ending loudly is what lets the next liveness check say so.
func (s *server) relay(ctx context.Context, client net.Conn, upstream netip.AddrPort, port uint16) {
	if !s.track(client) {
		return
	}

	defer s.untrack(client)

	conn, err := s.dial(ctx, upstream, port)
	if err != nil {
		_ = reset(client) //nolint:errcheck // the client is being refused; a failed close changes nothing.

		if ctx.Err() == nil {
			s.fail(err)
		}

		return
	}

	if !s.track(conn) {
		return
	}

	defer s.untrack(conn)

	Splice(client, conn)
}

// dial opens a host-side connection and frames it with the preamble.
func (s *server) dial(ctx context.Context, upstream netip.AddrPort, port uint16) (net.Conn, error) {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp4", upstream.String())
	if err != nil {
		return nil, fmt.Errorf("dial %s for port %d: %w", upstream, port, err)
	}

	if err := WritePreamble(conn, s.spec.Token, port); err != nil {
		_ = reset(conn) //nolint:errcheck // the dial has already failed.

		return nil, fmt.Errorf("dial %s for port %d: %w", upstream, port, err)
	}

	return conn, nil
}

// track registers an open connection, or closes it when the relay is already shutting down.
func (s *server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		_ = conn.Close()

		return false
	}

	s.conns[conn] = struct{}{}

	return true
}

func (s *server) untrack(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.conns, conn)
}

// fail records the first failure and ends the relay.
func (s *server) fail(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.failed = err
		s.mu.Unlock()

		s.cancel()
	})
}

func (s *server) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.failed
}

// shutdown closes every socket and waits for every goroutine. A stopped relay does not drain: whatever
// is in flight is cut off, as it would be when the process exits.
func (s *server) shutdown() {
	s.mu.Lock()
	s.closed = true

	open := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		open = append(open, conn)
	}
	s.mu.Unlock()

	for _, pipe := range s.pipes {
		_ = pipe.listener.Close()
	}

	for _, conn := range open {
		_ = conn.Close()
	}

	s.closeDNS()

	if s.catchAll != nil {
		s.catchAll.wake()
	}

	s.wg.Wait()

	// Only once its loop has returned: the loop waits on these descriptors.
	if s.catchAll != nil {
		s.catchAll.close()
	}
}
