package harness

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Wintersta7e/stutter/internal/relay"
)

var (
	// errListenerConfig means the invocation listeners were asked for with something missing or
	// unusable.
	errListenerConfig = errors.New("invalid invocation listener configuration")
	// errSRV means the service looked up an SRV record: it names a service and port Stutter cannot stand
	// in for, and the connection that follows would go unseen.
	errSRV = errors.New("the service looked up an SRV record")
	// errSeedBusOpen means the seed bus was opened twice.
	errSeedBusOpen = errors.New("the seed bus is already open")
)

// srvQuery is the DNS query type of an SRV lookup.
const srvQuery uint16 = 33

// UpstreamSource returns, for one start, where each endpoint the listener set serves really is: every
// restored dependency's address and the bus's, keyed like the listeners. It is read once per start,
// after that start's restores, because a restored dependency comes back on a new address.
type UpstreamSource func(ctx context.Context) (map[string]netip.AddrPort, error)

// ListenerConfig describes the invocation listeners.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type ListenerConfig struct {
	// Bind is the one address every listener binds: never a wildcard.
	Bind netip.Addr
	// Upstreams is read once per start, when the start attaches. Required.
	Upstreams UpstreamSource
	// HTTPHost is the logical host the HTTP stub's certificate and effects name. Empty uses the default.
	HTTPHost string
	// Postgres are the endpoint keys a Postgres proxy serves.
	Postgres []string
	// Opaque are the endpoint keys an opaque proxy serves.
	Opaque []string
	// Bus are the bus relay's client ports.
	Bus []uint16
	// Token is what every relay's preamble must carry.
	Token relay.Token
	// Mode is how containers reach Bind. Required.
	Mode Mode
}

// ListenerCounts are the connections the set refused. Foreign did not open with this check's preamble,
// or named a port its listener does not serve; Unattached were valid but arrived while no start was
// attached. Neither is ever recorded.
type ListenerCounts struct {
	Foreign    int
	Unattached int
}

// endpoint is one of the set's listeners and the preamble ports it accepts.
type endpoint struct {
	listener net.Listener
	accepts  func(port uint16) bool
	key      string
	port     uint16
}

// ListenerSet is one check's listeners on the host, opened once before any relay exists and closed
// only after every relay is gone. Every relay pipes to one of them; each start attaches to the set and
// serves what it hands over, and between starts a valid connection is refused and counted.
//
// Every connection must open with the check's preamble, because the listeners are reachable from every
// container on the engine: one that does not is closed, counted Foreign, and never recorded.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type ListenerSet struct {
	// authority is the one certificate authority every consumer check's TLS stub presents.
	authority *authority
	// attached is the start the set hands connections to, nil between starts.
	attached  *attachment
	endpoints map[string]*endpoint
	// conns are the connections whose preamble is still being read, closed with the set.
	conns map[net.Conn]struct{}
	// seedBus pipes the seed phase's jobs to the bus; seedHandover feeds it. Both nil outside the
	// seed phase.
	seedBus      *pipe
	seedHandover *handover
	// signals are the stub relays' signal connections, held for the set's life.
	signals []net.Conn
	// queries are every signal record, in arrival order.
	queries []relay.Query
	// advertise is the address containers dial the listeners at, once verified.
	advertise  netip.Addr
	cfg        ListenerConfig
	wg         sync.WaitGroup
	foreign    atomic.Int64
	unattached atomic.Int64
	mu         sync.Mutex
	closed     bool
}

// OpenListeners opens one listener per endpoint key on cfg.Bind — every dependency key, the bus, its
// monitor, the two HTTP stubs, the catch-all, the DNS signal and the verification listener — and mints
// the check's certificate authority. The seed bus's listener opens only for the seed phase.
func OpenListeners(ctx context.Context, cfg ListenerConfig) (*ListenerSet, error) {
	if err := validateListeners(cfg); err != nil {
		return nil, err
	}

	host := cfg.HTTPHost
	if host == "" {
		host = defaultHTTPHost
	}

	minted, err := newAuthority(host)
	if err != nil {
		return nil, err
	}

	set := &ListenerSet{
		authority: minted,
		endpoints: make(map[string]*endpoint),
		conns:     make(map[net.Conn]struct{}),
		cfg:       cfg,
	}

	for key, accepts := range set.acceptance() {
		if err := set.open(ctx, key, accepts); err != nil {
			return nil, errors.Join(err, set.Close(ctx))
		}
	}

	return set, nil
}

// validateListeners refuses a configuration the set could not serve.
func validateListeners(cfg ListenerConfig) error {
	switch {
	case cfg.Upstreams == nil:
		return fmt.Errorf("%w: no upstream source", errListenerConfig)
	case cfg.Mode == 0:
		return fmt.Errorf("%w: no host-address mode", errListenerConfig)
	case !cfg.Bind.Is4() || cfg.Bind.IsUnspecified():
		return fmt.Errorf("%w: bind %q is not one IPv4 address", errListenerConfig, cfg.Bind)
	case cfg.Token == relay.Token{}:
		return fmt.Errorf("%w: no token", errListenerConfig)
	default:
	}

	if err := validateBus(cfg.Bus); err != nil {
		return err
	}

	return validateKeys(slices.Concat(cfg.Postgres, cfg.Opaque))
}

// validateBus refuses a bus relay with no client port, or one on a port that is not a client's.
func validateBus(ports []uint16) error {
	if len(ports) == 0 {
		return fmt.Errorf("%w: no bus port", errListenerConfig)
	}

	for _, port := range ports {
		if port == 0 || port == MonitorPort {
			return fmt.Errorf("%w: bus port %d is not a client port", errListenerConfig, port)
		}
	}

	return nil
}

// validateKeys refuses a malformed endpoint key, or one served twice.
func validateKeys(keys []string) error {
	seen := make(map[string]bool, len(keys))

	for _, key := range keys {
		if _, _, ok := ParseEndpointKey(key); !ok {
			return fmt.Errorf("%w: %q is not a <service>:<port> key", errListenerConfig, key)
		}

		if seen[key] {
			return fmt.Errorf("%w: %s is served twice", errListenerConfig, key)
		}

		seen[key] = true
	}

	return nil
}

// catchAllPort reports whether a port reaches the catch-all: every port the stub relay's catch-all
// binds, which is none of the DNS port, the two stub ports and the relay's reserved source ports.
func catchAllPort(port uint16) bool {
	return port != 0 && port != relay.DNSPort && port != HTTPPort && port != HTTPSPort && port < relay.ReservedFirst
}

// Port is the port the listener for key is bound to, and false when the set has none open for it.
func (s *ListenerSet) Port(key string) (uint16, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	opened, found := s.endpoints[key]
	if !found {
		return 0, false
	}

	return opened.port, true
}

// SetAdvertise records the verified address containers dial the listeners at.
func (s *ListenerSet) SetAdvertise(host netip.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.advertise = host
}

// CAPEM is the check's certificate authority, which the service must trust to reach the TLS stub.
func (s *ListenerSet) CAPEM() []byte {
	return s.authority.pem
}

// CloseVerify closes the verification listener, once the host's address is verified.
func (s *ListenerSet) CloseVerify() error {
	s.mu.Lock()
	opened := s.endpoints[KeyVerify]
	delete(s.endpoints, KeyVerify)
	s.mu.Unlock()

	if opened == nil {
		return nil
	}

	if err := opened.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("close the verification listener: %w", err)
	}

	return nil
}

// Counts are the connections the set refused so far.
func (s *ListenerSet) Counts() ListenerCounts {
	return ListenerCounts{Foreign: int(s.foreign.Load()), Unattached: int(s.unattached.Load())}
}

// Mode is how containers reach the listeners.
func (s *ListenerSet) Mode() Mode {
	return s.cfg.Mode
}

// Queries are every DNS query the stub relay signalled, in the order they arrived.
func (s *ListenerSet) Queries() []relay.Query {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.queries)
}

// OpenSeedBus opens the seed bus's listener for the seed phase: jobs reach the bus through it, never
// recorded and never attached to a start. A connection for the monitoring port is piped to monitor,
// one for a bus client port to client. It returns the listener's port.
//
// The addresses are the caller's to give once, because the bus does not restart during the seed phase.
func (s *ListenerSet) OpenSeedBus(ctx context.Context, client, monitor netip.AddrPort) (uint16, error) {
	seed := newHandover(nil)
	bus := &pipe{listener: seed, route: func(conn net.Conn) string {
		if relayed, ok := conn.(*relay.Conn); ok && relayed.DestinationPort() == MonitorPort {
			return monitor.String()
		}

		return client.String()
	}}

	s.mu.Lock()

	if s.seedHandover != nil {
		s.mu.Unlock()

		return 0, errSeedBusOpen
	}

	s.seedHandover, s.seedBus = seed, bus
	s.mu.Unlock()

	s.wg.Go(func() {
		_ = bus.serve(ctx) //nolint:errcheck // its failure is read, and named, by CloseSeedBus.
	})

	accepts := func(port uint16) bool { return port == MonitorPort || slices.Contains(s.cfg.Bus, port) }

	if err := s.open(ctx, KeySeedBus, accepts); err != nil {
		return 0, errors.Join(err, s.CloseSeedBus())
	}

	port, _ := s.Port(KeySeedBus)

	return port, nil
}

// CloseSeedBus closes the seed bus's listener and waits for its pipes. It returns the first pipe that
// could not reach the bus, naming the address it tried.
func (s *ListenerSet) CloseSeedBus() error {
	s.mu.Lock()
	opened := s.endpoints[KeySeedBus]
	delete(s.endpoints, KeySeedBus)
	bus := s.seedBus
	s.seedBus, s.seedHandover = nil, nil
	s.mu.Unlock()

	if opened != nil {
		_ = opened.listener.Close()
	}

	if bus == nil {
		return nil
	}

	bus.stop()

	if failure := bus.failure(); failure != nil {
		return fmt.Errorf("the seed bus: %w", failure)
	}

	return nil
}

// Close closes every listener and every connection still being read, and waits for their goroutines
// until ctx ends. The relays must be gone first: a listener's close waits for in-flight connections.
func (s *ListenerSet) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true

	listeners := make([]net.Listener, 0, len(s.endpoints))
	for _, opened := range s.endpoints {
		listeners = append(listeners, opened.listener)
	}

	conns := make([]net.Conn, 0, len(s.conns)+len(s.signals))
	for conn := range s.conns {
		conns = append(conns, conn)
	}

	conns = append(conns, s.signals...)
	s.mu.Unlock()

	for _, listener := range listeners {
		_ = listener.Close()
	}

	for _, conn := range conns {
		_ = conn.Close()
	}

	// Closed with the rest if the seed phase never ended; its dial failures are the seed phase's.
	_ = s.CloseSeedBus() //nolint:errcheck // see above.

	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("close the invocation listeners: %w", ctx.Err())
	}
}

// acceptance is, for every key the set opens, the preamble ports that key's listener accepts.
func (s *ListenerSet) acceptance() map[string]func(uint16) bool {
	only := func(want uint16) func(uint16) bool {
		return func(port uint16) bool { return port == want }
	}

	keys := map[string]func(uint16) bool{
		KeyBus:        func(port uint16) bool { return slices.Contains(s.cfg.Bus, port) },
		KeyBusMonitor: only(MonitorPort),
		KeyHTTP:       only(HTTPPort),
		KeyHTTPS:      only(HTTPSPort),
		KeyCatchAll:   catchAllPort,
		KeyDNSSignal:  only(relay.DNSPort),
		KeyVerify:     only(0),
	}

	for _, key := range slices.Concat(s.cfg.Postgres, s.cfg.Opaque) {
		_, port, _ := ParseEndpointKey(key)
		keys[key] = only(port)
	}

	return keys
}

// open binds one key's listener and starts accepting on it.
func (s *ListenerSet) open(ctx context.Context, key string, accepts func(uint16) bool) error {
	var config net.ListenConfig

	listener, err := config.Listen(ctx, "tcp4", netip.AddrPortFrom(s.cfg.Bind, 0).String())
	if err != nil {
		return fmt.Errorf("open the %s listener on %s: %w", key, s.cfg.Bind, err)
	}

	opened := &endpoint{
		listener: listener,
		accepts:  accepts,
		key:      key,
		port:     netip.MustParseAddrPort(listener.Addr().String()).Port(),
	}

	s.mu.Lock()
	s.endpoints[key] = opened
	s.mu.Unlock()

	s.wg.Go(func() { s.accept(opened) })

	return nil
}

// accept reads each connection's preamble in a goroutine of its own, so a stranger that never sends
// one holds up nobody else. It ends when the listener closes.
func (s *ListenerSet) accept(opened *endpoint) {
	for {
		conn, err := opened.listener.Accept()
		if err != nil {
			return
		}

		if !s.track(conn) {
			return
		}

		s.wg.Go(func() { s.admit(opened, conn) })
	}
}

// admit checks one connection's preamble and decides what becomes of it: a stranger is closed and
// counted, a probe is answered, and a relayed connection goes to the attached start — or, between
// starts, is closed and counted.
func (s *ListenerSet) admit(opened *endpoint, conn net.Conn) {
	relayed, err := relay.Accept(conn, s.cfg.Token)

	s.forget(conn)

	if err != nil || !opened.accepts(relayed.DestinationPort()) {
		s.foreign.Add(1)

		_ = conn.Close()

		return
	}

	switch attached := s.current(); {
	case opened.key == KeyVerify:
		// The probe is judged by its answer; one that goes astray is the verifier's to report.
		_ = relay.WriteAck(relayed) //nolint:errcheck // see above.
	case opened.key == KeyDNSSignal:
		// Held across starts: a stub relay opens it once, before any start attaches.
		s.holdSignal(relayed)

		return
	case opened.key == KeySeedBus:
		s.offerSeed(relayed)

		return
	case attached != nil:
		attached.take(opened.key, relayed)

		return
	default:
		s.unattached.Add(1)
	}

	_ = conn.Close()
}

// holdSignal reads a stub relay's signal records until the connection ends. The relay outlives every
// start, so the connection does too; a record is attributed to whichever start is attached when it
// arrives.
func (s *ListenerSet) holdSignal(conn *relay.Conn) {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()

		_ = conn.Close()

		return
	}

	s.signals = append(s.signals, conn)
	s.mu.Unlock()

	for {
		query, err := relay.ReadQuery(conn)
		if err != nil {
			// The relay is gone; its liveness check reports that, not the signal.
			_ = conn.Close()

			return
		}

		s.signalled(query)
	}
}

// signalled records one query. Attached, it counts on the start, and an SRV lookup stops it: the
// record names a service Stutter cannot stand in for. Between starts it is counted Unattached.
func (s *ListenerSet) signalled(query relay.Query) {
	s.mu.Lock()
	s.queries = append(s.queries, query)
	attached := s.attached
	s.mu.Unlock()

	if attached == nil {
		s.unattached.Add(1)

		return
	}

	attached.signalled.Add(1)

	if query.Type == srvQuery {
		attached.fail(fmt.Errorf("%w: %s", errSRV, query.Name))
	}
}

// offerSeed hands a seed-phase connection to the seed bus's pipe, or refuses it outside the seed phase.
func (s *ListenerSet) offerSeed(conn *relay.Conn) {
	s.mu.Lock()
	seed := s.seedHandover
	s.mu.Unlock()

	if seed == nil || !seed.offer(conn) {
		_ = conn.Abort() //nolint:errcheck // the seed phase is over; nothing serves the connection.
	}
}

// track registers a connection whose preamble is being read, or closes it when the set is closing.
func (s *ListenerSet) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		_ = conn.Close()

		return false
	}

	s.conns[conn] = struct{}{}

	return true
}

// forget stops tracking a connection once its preamble has been read: from then on it is closed where
// it is decided, or handed to the attached start, which owns it.
func (s *ListenerSet) forget(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.conns, conn)
}

// advertised is the verified address containers dial the listeners at; zero before verification.
func (s *ListenerSet) advertised() netip.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.advertise
}
