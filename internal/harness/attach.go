package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wintersta7e/stutter/internal/effect"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	opaqueproxy "github.com/Wintersta7e/stutter/internal/proxy/opaque"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
	"github.com/Wintersta7e/stutter/internal/relay"
	"github.com/Wintersta7e/stutter/internal/waits"
)

// keyListeners names the attachment's own entry among a start's proxies: it fails when the set hands
// the start something it cannot serve.
const keyListeners = "listeners"

var (
	// errAttached means a second start tried to attach while one already was: an internal error, since
	// starts are serial.
	errAttached = errors.New("internal error: a start is already attached to the listener set")
	// errUpstreamMissing means the per-start upstream source left out an endpoint the set serves.
	errUpstreamMissing = errors.New("the upstream source has no usable address for an endpoint the set serves")
	// errUnserved means a relayed connection arrived on an endpoint no proxy of this start serves.
	errUnserved = errors.New("a connection arrived on an endpoint this start does not serve")
	// errDetach means proxies were still busy when the drain bound ran out.
	errDetach = errors.New("proxies did not finish their connections within the drain bound")
)

// handover is an in-memory listener: the set hands it connections whose preamble it verified, and a
// proxy serves them exactly as if it had accepted them itself. It binds nothing.
type handover struct {
	addr  net.Addr
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func newHandover(addr net.Addr) *handover {
	return &handover{addr: addr, conns: make(chan net.Conn), done: make(chan struct{})}
}

// Accept returns the next handed-over connection, or net.ErrClosed once closed. A connection that
// arrives as the hand-over closes is reset rather than returned: whoever serves the listener is
// stopping, and must not be handed work after its close has begun.
func (h *handover) Accept() (net.Conn, error) {
	select {
	case conn := <-h.conns:
		select {
		case <-h.done:
			abort(conn)

			return nil, net.ErrClosed
		default:
			return conn, nil
		}
	case <-h.done:
		return nil, net.ErrClosed
	}
}

// Close stops the hand-over. It may be called more than once.
func (h *handover) Close() error {
	h.once.Do(func() { close(h.done) })

	return nil
}

// Addr is the set's listener for the same key.
func (h *handover) Addr() net.Addr {
	return h.addr
}

// offer hands a connection to whoever is accepting, and reports false once the hand-over is closed.
func (h *handover) offer(conn net.Conn) bool {
	select {
	case h.conns <- conn:
		return true
	case <-h.done:
		return false
	}
}

// attachment is one start's hold on the listener set, from its proxies' construction until the
// service is gone and they have drained.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type attachment struct {
	set *ListenerSet
	// failure carries the first thing the set handed over that this start cannot serve.
	failure  chan error
	detached chan struct{}
	// handovers are the endpoints this start serves; tracked are the connections handed over on each.
	handovers map[string]*handover
	tracked   map[string][]*relay.Conn
	upstreams map[string]netip.AddrPort
	// signalled counts the DNS signal records that arrived while this start was attached.
	signalled  atomic.Int64
	failOnce   sync.Once
	detachOnce sync.Once
	mu         sync.Mutex
}

// attach attaches a start to the set: at most one at a time. The per-start upstream source is read
// here, once, after the start's restores, and must name every endpoint the set serves.
func (s *ListenerSet) attach(ctx context.Context) (*attachment, error) {
	s.mu.Lock()

	if s.attached != nil {
		s.mu.Unlock()

		return nil, errAttached
	}

	attached := &attachment{
		set:       s,
		failure:   make(chan error, 1),
		detached:  make(chan struct{}),
		handovers: make(map[string]*handover),
		tracked:   make(map[string][]*relay.Conn),
	}

	for _, key := range s.servedKeys() {
		attached.handovers[key] = newHandover(s.endpoints[key].listener.Addr())
	}

	s.attached = attached
	s.mu.Unlock()

	upstreams, err := s.cfg.Upstreams(ctx)
	if err != nil {
		attached.release()

		return nil, fmt.Errorf("read the upstream addresses: %w", err)
	}

	for _, key := range s.servedKeys() {
		if address, found := upstreams[key]; !found || !address.Addr().Is4() {
			attached.release()

			return nil, fmt.Errorf("%w: %s (source gave %q)", errUpstreamMissing, key, address)
		}
	}

	attached.upstreams = upstreams

	return attached, nil
}

// servedKeys are the endpoints every start serves with a proxy or a pipe of its own.
func (s *ListenerSet) servedKeys() []string {
	return slices.Concat(s.cfg.Postgres, s.cfg.Opaque, []string{KeyBus, KeyBusMonitor})
}

// current is the attached start, or nil between starts.
func (s *ListenerSet) current() *attachment {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.attached
}

// listener is the in-memory listener a proxy of this start serves key's connections from. A key with no
// listener yet gets one, and is served from then on.
func (a *attachment) listener(key string) net.Listener {
	a.mu.Lock()
	defer a.mu.Unlock()

	if served, found := a.handovers[key]; found {
		return served
	}

	a.set.mu.Lock()
	addr := a.set.endpoints[key].listener.Addr()
	a.set.mu.Unlock()

	served := newHandover(addr)
	a.handovers[key] = served

	return served
}

// upstream is where key's endpoint really is, for this start.
func (a *attachment) upstream(key string) string {
	return a.upstreams[key].String()
}

// take hands a verified connection to the proxy serving key. A key no proxy of this start serves is
// reset and fails the start, loudly: a silent reset would read as a dependency the service never used.
func (a *attachment) take(key string, conn *relay.Conn) {
	a.mu.Lock()

	served, found := a.handovers[key]
	if found {
		a.tracked[key] = append(a.tracked[key], conn)
	}

	a.mu.Unlock()

	if !found {
		_ = conn.Abort() //nolint:errcheck // the start is failing either way.

		a.fail(fmt.Errorf("%w: %s", errUnserved, key))

		return
	}

	if !served.offer(conn) {
		_ = conn.Abort() //nolint:errcheck // the start is detaching; nothing serves the connection.
	}
}

// fail ends the start at the first thing it cannot serve.
func (a *attachment) fail(err error) {
	a.failOnce.Do(func() { a.failure <- err })
}

// serve is the attachment's entry among the start's proxies: it returns the attachment's failure, or
// nil once the start detaches.
func (a *attachment) serve(context.Context) error {
	select {
	case err := <-a.failure:
		return err
	case <-a.detached:
		return nil
	}
}

// markDetached lets serve return. It is the attachment entry's closer.
func (a *attachment) markDetached(context.Context) error {
	a.detachOnce.Do(func() { close(a.detached) })

	return nil
}

// stopHandingOver closes every hand-over, so each proxy's Serve returns and nothing new arrives.
func (a *attachment) stopHandingOver() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, served := range a.handovers {
		_ = served.Close()
	}
}

// abort resets every connection handed over on keys, so proxies stuck on them finish.
func (a *attachment) abort(keys []string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, key := range keys {
		for _, conn := range a.tracked[key] {
			_ = conn.Abort() //nolint:errcheck // a connection already closed has nothing left to reset.
		}
	}
}

// release detaches the start from the set, so the next can attach.
func (a *attachment) release() {
	a.stopHandingOver()

	a.set.mu.Lock()
	defer a.set.mu.Unlock()

	if a.set.attached == a {
		a.set.attached = nil
	}
}

// detach ends a start's hold on the set: hand-over stops, every proxy drains concurrently within the
// drain bound, and the keys whose proxies have not are reset and named — a stuck proxy fails the start
// rather than hanging it. The set is released last.
func (e *egress) detach(ctx context.Context) error {
	e.attached.stopHandingOver()

	finished := make(chan closed, len(e.closers))

	for _, entry := range e.closers {
		go func() { finished <- closed{key: entry.key, err: entry.close(ctx)} }()
	}

	pending := make(map[string]int, len(e.closers))
	for _, entry := range e.closers {
		pending[entry.key]++
	}

	err := drainWithin(finished, pending, len(e.closers), waits.ProxyDrain, e.attached.abort)

	e.attached.release()

	return err
}

// closed is one entry's closer having returned.
type closed struct {
	err error
	key string
}

// drainWithin collects count closers from finished. Past bound it resets the keys still pending, names
// them, and collects the rest.
func drainWithin(
	finished <-chan closed,
	pending map[string]int,
	count int,
	bound time.Duration,
	reset func(keys []string),
) error {
	var err error

	collect := func(done closed) {
		err = errors.Join(err, done.err)

		if pending[done.key]--; pending[done.key] == 0 {
			delete(pending, done.key)
		}
	}

	timer := time.NewTimer(bound)
	defer timer.Stop()

	for received := 0; received < count; received++ {
		select {
		case done := <-finished:
			collect(done)
		case <-timer.C:
			stuck := slices.Sorted(maps.Keys(pending))
			reset(stuck)

			err = errors.Join(err, fmt.Errorf("%w of %s: %s", errDetach, bound, strings.Join(stuck, ", ")))

			for ; received < count; received++ {
				collect(<-finished)
			}

			return err
		}
	}

	return err
}

// observeRelayed attaches the start to the invocation listeners and serves every endpoint from them: a
// proxy per database and unparsed dependency, the bus's proxy, and the unrecorded monitoring pipe. The
// service is told no address: it dials its dependencies by name, and the names resolve to relays.
func (s *Sandbox) observeRelayed(ctx context.Context, sink *effect.Recorder, bus natsproxy.Options) (*egress, error) {
	attached, err := s.cfg.Listeners.attach(ctx)
	if err != nil {
		return nil, err
	}

	observed := &egress{served: make(chan error, s.relayedEntries()), attached: attached}
	observed.start(ctx, keyListeners, attached.markDetached, attached.serve)

	if err := s.serveRelayed(ctx, sink, bus, observed, attached); err != nil {
		if observed.httpRun != nil {
			err = errors.Join(err, observed.httpRun.Abort())
		}

		return nil, errors.Join(err, observed.close(ctx))
	}

	return observed, nil
}

// relayedEntries is how many Serve goroutines a relayed start has, and so how many results its close
// drains: the attachment, the bus, its monitoring pipe, the HTTP stub, and a proxy per database and
// unparsed key.
func (s *Sandbox) relayedEntries() int {
	const alwaysRelayed = 4

	return alwaysRelayed + len(s.cfg.Listeners.cfg.Postgres) + len(s.cfg.Listeners.cfg.Opaque)
}

// serveRelayed constructs the start's proxies over the attachment's listeners.
func (s *Sandbox) serveRelayed(
	ctx context.Context,
	sink *effect.Recorder,
	bus natsproxy.Options,
	observed *egress,
	attached *attachment,
) error {
	busProxy := natsproxy.New(attached.listener(KeyBus), attached.upstream(KeyBus), sink, bus)
	observed.start(ctx, KeyBus, ignoringContext(busProxy.Close), busProxy.Serve)

	monitor := startPipe(ctx, attached.listener(KeyBusMonitor),
		func(net.Conn) string { return attached.upstream(KeyBusMonitor) })
	observed.start(ctx, KeyBusMonitor, monitor.close, monitor.serve)

	for _, key := range s.cfg.Listeners.cfg.Postgres {
		database := pg.New(attached.listener(key), attached.upstream(key), sink)
		observed.start(ctx, key, ignoringContext(database.Close), database.Serve)
	}

	for _, key := range s.cfg.Listeners.cfg.Opaque {
		dependency, err := opaqueproxy.New(attached.listener(key), key, attached.upstream(key), sink)
		if err != nil {
			return fmt.Errorf("serve %s: %w", key, err)
		}

		observed.start(ctx, key, ignoringContext(dependency.Close), dependency.Serve)
	}

	return s.serveStub(ctx, sink, observed, attached)
}

// pipe splices every connection it is handed to an upstream, byte for byte and unrecorded. route picks
// the upstream; one it cannot dial resets the connection and stops the pipe, naming the upstream.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type pipe struct {
	listener net.Listener
	route    func(conn net.Conn) string
	failed   error
	// ended closes when the accept loop has returned.
	ended chan struct{}
	wg    sync.WaitGroup
	once  sync.Once
	mu    sync.Mutex
}

// startPipe starts accepting on listener. The accept loop counts itself in the pipe's WaitGroup, so
// each connection's goroutine is added while the count is above zero, never racing stop's wait.
func startPipe(ctx context.Context, listener net.Listener, route func(conn net.Conn) string) *pipe {
	started := &pipe{listener: listener, route: route, ended: make(chan struct{})}

	started.wg.Go(func() {
		defer close(started.ended)

		started.accept(ctx)
	})

	return started
}

// accept hands each connection to a goroutine of its own until the listener closes.
func (p *pipe) accept(ctx context.Context) {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}

		p.wg.Go(func() { p.splice(ctx, conn) })
	}
}

// serve waits for the accept loop to end, and returns the first dial failure: an entry's serve.
func (p *pipe) serve(context.Context) error {
	<-p.ended

	return p.failure()
}

func (p *pipe) splice(ctx context.Context, conn net.Conn) {
	upstream := p.route(conn)

	var dialer net.Dialer

	dialled, err := dialer.DialContext(ctx, "tcp4", upstream)
	if err != nil {
		abort(conn)

		if ctx.Err() == nil {
			p.once.Do(func() {
				p.mu.Lock()
				p.failed = fmt.Errorf("dial the upstream %s: %w", upstream, err)
				p.mu.Unlock()

				_ = p.listener.Close()
			})
		}

		return
	}

	relay.Splice(conn, dialled)
}

func (p *pipe) failure() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.failed
}

// close stops the pipe and waits for its connections: an entry's closer.
func (p *pipe) close(context.Context) error {
	p.stop()

	return nil
}

// stop closes the pipe's listener and waits for its connections.
func (p *pipe) stop() {
	_ = p.listener.Close()

	p.wg.Wait()
}

// abort closes a connection so its far end reads a reset.
func abort(conn net.Conn) {
	if aborter, ok := conn.(interface{ Abort() error }); ok {
		_ = aborter.Abort() //nolint:errcheck // the connection is being refused either way.

		return
	}

	_ = conn.Close()
}
