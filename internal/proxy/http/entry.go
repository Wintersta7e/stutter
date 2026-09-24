package http

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"sync"
	"time"
)

// catchAllSilence is how long a connection on a catch-all port may stay silent: before its first byte,
// or after its TLS handshake before its first request. It is spent in full only on a connection about
// to stop the run: a client-first client speaks at once, and a server-first one never will.
const catchAllSilence = 5 * time.Second

// Entries are the listeners one run's stub serves. Every connection they accept records into the one
// sink and replies from the one script.
type Entries struct {
	// Cleartext is the entry a service dials on port 80.
	Cleartext net.Listener
	// TLS is the entry a service dials on port 443.
	TLS net.Listener
	// CatchAll is the entry every other port a service dials reaches. Its connections must be Tagged
	// with that port.
	CatchAll net.Listener
}

// Tagged is a connection that knows the port its client dialled, which the catch-all entry cannot
// learn from its own listener.
type Tagged interface {
	DestinationPort() uint16
}

// entryClass is which of the stub's entries a connection arrived on.
type entryClass uint8

const (
	entryCleartext entryClass = iota + 1
	entryTLS
	entryCatchAll
)

var (
	errNoEntries      = errors.New("HTTP stub requires at least one entry")
	errNoCertificates = errors.New("HTTP stub serving TLS requires a certificate source")
	// errUntagged means a connection reached the catch-all with no record of the port it was dialled on:
	// the relay in front of it is not doing its job, which is the environment's failure, not the
	// service's.
	errUntagged = errors.New("a connection reached the HTTP stub's catch-all without its destination port")
)

// servedKey is what a connection that sent a request was: the entry it arrived on, the port it was
// dialled on, and the server name it asked for. A request for a name on one port says nothing about a
// connection to the same name on another.
type servedKey struct {
	name  string
	port  uint16
	class entryClass
}

// connectError is a first request that asks for a tunnel to target.
type connectError struct{ target string }

func (e *connectError) Error() string { return "client sent a CONNECT request for " + e.target }

// entry is one listener of the stub. Its own goroutine accepts; a goroutine per connection decides
// what the connection is before net/http sees it; Accept only receives what has been decided, so no
// connection's wait ever delays another's accept.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type entry struct {
	listener net.Listener
	proxy    *Proxy
	ready    chan net.Conn
	done     chan struct{}
	err      error
	once     sync.Once
	class    entryClass
	port     uint16
}

func newEntry(listener net.Listener, proxy *Proxy, class entryClass, port uint16) *entry {
	return &entry{
		listener: listener,
		proxy:    proxy,
		ready:    make(chan net.Conn),
		done:     make(chan struct{}),
		class:    class,
		port:     port,
	}
}

// Accept returns the next connection judged ready for net/http.
func (e *entry) Accept() (net.Conn, error) {
	select {
	case conn := <-e.ready:
		return conn, nil
	case <-e.done:
		return nil, e.err
	}
}

// Close closes the listener the entry wraps.
func (e *entry) Close() error {
	e.stop(net.ErrClosed)

	return e.listener.Close() //nolint:wrapcheck // net/http tests the error for net.ErrClosed.
}

// Addr is the wrapped listener's address.
func (e *entry) Addr() net.Addr { return e.listener.Addr() }

// stop ends Accept with err, once.
func (e *entry) stop(err error) {
	e.once.Do(func() {
		e.err = err
		close(e.done)
	})
}

// listen accepts until the wrapped listener fails, judging each connection on a goroutine of its own.
func (e *entry) listen() {
	for {
		conn, err := e.listener.Accept()
		if err != nil {
			e.stop(fmt.Errorf("%w: %w", net.ErrClosed, err))

			return
		}

		if !e.proxy.hold(conn) {
			_ = conn.Close()

			continue
		}

		go e.judge(conn)
	}
}

// judge decides one connection before net/http sees it, and hands it over when net/http should.
func (e *entry) judge(conn net.Conn) {
	switch e.class {
	case entryTLS:
		e.judgeTLS(conn)
	case entryCleartext:
		e.judgeCleartext(conn)
	case entryCatchAll:
		e.judgeCatchAll(conn)
	default:
		e.proxy.drop(conn)
	}
}

// judgeCatchAll applies the catch-all rows. The connection has the silence bound to send its first
// byte; one that closes or stays silent stops the run, naming its port, unless it is exempt — beside a
// served connection on the same port it is a Go client's pool, and at the teardown point it is the
// service going away. A first byte that opens TLS is handed over for a handshake; anything else must be
// an HTTP/1.x request header, as on 80.
func (e *entry) judgeCatchAll(conn net.Conn) {
	tagged, isTagged := conn.(Tagged)
	if !isTagged {
		e.proxy.drop(conn)
		e.proxy.stopOn(errUntagged)

		return
	}

	port := tagged.DestinationPort()
	key := servedKey{port: port, class: entryCatchAll}

	first, silence := e.proxy.catchAllFirstByte(conn, key)
	if first == nil {
		e.proxy.drop(conn)

		if silence != nil {
			e.proxy.stopOn(silence)
		}

		return
	}

	if first[0] == tlsRecord {
		e.handOverTLS(conn, first, port)

		return
	}

	checked, err := checkRequest(conn, first, port)
	if err != nil {
		e.proxy.drop(conn)
		e.proxy.stopOn(asStop(err, port, ""))

		return
	}

	e.proxy.markServed(key)
	e.proxy.release(conn)
	e.handOver(checked)
}

// catchAllFirstByte waits the silence bound for a catch-all connection's first byte. It returns the
// byte; or nil and the stop the connection's silence is; or nil and nil for an exempt connection that
// closed.
func (p *Proxy) catchAllFirstByte(conn net.Conn, key servedKey) ([]byte, *EgressStop) {
	if conn.SetReadDeadline(time.Now().Add(catchAllSilence)) != nil {
		// Only the stub's own close makes this fail, and that says nothing about the client.
		return nil, nil //nolint:nilerr // see above.
	}

	first, err := firstByte(conn)

	switch {
	case err == nil:
		return first, nil
	case !p.exempt(key):
		return nil, silentStop(key.port, err)
	case isTimeout(err):
		return heldFirstByte(conn), nil
	default:
		return nil, nil
	}
}

// heldFirstByte waits with no deadline for an exempt connection's first byte, as on 80: it may yet be
// used. It returns nil when the connection closes instead.
func heldFirstByte(conn net.Conn) []byte {
	if conn.SetReadDeadline(time.Time{}) != nil {
		return nil
	}

	first, err := firstByte(conn)
	if err != nil {
		return nil
	}

	return first
}

// silentStop is the stop a catch-all connection on port is when it sent nothing before err.
func silentStop(port uint16, err error) *EgressStop {
	detail := "closed before sending a byte"
	if isTimeout(err) {
		detail = "sent nothing within " + catchAllSilence.String()
	}

	return &EgressStop{Detail: detail, Class: StopSilent, Port: port}
}

// exempt reports whether a byte-less catch-all connection with key hides nothing: a connection with
// the same key has been served, or the teardown point has passed.
func (p *Proxy) exempt(key servedKey) bool {
	return p.tornDown() || p.wasServed(key)
}

// judgeCleartext applies the cleartext rows. A connection that sends nothing is held with no deadline
// and one that closes before its first byte is ignored: neither carried a request, and a Go client's
// pool leaves both. A first byte that opens TLS stops the run. Anything else must be an HTTP/1.x
// request header within the header bound of its first byte.
func (e *entry) judgeCleartext(conn net.Conn) {
	first, err := firstByte(conn)
	if err != nil {
		e.proxy.drop(conn)

		return
	}

	if first[0] == tlsRecord {
		e.proxy.drop(conn)
		e.proxy.stopOn(&EgressStop{Class: StopTLSOnCleartext, Port: e.port})

		return
	}

	checked, err := checkRequest(conn, first, e.port)
	if err != nil {
		e.proxy.drop(conn)
		e.proxy.stopOn(asStop(err, e.port, ""))

		return
	}

	e.proxy.release(conn)
	e.handOver(checked)
}

// judgeTLS applies the TLS rows before the handshake. A connection that sends nothing is held and one
// that closes first is ignored, as in cleartext; a first byte that does not open TLS stops the run.
// Anything else is handed to net/http for its handshake, which it bounds from about that first byte.
func (e *entry) judgeTLS(conn net.Conn) {
	first, err := firstByte(conn)
	if err != nil {
		e.proxy.drop(conn)

		return
	}

	if first[0] != tlsRecord {
		e.proxy.drop(conn)
		e.proxy.stopOn(&EgressStop{Class: StopCleartextOnTLS, Port: e.port})

		return
	}

	e.handOverTLS(conn, first, e.port)
}

// handOverTLS gives net/http a connection whose first byte opened TLS, replaying that byte, for the
// handshake. The connection stays held until its first request's first byte, so a close still reaches
// it.
func (e *entry) handOverTLS(conn net.Conn, first []byte, port uint16) {
	replaying := &replayConn{Conn: conn, reader: io.MultiReader(bytes.NewReader(first), conn), port: port}

	e.handOver(&tunnel{
		Conn:  tls.Server(replaying, e.proxy.tlsConfig),
		raw:   conn,
		proxy: e.proxy,
		port:  port,
		class: e.class,
	})
}

// MarkTeardown marks the teardown point: the harness has begun removing the service under test. A
// connection that closes after it having sent no request is the service going away, not a client that
// could not talk to the stub — except a catch-all TLS connection that never spoke, which may be a
// server-first client that waited to the end: its wait is cut short here and it stops the run. It
// closes nothing.
func (p *Proxy) MarkTeardown() {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	p.teardown = true

	for waiting := range p.watched {
		//nolint:errcheck // a connection already closed has no wait left to cut short.
		_ = waiting.raw.SetReadDeadline(time.Now())
	}
}

// watch registers a catch-all TLS connection waiting for its first request.
func (p *Proxy) watch(t *tunnel) {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	p.watched[t] = struct{}{}
}

// unwatch forgets a connection watch registered.
func (p *Proxy) unwatch(t *tunnel) {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	delete(p.watched, t)
}

// boundRead sets a watched connection's read deadline: bound, or none while it is held, or at once
// past the teardown point. It is set under the lock MarkTeardown takes, so the mark never lands
// between the check and the deadline.
func (p *Proxy) boundRead(t *tunnel, bound time.Time, held bool) error {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	switch {
	case held:
		bound = time.Time{}
	case p.teardown:
		bound = time.Now()
	default:
	}

	return t.raw.SetReadDeadline(bound) //nolint:wrapcheck // the caller wraps it.
}

// tornDown reports whether the teardown point has passed.
func (p *Proxy) tornDown() bool {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	return p.teardown
}

// handOver gives a judged connection to net/http, or closes it when the entry is closing.
func (e *entry) handOver(conn net.Conn) {
	select {
	case e.ready <- conn:
	case <-e.done:
		_ = conn.Close()
	}
}

// hold registers a connection the stub is judging, or reports false once the stub is closing.
func (p *Proxy) hold(conn net.Conn) bool {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	if p.closing {
		return false
	}

	p.held[conn] = struct{}{}

	return true
}

// release stops tracking a connection: net/http has it now, or it is being dropped.
func (p *Proxy) release(conn net.Conn) {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	delete(p.held, conn)
}

// drop releases a connection and closes it.
func (p *Proxy) drop(conn net.Conn) {
	p.release(conn)

	_ = conn.Close()
}

// closeHeld marks the stub as closing and closes every connection it holds.
func (p *Proxy) closeHeld() {
	p.heldMu.Lock()
	p.closing = true

	held := make([]net.Conn, 0, len(p.held))
	for conn := range p.held {
		held = append(held, conn)
	}

	p.heldMu.Unlock()

	for _, conn := range held {
		_ = conn.Close()
	}
}

// isClosing reports whether the stub has begun to close.
func (p *Proxy) isClosing() bool {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	return p.closing
}

// stopOn stops the run on err, unless the stub is closing: then err is the stub's own close cutting a
// connection short, which says nothing about the client.
func (p *Proxy) stopOn(err error) {
	if !p.isClosing() {
		p.fail(err)
	}
}

// markServed records that a connection with key has sent a request.
func (p *Proxy) markServed(key servedKey) {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	p.served[key] = struct{}{}
}

// wasServed reports whether a connection with key has already sent a request in this run.
func (p *Proxy) wasServed(key servedKey) bool {
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	_, served := p.served[key]

	return served
}

// isTimeout reports whether err is a read deadline passing.
func isTimeout(err error) bool {
	network, isNetwork := errors.AsType[net.Error](err)

	return isNetwork && network.Timeout()
}

// firstByte waits for a connection's first byte.
func firstByte(conn net.Conn) ([]byte, error) {
	first := make([]byte, 1)

	if _, err := io.ReadFull(conn, first); err != nil {
		return nil, err //nolint:wrapcheck // the caller reads only whether a byte came.
	}

	return first, nil
}

// checkRequest admits a connection once its first request header, which starts with first, parses as
// HTTP/1.x within the header bound of that first byte. It hands back a connection that replays what it
// read.
func checkRequest(conn net.Conn, first []byte, port uint16) (net.Conn, error) {
	if err := conn.SetReadDeadline(time.Now().Add(headerBound)); err != nil {
		return nil, fmt.Errorf("bound HTTP header read: %w", err)
	}

	reader := bufio.NewReader(io.MultiReader(bytes.NewReader(first), conn))

	header, err := readRequestHeader(reader)
	if err != nil {
		return nil, errUnparseable
	}

	if bytes.HasPrefix(header, []byte(http2Preface)) {
		return nil, errPreface
	}

	request, err := nethttp.ReadRequest(bufio.NewReader(bytes.NewReader(header)))
	if err != nil || request.ProtoMajor != 1 {
		return nil, errUnparseable
	}

	if request.Method == nethttp.MethodConnect {
		return nil, &connectError{target: request.Host}
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear HTTP header deadline: %w", err)
	}

	return &replayConn{Conn: conn, reader: io.MultiReader(bytes.NewReader(header), reader), port: port}, nil
}
