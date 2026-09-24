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

// Entries are the listeners one run's stub serves: cleartext HTTP, TLS, or both. Every connection
// they accept records into the one sink and replies from the one script.
type Entries struct {
	// Cleartext is the entry a service dials on port 80.
	Cleartext net.Listener
	// TLS is the entry a service dials on port 443.
	TLS net.Listener
}

// entryClass is which of the stub's entries a connection arrived on.
type entryClass uint8

const (
	entryCleartext entryClass = iota + 1
	entryTLS
)

var (
	errNoEntries      = errors.New("HTTP stub requires at least one entry")
	errNoCertificates = errors.New("HTTP stub serving TLS requires a certificate source")
)

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
		e.proxy.release(conn)
		e.handOver(&tunnel{Conn: tls.Server(conn, e.proxy.tlsConfig), proxy: e.proxy, port: e.port})
	case entryCleartext:
		e.judgeCleartext(conn)
	default:
		e.proxy.drop(conn)
	}
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
