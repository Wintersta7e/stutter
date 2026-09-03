// Package nats proxies the NATS client protocol, recording every message a service under test
// publishes back to the bus.
//
// It exists because the commonest bus-native idempotency guard is a key/value claim, and on the wire
// a claim is nothing more than a publish to $KV.<bucket>.<key>. A handler whose entire dedupe guard
// is such a claim touches no database at all, so watching Postgres alone makes a clean run and a
// faulted run look identical: the verdict is then reached by luck rather than by observation, and
// "no observed effects" reads as "idempotent".
//
// The proxy forwards bytes untouched and inspects a copy, so a message Stutter cannot parse costs an
// effect but never corrupts the connection. Parsing goes only as far as producing a stable, readable
// line: understanding the message is not required to notice that one run published it twice.
//
// Only the client-to-server direction is parsed. Divergence is decided by what the service did, not
// by what it was told, so subscriptions, pings and everything the bus sends back are forwarded
// without inspection and never become effects.
package nats

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Wintersta7e/stutter/internal/effect"
)

const (
	// readBuffer sizes each connection's buffered reader. Comfortably above the 4096 bytes a server
	// allows a control line by default, so an ordinary line never needs a second pass.
	readBuffer = 32 << 10
	// greeting is how many bytes of the client's first message are needed to tell a CONNECT from a
	// TLS handshake: the record type is the first byte of either.
	greeting = 1
	// tlsRecord opens a TLS handshake record. A client that upgrades sends it where a CONNECT would
	// otherwise be.
	tlsRecord = 0x16
)

// errEncrypted means the client negotiated TLS with the bus, leaving nothing for the proxy to read.
//
// Failing here is deliberate. A silently unreadable connection would report a handler as having no
// side effects at all, which reads as "idempotent" — the most dangerous wrong answer this tool can
// give, and the exact answer this package exists to stop being reached by accident.
var errEncrypted = errors.New("client negotiated TLS with the bus; " +
	"published effects cannot be observed — disable TLS on the sandbox connection")

// Sink receives the effects the proxy observes.
type Sink interface {
	Record(kind effect.Kind, raw, printable string)
}

// Proxy accepts NATS client connections and forwards them to an upstream server.
type Proxy struct {
	listener net.Listener
	sink     Sink
	upstream string
	dialer   net.Dialer
	wg       sync.WaitGroup
}

// Listen binds a proxy on addr, forwarding to the NATS server at upstream.
//
// Use "127.0.0.1:0" to let the kernel pick a port and read it back from Addr.
func Listen(ctx context.Context, addr, upstream string, sink Sink) (*Proxy, error) {
	var config net.ListenConfig

	listener, err := config.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	return &Proxy{listener: listener, upstream: upstream, sink: sink}, nil
}

// Addr is the address the proxy is listening on.
func (p *Proxy) Addr() string {
	return p.listener.Addr().String()
}

// Serve accepts connections until the proxy is closed.
func (p *Proxy) Serve(ctx context.Context) error {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				//nolint:nilerr // a closed listener is how Serve is stopped, not a failure.
				return nil
			}

			return fmt.Errorf("accept: %w", err)
		}

		p.wg.Go(func() { p.handle(ctx, client) })
	}
}

// Close stops accepting and waits for in-flight connections to finish.
func (p *Proxy) Close() error {
	if err := p.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("close listener: %w", err)
	}

	p.wg.Wait()

	return nil
}

func (p *Proxy) handle(ctx context.Context, client net.Conn) {
	defer func() { _ = client.Close() }()

	upstream, err := p.dialer.DialContext(ctx, "tcp", p.upstream)
	if err != nil {
		return
	}

	defer func() { _ = upstream.Close() }()

	current := &session{
		client:     client,
		upstream:   upstream,
		fromClient: bufio.NewReaderSize(client, readBuffer),
		fromServer: bufio.NewReaderSize(upstream, readBuffer),
		sink:       p.sink,
	}

	if err := current.negotiate(); err != nil {
		if errors.Is(err, errEncrypted) {
			p.sink.Record(effect.KindNATS, errEncrypted.Error(), errEncrypted.Error())
		}

		return
	}

	served := make(chan struct{})

	go func() {
		defer close(served)

		current.pumpServer()
	}()

	current.pumpClient()

	// The client side is finished, so the bus has nothing left to deliver. Closing upstream here
	// rather than leaving it to the deferred close is what lets Close wait on every connection
	// instead of hanging on one whose server side is still blocked on a read.
	_ = upstream.Close()

	<-served
}

// session is one client connection and its upstream counterpart.
type session struct {
	client     net.Conn
	upstream   net.Conn
	fromClient *bufio.Reader
	fromServer *bufio.Reader
	sink       Sink
}

// negotiate forwards the server's opening INFO and confirms the connection will stay readable.
//
// The server speaks first, and a client that is told to upgrade does so immediately: what follows is
// a TLS handshake rather than a CONNECT. Both signs are checked, and both are fatal — see
// errEncrypted for why refusing beats reporting a handler that appears to do nothing.
func (s *session) negotiate() error {
	info, err := readLine(s.fromServer)
	if err != nil {
		return err
	}

	if _, writeErr := s.client.Write(info); writeErr != nil {
		return fmt.Errorf("forward server info: %w", writeErr)
	}

	if infoRequiresTLS(info) {
		return errEncrypted
	}

	first, err := s.fromClient.Peek(greeting)
	if err != nil {
		return fmt.Errorf("read client greeting: %w", err)
	}

	if first[0] == tlsRecord {
		return errEncrypted
	}

	return nil
}

// pumpClient forwards client messages, inspecting a copy of each.
func (s *session) pumpClient() {
	for {
		current, err := readFrame(s.fromClient)
		if err != nil {
			if current == nil {
				return
			}

			s.forward(current.raw)

			if errors.Is(err, errDesynced) {
				// The next control line can no longer be located, so no further effect can be
				// attributed on this connection. The bytes keep flowing regardless: losing an effect
				// is recoverable, and cutting off a service under test is not.
				//nolint:errcheck // a forwarding failure is the connection ending, and it ends here anyway.
				_, _ = io.Copy(s.upstream, s.fromClient)
			}

			return
		}

		s.inspect(current)

		if !s.forward(current.raw) {
			return
		}
	}
}

// pumpServer forwards everything the bus sends, uninspected.
//
// Closing the client afterwards releases pumpClient, which is otherwise blocked on a read that a
// disconnected bus will never satisfy.
func (s *session) pumpServer() {
	//nolint:errcheck // a forwarding failure is the connection ending, which is what happens next.
	_, _ = io.Copy(s.client, s.fromServer)

	_ = s.client.Close()
}

func (s *session) inspect(current *frame) {
	if !isPublish(current.op) {
		return
	}

	headers := parseHeaders(current.body[:current.args.headerLen])

	text := render(current.args, headers, current.body[current.args.headerLen:])

	s.sink.Record(effect.KindNATS, text, text)
}

// forward writes bytes on to the upstream, reporting whether the connection is still usable.
func (s *session) forward(raw []byte) bool {
	if len(raw) == 0 {
		return true
	}

	_, err := s.upstream.Write(raw)

	return err == nil
}

// infoRequiresTLS reports whether the server's INFO tells the client to upgrade.
//
// An unreadable INFO is treated as not requiring TLS: the client's own first byte is checked next
// and settles the question either way, so guessing here would only turn a parse failure into a
// refused connection.
func infoRequiresTLS(line []byte) bool {
	_, options, found := bytes.Cut(line, []byte(" "))
	if !found {
		return false
	}

	var info struct {
		TLSRequired bool `json:"tls_required"`
	}

	if err := json.Unmarshal(bytes.TrimSpace(options), &info); err != nil {
		return false
	}

	return info.TLSRequired
}
