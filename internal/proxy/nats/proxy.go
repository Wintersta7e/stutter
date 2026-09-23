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
// Only the client-to-server direction becomes effects. Divergence is decided by what the service
// did, not by what it was told, so subscriptions and pings never become effects. The bus side is
// read for exactly two facts that are visible nowhere else: which message was handed over, and
// whether the bus REFUSED a publish. A refused operation changed nothing, and without that fact a
// working idempotency guard reports as a divergence made entirely of its own rejected claim.
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

// Sink receives the effects the proxy observes, and the bus's verdict on the ones it answers.
type Sink interface {
	Record(observed effect.Observation)
	// Reject marks the effect awaiting this correlation as refused, so it decides no verdict.
	Reject(correlation string)
	// Answered forgets a correlation the bus accepted.
	Answered(correlation string)
}

// Acks decides the fate of an acknowledgement the service under test sends.
//
// It exists for the service Stutter does not drive. A containerised consumer pulls from JetStream
// itself and acknowledges for itself, so the driver has no acknowledgement to withhold — the only
// lever left is the wire, where an acknowledgement is an ordinary publish the proxy can swallow.
type Acks interface {
	// Withhold reports whether to drop this acknowledgement instead of forwarding it, which makes
	// the server redeliver exactly as an unacknowledged message would be.
	Withhold(ack Ack) bool
}

// Options are the hooks an observed run needs and a driven run does not.
//
// Their zero value leaves the wire untouched in both directions, which is what a driven service
// requires: there the driver owns delivery, and a proxy that swallowed or watched anything would be
// interfering with a run it does not control.
type Options struct {
	// Acks decides the fate of each acknowledgement the service sends.
	Acks Acks
	// Deliveries is notified of each message the bus hands to the service.
	Deliveries Deliveries
}

// Proxy accepts NATS client connections and forwards them to an upstream server.
type Proxy struct {
	listener net.Listener
	sink     Sink
	opts     Options
	upstream string
	dialer   net.Dialer
	wg       sync.WaitGroup
}

// Listen binds a proxy on addr, forwarding to the NATS server at upstream.
//
// Use "127.0.0.1:0" to let the kernel pick a port and read it back from Addr.
func Listen(ctx context.Context, addr, upstream string, sink Sink) (*Proxy, error) {
	return ListenWith(ctx, addr, upstream, sink, Options{})
}

// ListenWith binds a proxy that also watches deliveries and can withhold acknowledgements, which is
// what driving a service Stutter does not dispatch to requires.
func ListenWith(ctx context.Context, addr, upstream string, sink Sink, opts Options) (*Proxy, error) {
	var config net.ListenConfig

	listener, err := config.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	return &Proxy{listener: listener, upstream: upstream, sink: sink, opts: opts}, nil
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
		awaiting:   make(map[string]struct{}),
		opts:       p.opts,
	}

	if err := current.negotiate(); err != nil {
		if errors.Is(err, errEncrypted) {
			p.sink.Record(effect.Observation{
				Raw:       errEncrypted.Error(),
				Printable: errEncrypted.Error(),
				Kind:      effect.KindNATS,
			})
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
	// awaiting holds the reply inboxes of publishes the bus has not answered yet. The two pumps are
	// separate goroutines, so it is guarded.
	awaiting map[string]struct{}
	opts     Options
	mu       sync.Mutex
}

// expect notes that the bus owes an answer on this inbox.
func (s *session) expect(inbox string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.awaiting[inbox] = struct{}{}
}

// awaited reports whether a message the bus sent is the answer to a publish this session observed,
// consuming the expectation.
func (s *session) awaited(subject string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, owed := s.awaiting[subject]; !owed {
		return false
	}

	delete(s.awaiting, subject)

	return true
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

		if s.withhold(current) {
			continue
		}

		if !s.forward(current.raw) {
			return
		}
	}
}

// pumpServer forwards everything the bus sends, reading it on the way past.
//
// Both run models parse this side now. A driven run used to take a straight copy on the grounds that
// nothing was watching, but something is: an operation the bus REFUSED changed nothing, and the only
// place that refusal is visible is the answer it sends back. Without it a working idempotency guard
// reports as a divergence made entirely of its own refused claim.
//
// Closing the client afterwards releases pumpClient, which is otherwise blocked on a read that a
// disconnected bus will never satisfy.
//
// It mirrors pumpClient, including the rule that matters most: losing sight of the protocol costs
// observation, never the connection. A service cut off mid-run reports as a handler that stopped
// producing effects, which is a far worse answer than a missing window.
func (s *session) pumpServer() {
	defer func() { _ = s.client.Close() }()

	for {
		current, err := readServerFrame(s.fromServer)
		if err != nil {
			if current == nil {
				return
			}

			s.forwardClient(current.raw)

			if errors.Is(err, errDesynced) {
				//nolint:errcheck // a forwarding failure is the connection ending, and it ends here anyway.
				_, _ = io.Copy(s.client, s.fromServer)
			}

			return
		}

		s.judge(current)
		s.noteDelivery(current)

		if !s.forwardClient(current.raw) {
			return
		}
	}
}

// judge reports the bus's answer to a publish this session recorded.
//
// Only a message addressed to an inbox the session is waiting on is an answer, so ordinary traffic
// that happens to carry an error payload is never mistaken for one.
func (s *session) judge(current *frame) {
	if !isDelivery(current.op) || !s.awaited(current.args.subject) {
		return
	}

	if refused(current.body[current.args.headerLen:]) {
		s.sink.Reject(current.args.subject)

		return
	}

	s.sink.Answered(current.args.subject)
}

// noteDelivery reports a message the bus handed over, identified by the subject it will be
// acknowledged on.
//
// A delivery whose reply subject is not an acknowledgement subject is ordinary pub/sub traffic, not
// a consumer delivery, and opening a window for it would attribute effects to a message the service
// was never working on.
func (s *session) noteDelivery(current *frame) {
	if !isDelivery(current.op) {
		return
	}

	ack, parsed := parseAck(current.args.reply, nil)
	if !parsed {
		return
	}

	s.opts.Deliveries.Delivered(Delivery{
		Subject: current.args.subject,
		Payload: current.body[current.args.headerLen:],
		Ack:     ack,
	})
}

// forwardClient writes bytes back to the service, reporting whether the connection is still usable.
func (s *session) forwardClient(raw []byte) bool {
	if len(raw) == 0 {
		return true
	}

	_, err := s.client.Write(raw)

	return err == nil
}

func (s *session) inspect(current *frame) {
	if !isPublish(current.op) {
		return
	}

	// An acknowledgement or a pull request is delivery bookkeeping, not the service's own work.
	// Recording one would put Stutter's own fault injection into the sequence it is comparing.
	if isBookkeeping(current.args.subject) {
		return
	}

	headers := parseHeaders(current.body[:current.args.headerLen])

	text := render(current.args, headers, current.body[current.args.headerLen:])

	// A publish carrying a reply inbox is a request, and the bus will say whether it stored the
	// message. Waiting for that answer is what lets a refused operation be excluded from the
	// comparison; a publish with nowhere to answer is fire-and-forget and stands as recorded.
	correlation := current.args.reply
	if correlation != "" {
		s.expect(correlation)
	}

	// A direct get looks a key up. It is the lookup a dedupe guard repeats on every redelivery.
	_, lookup := splitDirectGet(current.args.subject)

	s.sink.Record(effect.Observation{
		Raw:         text,
		Printable:   text,
		Kind:        effect.KindNATS,
		Correlation: correlation,
		Read:        lookup,
	})
}

// withhold reports whether this frame is an acknowledgement the run has chosen to swallow.
//
// Dropping it is a genuine withheld ack: a JetStream acknowledgement is a fire-and-forget publish,
// so the client cannot tell, and the server redelivers once the deadline passes. That is the only
// lever available against a service that acknowledges for itself rather than being driven.
func (s *session) withhold(current *frame) bool {
	if s.opts.Acks == nil || !isPublish(current.op) || !isAckSubject(current.args.subject) {
		return false
	}

	ack, parsed := parseAck(current.args.subject, current.body[current.args.headerLen:])
	if !parsed {
		return false
	}

	return s.opts.Acks.Withhold(ack)
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
