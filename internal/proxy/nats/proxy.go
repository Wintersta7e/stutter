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
	"strings"
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
	// jsPrefix opens every JetStream subject.
	jsPrefix = "$JS."
	// statusNoResponders is the header status the bus answers a request with when nothing is
	// subscribed to its subject.
	statusNoResponders = "503"
)

// Sink receives the effects the proxy observes, and the bus's verdict on the ones it answers.
type Sink interface {
	Record(observed effect.Observation)
	// Reject marks the effect awaiting this correlation as refused, so it decides no verdict.
	Reject(correlation string)
	// Answered forgets a correlation the bus accepted.
	Answered(correlation string)
	// Declined carries what the bus said when it refused a JetStream API request: before the first
	// delivery there is no effect to mark, and this is the only record of why a service never started.
	Declined(refusal effect.Refusal)
	// NoResponder counts a request nothing answered. The request stays an effect.
	NoResponder()
	// ClosedAfterInfo counts a client that hung up after the greeting without sending a byte.
	ClosedAfterInfo()
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
	// Pulls is told of each pull request the service makes. Nil is never called.
	Pulls Pulls
	// Hold keeps what the bus sends waiting while it is on. Nil never holds.
	Hold *Hold
	// Requests is told of each JetStream API request the service sends. Nil is never called.
	Requests Requests
}

// Proxy accepts NATS client connections and forwards them to an upstream server.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Proxy struct {
	listener net.Listener
	sink     Sink
	// failure is the first bus client the embedded server cannot stand in for. It stops the proxy,
	// and Serve reports it.
	failure  error
	opts     Options
	upstream string
	dialer   net.Dialer
	wg       sync.WaitGroup
	failOnce sync.Once
	failed   sync.Mutex
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

	return New(listener, upstream, sink, opts), nil
}

// New serves a proxy on a listener the caller supplies, forwarding to the NATS server at upstream, as
// ListenWith does. The proxy binds no socket of its own: the caller's listener is where connections
// come from.
func New(listener net.Listener, upstream string, sink Sink, opts Options) *Proxy {
	return &Proxy{listener: listener, upstream: upstream, sink: sink, opts: opts}
}

// Addr is the address the proxy is listening on.
func (p *Proxy) Addr() string {
	return p.listener.Addr().String()
}

// Serve accepts connections until the proxy is closed, or until a bus client the embedded server
// cannot stand in for stops it — that failure is what Serve then returns.
func (p *Proxy) Serve(ctx context.Context) error {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			// A stop outranks the closure it caused: closing the listener is how fail stops the proxy,
			// so the closed listener would otherwise read as an orderly stop.
			if failure := p.serveFailure(); failure != nil {
				return failure
			}

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

// fail records the first unsupported bus client and stops accepting, so Serve reports it. Connections
// already open keep flowing: the run is over, but its service still has to be shut down cleanly.
func (p *Proxy) fail(err error) {
	p.failOnce.Do(func() {
		p.failed.Lock()
		p.failure = err
		p.failed.Unlock()

		_ = p.listener.Close()
	})
}

// serveFailure is the failure fail recorded, if any.
func (p *Proxy) serveFailure() error {
	p.failed.Lock()
	defer p.failed.Unlock()

	return p.failure
}

// dialUpstream dials the bus for one client. The bus is Stutter's own server, so a refused dial is a
// real failure: it resets the client and stops the run. One abandoned because the run is over is the
// run ending.
func (p *Proxy) dialUpstream(ctx context.Context, client net.Conn) (net.Conn, bool) {
	upstream, err := p.dialer.DialContext(ctx, "tcp", p.upstream)
	if err == nil {
		return upstream, true
	}

	abort(client)

	if ctx.Err() == nil {
		p.fail(fmt.Errorf("dial the upstream %s: %w", p.upstream, err))
	}

	return nil, false
}

func (p *Proxy) handle(ctx context.Context, client net.Conn) {
	defer func() { _ = client.Close() }()

	upstream, dialled := p.dialUpstream(ctx, client)
	if !dialled {
		return
	}

	defer func() { _ = upstream.Close() }()

	current := &session{
		client:     client,
		upstream:   upstream,
		fromClient: bufio.NewReaderSize(client, readBuffer),
		fromServer: bufio.NewReaderSize(upstream, readBuffer),
		sink:       p.sink,
		awaiting:   make(map[string]string),
		opts:       p.opts,
		fail:       p.fail,
	}

	// An unreadable connection is never recorded as anything: it would report a handler as having no
	// side effects at all, which reads as "idempotent" — the most dangerous wrong answer this tool can
	// give. It stops the run instead.
	if err := current.negotiate(); err != nil {
		if errors.Is(err, ErrUnsupportedBus) {
			p.fail(err)
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

// abort closes a client so it reads a reset, never EOF: the bus could not be reached, and a clean close
// would tell the client the bus hung up on it. A relayed connection aborts itself, which carries the
// reset back through the relay.
func abort(conn net.Conn) {
	if aborter, ok := conn.(interface{ Abort() error }); ok {
		_ = aborter.Abort() //nolint:errcheck // the connection is being refused either way.

		return
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0) //nolint:errcheck // a failed linger leaves a clean close, still a refusal.
	}

	_ = conn.Close()
}

// session is one client connection and its upstream counterpart.
type session struct {
	client     net.Conn
	upstream   net.Conn
	fromClient *bufio.Reader
	fromServer *bufio.Reader
	sink       Sink
	// fail stops the proxy on a bus client the embedded server cannot stand in for.
	fail func(err error)
	// awaiting maps the reply inbox of each publish the bus has not answered yet to the subject it
	// was published to. The two pumps are separate goroutines, so it is guarded.
	awaiting map[string]string
	opts     Options
	mu       sync.Mutex
}

// expect notes that the bus owes an answer on this inbox to a publish on subject.
func (s *session) expect(inbox, subject string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.awaiting[inbox] = subject
}

// awaited reports whether a message the bus sent is the answer to a publish this session observed,
// and the subject that publish went to, consuming the expectation.
func (s *session) awaited(inbox string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	request, owed := s.awaiting[inbox]
	if !owed {
		return "", false
	}

	delete(s.awaiting, inbox)

	return request, true
}

// negotiate forwards the server's opening INFO and confirms the connection will stay readable.
//
// The server speaks first, and a client that is told to upgrade does so immediately: what follows is
// a TLS handshake rather than a CONNECT. Both signs are checked, and both stop the run: nothing on
// such a connection could be observed.
func (s *session) negotiate() error {
	info, err := readLine(s.fromServer)
	if err != nil {
		return err
	}

	if _, writeErr := s.client.Write(info); writeErr != nil {
		return fmt.Errorf("forward server info: %w", writeErr)
	}

	if infoRequiresTLS(info) {
		return fmt.Errorf("%w: a TLS-first bus client (the bus requires TLS)", ErrUnsupportedBus)
	}

	first, err := s.fromClient.Peek(greeting)
	if err != nil {
		// Hanging up after the greeting is what a client that requires TLS does, and what a script
		// waiting for the port does: the same bytes, so it is counted and never stops anything.
		if s.fromClient.Buffered() == 0 {
			s.sink.ClosedAfterInfo()
		}

		return fmt.Errorf("read client greeting: %w", err)
	}

	if first[0] == tlsRecord {
		return fmt.Errorf("%w: a TLS-first bus client (it opened with a TLS handshake)", ErrUnsupportedBus)
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

			s.opts.Hold.wait()
			s.forwardClient(current.raw)

			if errors.Is(err, errDesynced) {
				//nolint:errcheck // a forwarding failure is the connection ending, and it ends here anyway.
				_, _ = io.Copy(s.client, s.fromServer)
			}

			return
		}

		// Noted as it is read, forwarded once any hold is released: a delivery's window is open before
		// the service can act on it, however long the hold.
		// An answer to one of the service's own requests is nobody's delivery.
		if !s.judge(current) {
			s.noteDelivery(current)
		}

		s.opts.Hold.wait()

		if !s.forwardClient(current.raw) {
			return
		}
	}
}

// judge reports the bus's answer to a publish this session recorded.
//
// Only a message addressed to an inbox the session is waiting on is an answer, so ordinary traffic
// that happens to carry an error payload is never mistaken for one. A "no responders" status is not a
// refusal — nothing was there to refuse — so the request stays an effect and the answer is counted.
// It reports whether the message was such an answer.
func (s *session) judge(current *frame) bool {
	if !isDelivery(current.op) {
		return false
	}

	request, owed := s.awaited(current.args.subject)
	if !owed {
		return false
	}

	if noResponders(current.body[:current.args.headerLen]) {
		s.sink.NoResponder()
		s.sink.Answered(current.args.subject)

		return true
	}

	answer, declined := apiRefusal(current.body[current.args.headerLen:])
	if !declined {
		s.sink.Answered(current.args.subject)

		return true
	}

	s.sink.Reject(current.args.subject)

	if answer.ErrCode == errCodeReplicas {
		s.fail(fmt.Errorf("%w: the bus refused %s, which asks for more replicas than one server has (err_code %d)",
			ErrUnsupportedBus, refusedReplicas(request), errCodeReplicas))
	}

	if strings.HasPrefix(request, jsPrefix) {
		s.sink.Declined(effect.Refusal{
			Subject:     request,
			Description: answer.Description,
			Code:        answer.Code,
			ErrCode:     answer.ErrCode,
		})
	}

	return true
}

// noteDelivery reports a message the bus handed over, identified by the subject it will be
// acknowledged on.
//
// A delivery whose reply subject is not an acknowledgement subject is ordinary pub/sub traffic, not
// a consumer delivery, and opening a window for it would attribute effects to a message the service
// was never working on. It is reported as a core delivery instead: a core subscriber on a corpus
// subject is handed the corpus too.
func (s *session) noteDelivery(current *frame) {
	if !isDelivery(current.op) || s.opts.Deliveries == nil {
		return
	}

	ack, parsed := parseAck(current.args.reply, nil)
	if !parsed {
		s.opts.Deliveries.CoreDelivered(current.args.subject)

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

	// The one embedded server has no domain: every request to one goes unanswered, and the service
	// never starts. The frame is still forwarded, so the service sees what it would have seen.
	if domain, addressed := jetStreamDomain(current.args.subject); addressed {
		s.fail(fmt.Errorf("%w: a request to JetStream domain %s, and the embedded bus has none",
			ErrUnsupportedBus, domain))
	}

	// A pull request's timers bound how long deliveries may be held; it stays bookkeeping.
	if s.opts.Pulls != nil {
		if pull, isPull := parsePull(current.args.subject, current.body[current.args.headerLen:]); isPull {
			s.opts.Pulls.Pulled(pull)
		}
	}

	// Reported before the bookkeeping return: a pull request is the service asking JetStream too.
	s.noteRequest(current.args.subject)

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
		s.expect(correlation, current.args.subject)
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

// noResponders reports whether a header block carries the "no responders" status: nothing was
// subscribed to answer the request.
func noResponders(block []byte) bool {
	line, _, _ := bytes.Cut(block, []byte(crlf))
	fields := strings.Fields(string(line))

	return len(fields) > 1 && fields[0] == headerVersion && fields[1] == statusNoResponders
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
