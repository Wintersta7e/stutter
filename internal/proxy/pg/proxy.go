// Package pg proxies the PostgreSQL wire protocol, recording every statement a service under test
// sends to its database.
//
// The proxy forwards bytes untouched and inspects a copy, so a message Stutter cannot parse costs
// an effect but never corrupts the connection. Parsing goes only as far as producing a stable,
// readable statement: understanding the query is not required to notice that one run issued it
// twice.
//
// Both directions are inspected, for one specific reason. Clients routinely send Parse with no
// parameter types and let the server infer them, so the frontend stream alone cannot say whether a
// binary parameter is a timestamp or a bigint. The resolved types arrive in the backend's
// ParameterDescription, and without them a wall-clock stamp stays an opaque blob that the
// normaliser cannot see — which shows up as a determinism gate that can never pass.
package pg

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/Wintersta7e/stutter/internal/effect"
)

const (
	// maxMessageSize caps a single protocol message. Postgres' own limit is 1GB; anything
	// approaching it in a replay sandbox is a malformed stream, not a large query.
	maxMessageSize = 64 << 20
	// lengthWidth is the 4-byte big-endian length field every message carries.
	lengthWidth = 4
	// typedHeaderSize is one type byte followed by the length field.
	typedHeaderSize = lengthWidth + 1
)

// errEncrypted means the client negotiated TLS, leaving nothing for the proxy to read.
//
// Failing here is deliberate. A silently unparsed connection would report a handler as having no
// side effects at all, which reads as "idempotent" — the most dangerous wrong answer this tool can
// give.
var errEncrypted = errors.New("client negotiated TLS with the database; " +
	"effects cannot be observed — disable TLS on the sandbox connection")

// Sink receives the effects the proxy observes, and what the database made of each.
type Sink interface {
	Record(observed effect.Observation)
	// Reject marks the recorded statement as having changed nothing.
	Reject(correlation string)
	// Answered releases a recorded statement that did change something.
	Answered(correlation string)
}

// Proxy accepts Postgres connections and forwards them to an upstream server.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Proxy struct {
	listener net.Listener
	sink     Sink
	// dialFailed is the first upstream dial that failed. It stops the proxy, and Serve reports it.
	dialFailed error
	upstream   string
	dialer     net.Dialer
	wg         sync.WaitGroup
	// sessions numbers connections, so a statement's correlation token is unique across all of them.
	sessions atomic.Uint64
	failOnce sync.Once
	failMu   sync.Mutex
}

// New serves a proxy on a listener the caller supplies, forwarding to the Postgres server at upstream.
// The proxy binds no socket of its own: the caller's listener is where connections come from.
func New(listener net.Listener, upstream string, sink Sink) *Proxy {
	return &Proxy{listener: listener, upstream: upstream, sink: sink}
}

// Listen binds a proxy on addr, forwarding to the Postgres server at upstream.
//
// Use "127.0.0.1:0" to let the kernel pick a port and read it back from Addr.
func Listen(ctx context.Context, addr, upstream string, sink Sink) (*Proxy, error) {
	var config net.ListenConfig

	listener, err := config.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	return New(listener, upstream, sink), nil
}

// Addr is the address the proxy is listening on.
func (p *Proxy) Addr() string {
	return p.listener.Addr().String()
}

// Serve accepts connections until the proxy is closed, or until an upstream dial fails — that failure
// is what Serve then returns.
func (p *Proxy) Serve(ctx context.Context) error {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			// A dial failure outranks the closure it caused.
			if failure := p.dialFailure(); failure != nil {
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

// failDial records the first upstream dial that failed and stops accepting. There is no retry: the
// database's readiness was the caller's to establish, and a run whose database the service cannot
// reach reports a handler that did nothing.
func (p *Proxy) failDial(err error) {
	p.failOnce.Do(func() {
		p.failMu.Lock()
		p.dialFailed = err
		p.failMu.Unlock()

		_ = p.listener.Close()
	})
}

func (p *Proxy) dialFailure() error {
	p.failMu.Lock()
	defer p.failMu.Unlock()

	return p.dialFailed
}

func (p *Proxy) handle(ctx context.Context, client net.Conn) {
	defer func() { _ = client.Close() }()

	upstream, err := p.dialer.DialContext(ctx, "tcp", p.upstream)
	if err != nil {
		abort(client)

		// A dial abandoned because the run is over is the run ending, not the database failing.
		if ctx.Err() == nil {
			p.failDial(fmt.Errorf("dial the upstream %s: %w", p.upstream, err))
		}

		return
	}

	defer func() { _ = upstream.Close() }()

	current := &session{
		client:      client,
		upstream:    upstream,
		sink:        p.sink,
		completions: &completions{sink: p.sink},
		statements:  make(map[string]statement),
		portals:     make(map[string]portal),
		id:          p.sessions.Add(1),
	}

	if err := current.negotiate(); err != nil {
		if errors.Is(err, errEncrypted) {
			p.sink.Record(effect.Observation{
				Raw:       errEncrypted.Error(),
				Printable: errEncrypted.Error(),
				Kind:      effect.KindPostgres,
			})
		}

		return
	}

	go current.pumpBackend()

	current.pumpFrontend()
}

// session is one client connection and its upstream counterpart.
//
// The two pumps run concurrently and both touch the statement map, so it is guarded.
type session struct {
	client      net.Conn
	upstream    net.Conn
	sink        Sink
	completions *completions
	statements  map[string]statement
	portals     map[string]portal
	describing  string
	// id and issued make each recorded statement's correlation token. Only the frontend pump issues
	// tokens, so issued needs no guard.
	id     uint64
	issued uint64
	mu     sync.Mutex
}

// negotiate forwards the untyped startup exchange that precedes the typed message stream.
//
// The single-byte reply to an SSL or GSSAPI request is read here rather than by the backend pump,
// because that pump must not start until the stream shape is settled.
func (s *session) negotiate() error {
	for {
		body, err := s.forwardUntyped()
		if err != nil {
			return err
		}

		if !isEncryptionRequest(body) {
			return nil
		}

		reply := make([]byte, 1)
		if _, readErr := io.ReadFull(s.upstream, reply); readErr != nil {
			return fmt.Errorf("read encryption reply: %w", readErr)
		}

		if _, writeErr := s.client.Write(reply); writeErr != nil {
			return fmt.Errorf("forward encryption reply: %w", writeErr)
		}

		if reply[0] == 'S' {
			return errEncrypted
		}
	}
}

// pumpFrontend forwards client messages, inspecting a copy of each.
func (s *session) pumpFrontend() {
	for {
		msgType, body, err := readTyped(s.client)
		if err != nil {
			return
		}

		s.inspectFrontend(msgType, body)

		if err := writeTyped(s.upstream, msgType, body); err != nil {
			return
		}
	}
}

// pumpBackend forwards server messages, inspecting ParameterDescription and the answers that settle
// a statement.
//
// Nothing here becomes an effect. Divergence is decided by what the service asked for; the answer is
// read only to learn a parameter's type, and whether a statement changed anything at all.
func (s *session) pumpBackend() {
	for {
		msgType, body, err := readTyped(s.upstream)
		if err != nil {
			return
		}

		if msgType == msgParameterDescription {
			s.attachParameterTypes(body)
		}

		s.completions.settle(msgType, body)

		if err := writeTyped(s.client, msgType, body); err != nil {
			return
		}
	}
}

// inspectFrontend records any statement a client message carries, and queues every message the
// server will answer — recorded or not, since an unqueued request would shift every pairing after it.
func (s *session) inspectFrontend(msgType byte, body []byte) {
	switch msgType {
	case msgQuery:
		sql, _ := (&reader{buf: body}).cstring()
		s.completions.expect(requestSimple, s.emit(sql, nil), sql)
	case msgParse:
		s.recordParse(body)
	case msgDescribe:
		s.recordDescribe(body)
	case msgBind:
		s.recordBind(body)
	case msgExecute:
		s.execute(body)
	case msgSync:
		s.completions.expect(requestSync, "", "")
	case msgFunctionCall:
		s.completions.expect(requestCall, "", "")
	default:
		// Every other frontend message is forwarded uninspected. Close, Flush and Terminate carry no
		// statement, and a message Stutter does not understand must not become an effect.
	}
}

func (s *session) recordParse(body []byte) {
	name, parsed, ok := parseStatement(body)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.statements[name] = parsed
}

// recordDescribe remembers which statement the server is about to describe, so the reply can be
// matched to it. The extended protocol is strictly request-ordered, so the most recent Describe is
// the one a ParameterDescription answers.
func (s *session) recordDescribe(body []byte) {
	name, ok := parseDescribe(body)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.describing = name
}

func (s *session) attachParameterTypes(body []byte) {
	oids, ok := parseParameterDescription(body)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	prepared, found := s.statements[s.describing]
	if !found {
		return
	}

	prepared.oids = oids
	s.statements[s.describing] = prepared
}

func (s *session) recordBind(body []byte) {
	name, bound, ok := parseBind(body)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.portals[name] = bound
}

// execute records the statement an Execute runs and queues it for its answer. An unknown portal
// records nothing but is queued all the same: the server answers it either way.
func (s *session) execute(body []byte) {
	name, _ := (&reader{buf: body}).cstring()

	s.mu.Lock()

	bound, hasPortal := s.portals[name]
	prepared, hasStatement := s.statements[bound.statement]

	s.mu.Unlock()

	if !hasPortal || !hasStatement {
		s.completions.expect(requestExecute, "", "")

		return
	}

	token := s.emit(prepared.sql, renderParams(bound.params, prepared.oids))
	s.completions.expect(requestExecute, token, prepared.sql)
}

// emit records one statement and returns the token its answer will be matched by, or empty when
// there was no statement to record.
func (s *session) emit(sql string, params []string) string {
	if sql == "" {
		return ""
	}

	s.issued++
	token := "pg/" + strconv.FormatUint(s.id, 10) + "/" + strconv.FormatUint(s.issued, 10)

	text := render(sql, params)
	s.sink.Record(effect.Observation{
		Raw:         text,
		Printable:   text,
		Kind:        effect.KindPostgres,
		Correlation: token,
		Read:        isRead(sql),
	})

	return token
}

// isRead reports whether a statement asks for data rather than changing it. Only the plain forms
// qualify: a CTE can write, so WITH is not a read however it ends.
func isRead(sql string) bool {
	switch leadingVerb(sql) {
	case "SELECT", "SHOW":
		return true
	default:
		return false
	}
}

func (s *session) forwardUntyped() ([]byte, error) {
	header := make([]byte, lengthWidth)
	if _, err := io.ReadFull(s.client, header); err != nil {
		return nil, fmt.Errorf("read startup header: %w", err)
	}

	length := binary.BigEndian.Uint32(header)
	if length < lengthWidth || length > maxMessageSize {
		return nil, fmt.Errorf("startup message length %d out of range: %w", length, io.ErrUnexpectedEOF)
	}

	body := make([]byte, length-lengthWidth)
	if _, err := io.ReadFull(s.client, body); err != nil {
		return nil, fmt.Errorf("read startup body: %w", err)
	}

	out := make([]byte, 0, lengthWidth+len(body))
	out = append(out, header...)
	out = append(out, body...)

	if _, err := s.upstream.Write(out); err != nil {
		return nil, fmt.Errorf("forward startup message: %w", err)
	}

	return body, nil
}

func readTyped(from io.Reader) (byte, []byte, error) {
	header := make([]byte, typedHeaderSize)
	if _, err := io.ReadFull(from, header); err != nil {
		return 0, nil, fmt.Errorf("read message header: %w", err)
	}

	length := binary.BigEndian.Uint32(header[1:])
	if length < lengthWidth || length > maxMessageSize {
		return 0, nil, fmt.Errorf("message length %d out of range: %w", length, io.ErrUnexpectedEOF)
	}

	body := make([]byte, length-lengthWidth)
	if _, err := io.ReadFull(from, body); err != nil {
		return 0, nil, fmt.Errorf("read message body: %w", err)
	}

	return header[0], body, nil
}

func writeTyped(to io.Writer, msgType byte, body []byte) error {
	if len(body) > maxMessageSize {
		return fmt.Errorf("message body %d exceeds the cap: %w", len(body), io.ErrUnexpectedEOF)
	}

	out := make([]byte, 0, typedHeaderSize+len(body))
	out = append(out, msgType)
	// Bounded by the check above and by readTyped, the only producer of these bodies.
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)+lengthWidth)) //nolint:gosec // bounded above.
	out = append(out, body...)

	if _, err := to.Write(out); err != nil {
		return fmt.Errorf("forward message: %w", err)
	}

	return nil
}

// abort closes a client so it reads a reset, never EOF: the upstream could not be reached, and a clean
// close would tell the client the database hung up on it. A relayed connection aborts itself, which
// carries the reset back through the relay.
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

// isEncryptionRequest reports whether a startup body is an SSL or GSSAPI request rather than a real
// startup message.
func isEncryptionRequest(body []byte) bool {
	if len(body) != lengthWidth {
		return false
	}

	code := binary.BigEndian.Uint32(body)

	return code == sslRequest || code == gssEncRequest
}
