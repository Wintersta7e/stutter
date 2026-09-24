// Package opaque proxies a TCP dependency whose protocol Stutter does not parse, so that a service
// on something other than Postgres is observed rather than ignored.
//
// It detects divergence and cannot describe it. That is the whole bargain: a handler that talks to
// an unparsed datastore currently produces no effects at all, and a handler with no effects reads as
// idempotent — the most dangerous wrong answer this tool can give. A finding built on these effects
// is reported as not actionable rather than dressed up as one.
//
// FRAMING. Without the protocol there is no message boundary to read, so the boundary used is a
// semantic one: a request is the run of client bytes that ends when the upstream first answers it.
// That is stable between runs, where splitting on TCP reads would not be — read sizes vary and the
// determinism gate would never pass. A pipelined protocol merges requests into one effect, and a
// server that pushes unprompted splits them; both are still deterministic, which is what matters.
//
// There is deliberately no TLS check, unlike the Postgres and NATS proxies. An unparsed protocol may
// legitimately open with any byte, so a handshake cannot be told from payload. Encrypted traffic
// therefore records as bytes that differ every run, which fails the determinism gate — the tool
// refuses to report rather than reporting nothing and calling the handler clean.
package opaque

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/Wintersta7e/stutter/internal/effect"
)

const (
	// copyBuffer is the forwarding chunk size. It bounds a read, never an effect: effects are cut at
	// reply boundaries, not at whatever the kernel happened to hand over.
	copyBuffer = 32 << 10
	// maxPending caps one buffered request so a streaming upload cannot exhaust memory.
	maxPending = 1 << 20
	// deleteByte is DEL, the one control character that sits above the printable range.
	deleteByte = 0x7f
)

var (
	errMissingName     = errors.New("opaque proxy requires a logical dependency name")
	errMissingSink     = errors.New("opaque proxy requires an effect sink")
	errMissingUpstream = errors.New("opaque proxy requires an upstream address")
)

// Sink receives the effects the proxy observes.
type Sink interface {
	Record(observed effect.Observation)
}

// Proxy accepts connections for one unparsed dependency and forwards them to it.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Proxy struct {
	listener net.Listener
	sink     Sink
	// dialFailed is the first upstream dial that failed. It stops the proxy, and Serve reports it.
	dialFailed error
	name       string
	upstream   string
	dialer     net.Dialer
	wg         sync.WaitGroup
	failOnce   sync.Once
	failMu     sync.Mutex
}

// New serves a proxy on a listener the caller supplies, forwarding to the dependency at upstream. The
// proxy binds no socket of its own: the caller's listener is where connections come from.
//
// name is the logical dependency name written into every effect. The listener's port is assigned by
// the kernel and differs between runs, so the name is what makes two runs comparable.
func New(listener net.Listener, name, upstream string, sink Sink) (*Proxy, error) {
	if err := validate(name, upstream, sink); err != nil {
		return nil, err
	}

	return &Proxy{listener: listener, name: name, upstream: upstream, sink: sink}, nil
}

// Listen binds a proxy on addr, forwarding to the dependency at upstream, as New does.
func Listen(ctx context.Context, addr, name, upstream string, sink Sink) (*Proxy, error) {
	if err := validate(name, upstream, sink); err != nil {
		return nil, err
	}

	var config net.ListenConfig

	listener, err := config.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	return New(listener, name, upstream, sink)
}

// validate refuses a proxy that could not attribute or forward anything.
func validate(name, upstream string, sink Sink) error {
	switch {
	case sink == nil:
		return errMissingSink
	case name == "":
		return errMissingName
	case upstream == "":
		return errMissingUpstream
	default:
		return nil
	}
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
// dependency's readiness was the caller's to establish, and a run whose dependency the service cannot
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

// abort closes a connection so its peer reads a reset, never EOF: a clean close would tell the peer the
// other side finished, which is not what happened. A relayed connection aborts itself, which carries
// the reset back through the relay; a connection that can do neither is closed.
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

func (p *Proxy) handle(ctx context.Context, client net.Conn) {
	defer func() { _ = client.Close() }()

	upstream, err := p.dialer.DialContext(ctx, "tcp", p.upstream)
	if err != nil {
		abort(client)

		// A dial abandoned because the run is over is the run ending, not the dependency failing.
		if ctx.Err() == nil {
			p.failDial(fmt.Errorf("dial the upstream %s: %w", p.upstream, err))
		}

		return
	}

	defer func() { _ = upstream.Close() }()

	current := &session{sink: p.sink, name: p.name}

	// A reset on either leg resets both, once: the far end of each must read what the near end did.
	var once sync.Once

	resetBoth := func() {
		once.Do(func() {
			abort(client)
			abort(upstream)
		})
	}

	var pumps sync.WaitGroup

	pumps.Go(func() { pump(upstream, client, current.answered, resetBoth) })
	pump(client, upstream, current.requested, resetBoth)

	// Both directions have ended; the deferred closes finish a connection both peers finished.
	pumps.Wait()

	// A request the dependency never answered is still a request. Emitting it at close keeps a
	// fire-and-forget protocol visible.
	current.flush()
}

// pump forwards src to dst, showing every chunk to observe on the way past, until src ends.
//
// EOF is a half-close, passed on as one: dst's write side is shut and the other direction keeps
// flowing, because a client that shuts its write side still waits for the reply. Any other read error,
// or any write error, is a reset, passed on as one to both legs. Turning either ending into the other
// would hand the service a behaviour its real dependency never had.
func pump(src, dst net.Conn, observe func([]byte), resetBoth func()) {
	buffer := make([]byte, copyBuffer)

	for {
		read, err := src.Read(buffer)
		if read > 0 {
			observe(buffer[:read])

			if _, writeErr := dst.Write(buffer[:read]); writeErr != nil {
				resetBoth()

				return
			}
		}

		if err == nil {
			continue
		}

		if errors.Is(err, io.EOF) && closeWrite(dst) == nil {
			return
		}

		resetBoth()

		return
	}
}

// closeWrite shuts a connection's write side. Both production connections can — a TCP connection on
// the Go-caller path, a relayed connection on the compose path — and one that cannot is closed whole.
func closeWrite(conn net.Conn) error {
	if half, ok := conn.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite() //nolint:wrapcheck // the caller only asks whether it worked.
	}

	return conn.Close() //nolint:wrapcheck // as above.
}

// session accumulates one connection's requests. Both directions touch it, so it is guarded.
type session struct {
	sink    Sink
	name    string
	pending []byte
	mu      sync.Mutex
}

func (s *session) requested(chunk []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pending = append(s.pending, chunk...)

	// Split on an exact byte count, never where a read landed: read sizes vary between runs, so a
	// chunk-boundary split would produce a different effect sequence each time.
	for len(s.pending) >= maxPending {
		s.record(s.pending[:maxPending])
		s.pending = s.pending[maxPending:]
	}
}

// answered closes the current request: the dependency has started replying to it.
func (s *session) answered([]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.emit()
}

func (s *session) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.emit()
}

func (s *session) emit() {
	if len(s.pending) == 0 {
		return
	}

	s.record(s.pending)
	s.pending = nil
}

func (s *session) record(payload []byte) {
	text := render(s.name, payload)
	s.sink.Record(effect.Observation{Raw: text, Printable: text, Kind: effect.KindOpaque})
}

// render assembles the comparable, readable form of one unparsed request.
//
// The readable form and the comparable form are the same text, as they are for Postgres and NATS.
// There is nothing to summarise: without the protocol, the bytes are all the report has. The kind
// is not repeated here — the report already prints it in front of every effect.
func render(name string, payload []byte) string {
	return "dependency=" + name + " payload=" + renderPayload(payload)
}

// renderPayload turns a payload into stable one-line text.
//
// Readable text is kept as it arrived, with whitespace collapsed so that one effect stays one line.
// Keeping it verbatim is what lets provenance substitution work: it matches message-derived values
// by plain string comparison, so encoding a readable payload would hide the fact that an identifier
// came from the message. Anything else is hex-encoded, which is stable but not readable.
func renderPayload(payload []byte) string {
	if !utf8.Valid(payload) || hasControlByte(payload) {
		return "0x" + hex.EncodeToString(payload)
	}

	return strings.Join(strings.Fields(string(payload)), " ")
}

// hasControlByte reports whether a payload carries a control character other than the whitespace
// that collapsing already handles.
func hasControlByte(payload []byte) bool {
	for _, current := range payload {
		if current == '\t' || current == '\n' || current == '\r' {
			continue
		}

		if current < ' ' || current == deleteByte {
			return true
		}
	}

	return false
}
