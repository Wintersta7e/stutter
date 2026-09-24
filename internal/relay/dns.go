package relay

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

const (
	// tcpIdle is how long a DNS-over-TCP connection may stay idle after its last complete query. It
	// bounds nothing on UDP.
	tcpIdle = 10 * time.Second
	// udpMessage is the largest DNS message a UDP read takes.
	udpMessage = 1 << 16
	// mxPreference is the preference of the one exchange an MX answer names.
	mxPreference = 10
	// dnsLengthSize is DNS-over-TCP's message length prefix.
	dnsLengthSize = 2
)

// errDNS means the responder's sockets failed.
var errDNS = errors.New("the DNS responder failed")

// response is one answered query: the reply to send, and whether the query is signalled first.
type response struct {
	reply  []byte
	record Query
	signal bool
}

// answer is the responder's whole policy, as a pure function of the query and the stub relay's own
// address. It reports false for a query that does not parse, which is dropped.
//
// Every name gets an answer and none is refused: A is the stub relay itself, MX names the queried name,
// and every other type is NODATA. Never NXDOMAIN — a service that gives up on a name is a service
// whose egress Stutter never sees. A query whose type is neither A nor AAAA is signalled, so the
// harness can disclose it and stop on SRV.
func answer(query []byte, self netip.Addr) (response, bool) {
	var parser dnsmessage.Parser

	header, err := parser.Start(query)
	if err != nil {
		return response{}, false
	}

	questions, err := parser.AllQuestions()
	if err != nil {
		return response{}, false
	}

	rcode := rcodeFor(header, questions)

	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID: header.ID, Response: true, OpCode: header.OpCode, Authoritative: true,
		RecursionDesired: header.RecursionDesired, RecursionAvailable: true, RCode: rcode,
	})
	builder.EnableCompression()

	err = builder.StartQuestions()
	for _, question := range questions {
		err = errors.Join(err, builder.Question(question))
	}

	var answered response

	// A class other than IN is answered NOERROR with nothing: it names no host the service could dial.
	if rcode == dnsmessage.RCodeSuccess && questions[0].Class == dnsmessage.ClassINET {
		var answerErr error

		answered, answerErr = answerQuestion(&builder, questions[0], self)
		err = errors.Join(err, answerErr)
	}

	reply, finishErr := builder.Finish()
	if err = errors.Join(err, finishErr); err != nil {
		return response{}, false
	}

	answered.reply = reply

	return answered, true
}

// rcodeFor is the reply code for a query's shape: an opcode other than QUERY is not implemented, and a
// query is exactly one question.
func rcodeFor(header dnsmessage.Header, questions []dnsmessage.Question) dnsmessage.RCode {
	switch {
	case header.OpCode != 0:
		return dnsmessage.RCodeNotImplemented
	case len(questions) != 1:
		return dnsmessage.RCodeFormatError
	default:
		return dnsmessage.RCodeSuccess
	}
}

// answerQuestion writes the answer to one class-IN question.
func answerQuestion(builder *dnsmessage.Builder, question dnsmessage.Question, self netip.Addr) (response, error) {
	if err := builder.StartAnswers(); err != nil {
		return response{}, fmt.Errorf("start the answers: %w", err)
	}

	head := dnsmessage.ResourceHeader{Name: question.Name, Class: dnsmessage.ClassINET}

	var err error

	//nolint:exhaustive // every type but these two is answered NODATA, which is the default case.
	switch question.Type {
	case dnsmessage.TypeA:
		err = builder.AResource(head, dnsmessage.AResource{A: self.As4()})
	case dnsmessage.TypeMX:
		err = builder.MXResource(head, dnsmessage.MXResource{Pref: mxPreference, MX: question.Name})
	default:
		// NODATA: the name exists, with no record of this type.
	}

	if err != nil {
		return response{}, fmt.Errorf("write the answer: %w", err)
	}

	if question.Type == dnsmessage.TypeA || question.Type == dnsmessage.TypeAAAA {
		return response{}, nil
	}

	return response{record: Query{Name: question.Name.String(), Type: uint16(question.Type)}, signal: true}, nil
}

// signalWriter is the stub relay's one connection to the harness's signal listener, shared by the UDP
// and TCP responders.
type signalWriter struct {
	w  io.Writer
	mu sync.Mutex
}

func (s *signalWriter) write(q Query) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return WriteQuery(s.w, q)
}

// respond answers one query, writing its signal record first. A record that cannot be written ends
// the relay and the query goes unanswered: an answer the harness never heard of could let an SRV
// lookup through unseen.
func (s *server) respond(query []byte) ([]byte, bool) {
	answered, ok := answer(query, s.self)
	if !ok {
		return nil, false
	}

	if answered.signal {
		if err := s.signal.write(answered.record); err != nil {
			s.fail(fmt.Errorf("write to the signal connection %s: %w", s.spec.Signal, err))

			return nil, false
		}
	}

	return answered.reply, true
}

// bindDNS opens the responder's UDP and TCP sockets on the relay's own address, then the signal
// connection — before ready, so no query can be answered unsignalled.
func (s *server) bindDNS(ctx context.Context, port uint16) error {
	var config net.ListenConfig

	addr := netip.AddrPortFrom(s.self, port).String()

	packets, err := config.ListenPacket(ctx, "udp4", addr)
	if err != nil {
		return fmt.Errorf("listen for DNS on udp %s: %w", addr, err)
	}

	s.dnsUDP = packets

	stream, err := config.Listen(ctx, "tcp4", addr)
	if err != nil {
		return fmt.Errorf("listen for DNS on tcp %s: %w", addr, err)
	}

	s.dnsTCP = stream

	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp4", s.spec.Signal.String())
	if err != nil {
		return fmt.Errorf("dial the signal listener %s: %w", s.spec.Signal, err)
	}

	s.signalConn = conn
	s.signal = &signalWriter{w: conn}

	if err := WritePreamble(conn, s.spec.Token, DNSPort); err != nil {
		return fmt.Errorf("frame the signal connection %s: %w", s.spec.Signal, err)
	}

	return nil
}

// serveUDP answers DNS over UDP until the socket closes.
func (s *server) serveUDP(ctx context.Context) {
	buffer := make([]byte, udpMessage)

	for {
		read, from, err := s.dnsUDP.ReadFrom(buffer)
		if err != nil {
			if ctx.Err() == nil {
				s.fail(fmt.Errorf("%w: read on udp %s: %w", errDNS, s.dnsUDP.LocalAddr(), err))
			}

			return
		}

		if reply, ok := s.respond(buffer[:read]); ok {
			// A reply that does not arrive is the client's to retry, as on any UDP resolver.
			_, _ = s.dnsUDP.WriteTo(reply, from) //nolint:errcheck // see above.
		}
	}
}

// serveTCP accepts DNS-over-TCP connections until the listener closes.
func (s *server) serveTCP(ctx context.Context) {
	for {
		conn, err := s.dnsTCP.Accept()
		if err != nil {
			if ctx.Err() == nil {
				s.fail(fmt.Errorf("%w: accept on tcp %s: %w", errDNS, s.dnsTCP.Addr(), err))
			}

			return
		}

		s.wg.Go(func() { s.serveDNSConn(ctx, conn) })
	}
}

// serveDNSConn answers every query on one DNS-over-TCP connection, each framed by a two-byte length,
// and closes the connection once it has been idle for tcpIdle or sends something that does not parse.
func (s *server) serveDNSConn(ctx context.Context, conn net.Conn) {
	if !s.track(conn) {
		return
	}

	defer s.untrack(conn)
	defer func() { _ = conn.Close() }()

	for ctx.Err() == nil {
		if err := conn.SetReadDeadline(time.Now().Add(tcpIdle)); err != nil {
			return
		}

		query, err := readFramed(conn)
		if err != nil {
			return
		}

		reply, ok := s.respond(query)
		if !ok {
			return
		}

		framed := make([]byte, 0, dnsLengthSize+len(reply))
		framed = binary.BigEndian.AppendUint16(framed, uint16(len(reply))) //nolint:gosec // a reply is small.

		if _, err := conn.Write(append(framed, reply...)); err != nil {
			return
		}
	}
}

// closeDNS closes whatever of the responder's sockets and the signal connection were opened.
func (s *server) closeDNS() {
	for _, closer := range []io.Closer{s.dnsUDP, s.dnsTCP, s.signalConn} {
		if closer != nil {
			_ = closer.Close()
		}
	}
}

// readFramed reads one length-prefixed DNS message.
func readFramed(r io.Reader) ([]byte, error) {
	length := make([]byte, dnsLengthSize)
	if _, err := io.ReadFull(r, length); err != nil {
		return nil, fmt.Errorf("read a query's length: %w", err)
	}

	message := make([]byte, binary.BigEndian.Uint16(length))
	if _, err := io.ReadFull(r, message); err != nil {
		return nil, fmt.Errorf("read a query: %w", err)
	}

	return message, nil
}
