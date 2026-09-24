package relay

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

const (
	// queried is the name the DNS tests look up.
	queried = "example.test."
	// srvName is the SRV name the DNS tests look up.
	srvName = "_x._tcp." + queried
)

// stubAddress stands in for the stub relay's service-network address in the answer tests.
var stubAddress = netip.MustParseAddr("172.16.0.3")

// question is one query the tests send.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type question struct {
	name   string
	count  int
	qtype  dnsmessage.Type
	class  dnsmessage.Class
	opcode dnsmessage.OpCode
	opt    bool
}

// build encodes a query with ID 0x1234 and RD set, carrying count copies of its question.
func (q question) build(t *testing.T) []byte {
	t.Helper()

	class := q.class
	if class == 0 {
		class = dnsmessage.ClassINET
	}

	count := q.count
	if count == 0 {
		count = 1
	}

	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: 0x1234, RecursionDesired: true, OpCode: q.opcode})

	err := builder.StartQuestions()
	for range count {
		err = errors.Join(err, builder.Question(dnsmessage.Question{
			Name: dnsmessage.MustNewName(q.name), Type: q.qtype, Class: class,
		}))
	}

	if q.opt {
		var opt dnsmessage.ResourceHeader

		err = errors.Join(err, opt.SetEDNS0(4096, dnsmessage.RCodeSuccess, false), builder.StartAdditionals(),
			builder.OPTResource(opt, dnsmessage.OPTResource{}))
	}

	message, finishErr := builder.Finish()
	if err = errors.Join(err, finishErr); err != nil {
		t.Fatalf("build the query: %v", err)
	}

	return message
}

// expectation is what one row of the responder table must answer. want is the one answer record,
// rendered; empty wants none.
type expectation struct {
	name   string
	want   string
	query  []byte
	rcode  dnsmessage.RCode
	record bool
	drop   bool
}

// rendered is an answer record as the table spells it.
func rendered(resource dnsmessage.Resource) string {
	switch body := resource.Body.(type) {
	case *dnsmessage.AResource:
		return "A " + netip.AddrFrom4(body.A).String()
	case *dnsmessage.MXResource:
		return fmt.Sprintf("MX %d %s", body.Pref, body.MX)
	default:
		return resource.Body.GoString()
	}
}

// responderTable is §18.2's policy plus §16.6's mechanics, one row each.
func responderTable(t *testing.T) []expectation {
	t.Helper()

	address := "A " + stubAddress.String()

	return []expectation{
		{name: "A " + queried, query: question{name: queried, qtype: dnsmessage.TypeA}.build(t), want: address},
		{
			name: "MX " + queried, query: question{name: queried, qtype: dnsmessage.TypeMX}.build(t),
			want: "MX 10 " + queried, record: true,
		},
		{name: "SRV " + srvName, query: question{name: srvName, qtype: dnsmessage.TypeSRV}.build(t), record: true},
		{name: "AAAA " + queried, query: question{name: queried, qtype: dnsmessage.TypeAAAA}.build(t)},
		{name: "TXT " + queried, query: question{name: queried, qtype: dnsmessage.TypeTXT}.build(t), record: true},
		{name: "HTTPS " + queried, query: question{name: queried, qtype: dnsmessage.TypeHTTPS}.build(t), record: true},
		{
			name:  "class CH",
			query: question{name: "version.bind.", qtype: dnsmessage.TypeTXT, class: dnsmessage.ClassCHAOS}.build(t),
		},
		{
			name: "QDCOUNT 2", query: question{name: queried, qtype: dnsmessage.TypeA, count: 2}.build(t),
			rcode: dnsmessage.RCodeFormatError,
		},
		{
			name: "opcode STATUS", query: question{name: queried, qtype: dnsmessage.TypeA, opcode: 2}.build(t),
			rcode: dnsmessage.RCodeNotImplemented,
		},
		{name: "garbage", query: []byte{0x12, 0x34, 0x01}, drop: true},
		{
			name: "A with an OPT", query: question{name: queried, qtype: dnsmessage.TypeA, opt: true}.build(t),
			want: address,
		},
	}
}

// TestResponderMechanics holds the responder to §18.2's answers and §16.6's mechanics: every row's
// rcode, answers, TTL 0, flags, no OPT, never NXDOMAIN, and whether the query is signalled.
func TestResponderMechanics(t *testing.T) {
	t.Parallel()

	rows := responderTable(t)
	for _, row := range rows {
		checkRow(t, row)
	}

	t.Logf("responder rows checked: %d", len(rows))
}

func checkRow(t *testing.T, row expectation) {
	t.Helper()

	got, answered := answer(row.query, stubAddress)
	if row.drop {
		if answered {
			t.Errorf("%s: answered, want the query dropped", row.name)
		}

		return
	}

	if !answered {
		t.Fatalf("%s: dropped, want an answer", row.name)
	}

	var parser dnsmessage.Parser

	header, err := parser.Start(got.reply)
	if err != nil {
		t.Fatalf("%s: the reply does not parse: %v", row.name, err)
	}

	if header.RCode != row.rcode || header.RCode == dnsmessage.RCodeNameError {
		t.Errorf("%s: rcode %s, want %s", row.name, header.RCode, row.rcode)
	}

	if header.ID != 0x1234 || !header.Response || !header.Authoritative || !header.RecursionDesired ||
		!header.RecursionAvailable || header.Truncated {
		t.Errorf("%s: flags %+v, want QR AA RD RA, not TC, ID 0x1234", row.name, header)
	}

	checkSections(t, row, &parser)

	if got.signal != row.record {
		t.Errorf("%s: signalled = %v, want %v", row.name, got.signal, row.record)
	}
}

func checkSections(t *testing.T, row expectation, parser *dnsmessage.Parser) {
	t.Helper()

	if _, err := parser.AllQuestions(); err != nil {
		t.Fatalf("%s: questions: %v", row.name, err)
	}

	answers, err := parser.AllAnswers()
	if err != nil {
		t.Fatalf("%s: answers: %v", row.name, err)
	}

	got := make([]string, 0, len(answers))

	for _, resource := range answers {
		got = append(got, rendered(resource))

		if resource.Header.TTL != 0 {
			t.Errorf("%s: TTL %d, want 0", row.name, resource.Header.TTL)
		}
	}

	if joined := strings.Join(got, "; "); joined != row.want {
		t.Errorf("%s: answers %q, want %q", row.name, joined, row.want)
	}

	if skipErr := parser.SkipAllAuthorities(); skipErr != nil {
		t.Fatalf("%s: authorities: %v", row.name, skipErr)
	}

	additionals, additionalErr := parser.AllAdditionals()
	if additionalErr != nil || len(additionals) != 0 {
		t.Errorf("%s: %d additional records (%v), want none — no OPT", row.name, len(additionals), additionalErr)
	}
}

// framedQuery writes one DNS-over-TCP message: a two-byte length, then the message.
func framedQuery(t *testing.T, conn net.Conn, message []byte) {
	t.Helper()

	framed := binary.BigEndian.AppendUint16(nil, uint16(len(message))) //nolint:gosec // a test query is small.
	if _, err := conn.Write(append(framed, message...)); err != nil {
		t.Fatalf("write the query: %v", err)
	}
}

// awaitReply reads one DNS-over-TCP reply and reports whether it arrived whole.
func awaitReply(conn net.Conn) error {
	length := make([]byte, dnsLengthSize)
	if _, err := io.ReadFull(conn, length); err != nil {
		return err
	}

	_, err := io.ReadFull(conn, make([]byte, binary.BigEndian.Uint16(length)))

	return err
}

// serveDNS answers DNS-over-TCP on conn with a responder whose signal is written to signal.
func serveDNS(t *testing.T, signal io.Writer, conn net.Conn) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())

	srv := &server{
		self:   stubAddress,
		cancel: cancel,
		conns:  make(map[net.Conn]struct{}),
		signal: &signalWriter{w: signal},
	}

	srv.wg.Go(func() { srv.serveDNSConn(ctx, conn) })

	t.Cleanup(func() {
		cancel()

		_ = conn.Close()

		srv.wg.Wait()
	})
}

func TestTwoQueriesOnOneTCPConnection(t *testing.T) {
	t.Parallel()

	ends := tcpPairInternal(t)
	serveDNS(t, io.Discard, ends.accepted)

	for index, qtype := range []dnsmessage.Type{dnsmessage.TypeA, dnsmessage.TypeMX} {
		framedQuery(t, ends.dialled, question{name: queried, qtype: qtype}.build(t))

		if err := awaitReply(ends.dialled); err != nil {
			t.Fatalf("query %d on one connection: %v", index+1, err)
		}
	}
}

func TestAnIdleTCPConnectionIsClosedAtTenSeconds(t *testing.T) {
	t.Parallel()

	ends := tcpPairInternal(t)
	serveDNS(t, io.Discard, ends.accepted)

	// Timed from before the query leaves: the responder's idle clock starts only once it has answered,
	// so the connection cannot close sooner than tcpIdle after this.
	asked := time.Now()

	framedQuery(t, ends.dialled, question{name: queried, qtype: dnsmessage.TypeA}.build(t))

	if err := awaitReply(ends.dialled); err != nil {
		t.Fatalf("read the answer: %v", err)
	}

	if err := ends.dialled.SetReadDeadline(asked.Add(2 * tcpIdle)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}

	_, err := ends.dialled.Read(make([]byte, 1))
	idle := time.Since(asked)

	if !errors.Is(err, io.EOF) || idle < tcpIdle || idle >= tcpIdle+time.Second {
		t.Errorf("idle connection: read %v after %s, want EOF within [%s, %s)", err, idle, tcpIdle, tcpIdle+time.Second)
	}
}

// TestTheSignalRecordPrecedesTheAnswer is what lets the harness act on a query before the service
// acts on its answer. The signal is a synchronous pipe here, so the answer cannot leave before the
// record has been read.
func TestTheSignalRecordPrecedesTheAnswer(t *testing.T) {
	t.Parallel()

	relaySide, hostSide := net.Pipe()

	t.Cleanup(func() {
		_ = relaySide.Close()
		_ = hostSide.Close()
	})

	ends := tcpPairInternal(t)
	serveDNS(t, relaySide, ends.accepted)

	framedQuery(t, ends.dialled, question{name: srvName, qtype: dnsmessage.TypeSRV}.build(t))

	replies := make(chan error, 1)

	go func() { replies <- awaitReply(ends.dialled) }()

	select {
	case <-replies:
		t.Fatal("answer arrived before the signal record was read")
	case <-time.After(2 * time.Second):
	}

	record, err := ReadQuery(hostSide)
	if err != nil || record != (Query{Name: srvName, Type: uint16(dnsmessage.TypeSRV)}) {
		t.Fatalf("signal record = %+v, %v; want the SRV query", record, err)
	}

	select {
	case replyErr := <-replies:
		if replyErr != nil {
			t.Errorf("read the answer: %v", replyErr)
		}
	case <-time.After(testWait):
		t.Error("no answer after the record was read")
	}
}

// TestASignalWriteFailureStopsTheRelay ends a relay that can no longer report queries: answering one it
// could not report would let an SRV lookup through unseen.
func TestASignalWriteFailureStopsTheRelay(t *testing.T) {
	t.Parallel()

	token := testToken(t)
	signalHost := listenForRelay(t, token)
	dnsPort := unusedPort(t)

	sys := system{addresses: fixedAddresses("127.0.0.1"), dnsPort: dnsPort}
	spec := Spec{Bind: netip.MustParsePrefix("127.0.0.1/32"), Signal: signalHost.address, Token: token}

	run := runSystem(t, sys, spec.Args())
	run.ready(t)

	signal := signalHost.next(t)
	if signal.DestinationPort() != DNSPort {
		t.Fatalf("the signal connection's preamble port = %d, want %d", signal.DestinationPort(), DNSPort)
	}

	if err := signal.Abort(); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}

	sendUntilExit(t, run, loopback(dnsPort), question{name: queried, qtype: dnsmessage.TypeTXT}.build(t))

	if code := run.wait(t); code != exitFailure {
		t.Errorf("exit = %d, want %d", code, exitFailure)
	}

	stderr := strings.TrimSpace(run.stderr.String())
	if strings.Count(stderr, "\n") != 0 || !strings.Contains(stderr, signalHost.address.String()) {
		t.Errorf("stderr = %q, want one line naming %s", stderr, signalHost.address)
	}
}

// sendUntilExit sends query over UDP until the relay exits: the reset from the host lands at a moment
// the test cannot see, and every query after it fails the signal write.
func sendUntilExit(t *testing.T, run *started, to netip.AddrPort, query []byte) {
	t.Helper()

	var dialer net.Dialer

	conn, err := dialer.DialContext(t.Context(), "udp4", to.String())
	if err != nil {
		t.Fatalf("dial DNS: %v", err)
	}

	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(testWait)

	for time.Now().Before(deadline) {
		if _, err := conn.Write(query); err != nil {
			t.Fatalf("send a query: %v", err)
		}

		select {
		case <-run.done:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}

	t.Fatal("the relay kept running after its signal connection was reset")
}

// connPair is the two ends of one loopback TCP connection.
type connPair struct {
	dialled  net.Conn
	accepted net.Conn
}

func tcpPairInternal(t *testing.T) connPair {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	defer func() { _ = listener.Close() }()

	var dialer net.Dialer

	dialled, err := dialer.DialContext(t.Context(), "tcp4", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	accepted, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}

	t.Cleanup(func() {
		_ = dialled.Close()
		_ = accepted.Close()
	})

	return connPair{dialled: dialled, accepted: accepted}
}
