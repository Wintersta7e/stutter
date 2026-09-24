package relay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// The transports the responder answers on.
const (
	udpNetwork = "udp4"
	tcpNetwork = "tcp4"
)

// policySelf is the stub relay's own address in the policy test: where every A answer must point.
var policySelf = netip.MustParseAddr("127.0.0.1")

// policyResponder is a running responder on loopback, its UDP and TCP addresses, and the signal
// records the harness side has read, in arrival order.
type policyResponder struct {
	udp, tcp string
	signals  []Query
	mu       sync.Mutex
}

func (p *policyResponder) signalled() []Query {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.signals)
}

// startPolicyResponder runs the stub relay's responder over UDP and TCP on loopback, with a signal
// listener standing in for the harness's.
func startPolicyResponder(t *testing.T) *policyResponder {
	t.Helper()

	token, err := NewToken()
	if err != nil {
		t.Fatalf("NewToken() error = %v", err)
	}

	responder := &policyResponder{}
	signal := responder.listenForSignals(t, token)

	ctx, cancel := context.WithCancel(t.Context())
	srv := &server{
		self:   policySelf,
		cancel: cancel,
		conns:  make(map[net.Conn]struct{}),
		spec:   Spec{Signal: signal, Token: token},
	}

	if err := srv.bindDNS(ctx, 0); err != nil {
		t.Fatalf("bindDNS() error = %v", err)
	}

	srv.wg.Go(func() { srv.serveUDP(ctx) })
	srv.wg.Go(func() { srv.serveTCP(ctx) })

	t.Cleanup(func() {
		cancel()
		srv.closeDNS()
		srv.wg.Wait()
	})

	responder.udp, responder.tcp = srv.dnsUDP.LocalAddr().String(), srv.dnsTCP.Addr().String()

	return responder
}

// listenForSignals accepts the responder's signal connection and keeps every record it carries.
func (p *policyResponder) listenForSignals(t *testing.T, token Token) netip.AddrPort {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the signal: %v", err)
	}

	var reading sync.WaitGroup

	reading.Go(func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		framed, acceptErr := Accept(conn, token)
		if acceptErr != nil {
			return
		}

		for {
			query, readErr := ReadQuery(framed)
			if readErr != nil {
				return
			}

			p.mu.Lock()
			p.signals = append(p.signals, query)
			p.mu.Unlock()
		}
	})

	t.Cleanup(func() {
		_ = listener.Close()

		reading.Wait()
	})

	return netip.MustParseAddrPort(listener.Addr().String())
}

// ask sends one query over network and returns the reply.
func (p *policyResponder) ask(t *testing.T, network string, query []byte) []byte {
	t.Helper()

	address := p.udp
	if network == tcpNetwork {
		address = p.tcp
	}

	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), network, address)
	if err != nil {
		t.Fatalf("dial the responder over %s: %v", network, err)
	}

	defer func() { _ = conn.Close() }()

	if deadlineErr := conn.SetDeadline(time.Now().Add(testWait)); deadlineErr != nil {
		t.Fatal(deadlineErr)
	}

	if network == tcpNetwork {
		framedQuery(t, conn, query)

		reply, readErr := readFramed(conn)
		if readErr != nil {
			t.Fatalf("read the answer over tcp: %v", readErr)
		}

		return reply
	}

	if _, writeErr := conn.Write(query); writeErr != nil {
		t.Fatalf("write the query over udp: %v", writeErr)
	}

	reply := make([]byte, udpMessage)

	read, err := conn.Read(reply)
	if err != nil {
		t.Fatalf("read the answer over udp: %v", err)
	}

	return reply[:read]
}

// policyRow is one of §18.2's answers: the answer records, rendered, and whether the query is signalled.
type policyRow struct {
	name    string
	want    string
	qtype   dnsmessage.Type
	signals bool
}

func policyRows() []policyRow {
	return []policyRow{
		{name: queried, qtype: dnsmessage.TypeA, want: "A " + policySelf.String()},
		{name: queried, qtype: dnsmessage.TypeAAAA},
		{name: queried, qtype: dnsmessage.TypeTXT, signals: true},
		{name: queried, qtype: dnsmessage.TypeHTTPS, signals: true},
		{name: queried, qtype: dnsmessage.TypeMX, want: "MX 10 " + queried, signals: true},
		{name: srvName, qtype: dnsmessage.TypeSRV, signals: true},
	}
}

// randomRows are A queries for names nobody declared: every one must resolve to the stub relay.
func randomRows(t *testing.T) []policyRow {
	t.Helper()

	rows := make([]policyRow, 0, 20)

	for range 20 {
		label := make([]byte, 8)
		if _, err := rand.Read(label); err != nil {
			t.Fatal(err)
		}

		rows = append(rows, policyRow{
			name:  hex.EncodeToString(label) + ".unknown.example.",
			qtype: dnsmessage.TypeA,
			want:  "A " + policySelf.String(),
		})
	}

	return rows
}

// TestResponderFollowsTheEgressAnswerPolicy holds the stub relay to the egress answer policy over UDP
// and TCP: A is the stub relay; AAAA, TXT and HTTPS are NOERROR with nothing; MX names the queried
// name, preference 10; SRV is NODATA and signalled; no name is ever NXDOMAIN or SERVFAIL.
func TestResponderFollowsTheEgressAnswerPolicy(t *testing.T) {
	t.Parallel()

	rows := append(policyRows(), randomRows(t)...)
	asked := 0

	for _, network := range []string{udpNetwork, tcpNetwork} {
		responder := startPolicyResponder(t)

		var wantSignals []Query

		for _, row := range rows {
			reply := responder.ask(t, network, question{name: row.name, qtype: row.qtype}.build(t))
			checkPolicy(t, network, row, reply)

			asked++

			if row.signals {
				wantSignals = append(wantSignals, Query{Name: row.name, Type: uint16(row.qtype)})
			}
		}

		awaitSignals(t, network, responder, wantSignals)
	}

	t.Logf("queries: %d", asked)

	if asked == 0 {
		t.Fatal("queries: 0")
	}
}

// checkPolicy requires reply, over network, to be row's answer.
func checkPolicy(t *testing.T, network string, row policyRow, reply []byte) {
	t.Helper()

	var parser dnsmessage.Parser

	header, err := parser.Start(reply)
	if err != nil {
		t.Fatalf("%s %s %s: the reply does not parse: %v", network, row.qtype, row.name, err)
	}

	if header.RCode != dnsmessage.RCodeSuccess {
		t.Errorf("%s %s %s: rcode %s, want NOERROR", network, row.qtype, row.name, header.RCode)
	}

	if _, questionsErr := parser.AllQuestions(); questionsErr != nil {
		t.Fatalf("%s %s %s: questions: %v", network, row.qtype, row.name, questionsErr)
	}

	answers, err := parser.AllAnswers()
	if err != nil {
		t.Fatalf("%s %s %s: answers: %v", network, row.qtype, row.name, err)
	}

	got := ""
	if len(answers) == 1 {
		got = rendered(answers[0])
	}

	if len(answers) > 1 || got != row.want {
		t.Errorf("%s %s %s: %d answers (%q), want %q", network, row.qtype, row.name, len(answers), got, row.want)
	}
}

// awaitSignals waits for the signal records over network to be exactly want, in order.
func awaitSignals(t *testing.T, network string, responder *policyResponder, want []Query) {
	t.Helper()

	deadline := time.Now().Add(testWait)

	for len(responder.signalled()) < len(want) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := responder.signalled(); !slices.Equal(got, want) {
		t.Errorf("%s: signal records = %v, want %v", network, got, want)
	}
}
