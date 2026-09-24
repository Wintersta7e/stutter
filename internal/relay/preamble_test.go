package relay_test

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/relay"
)

// ioBound keeps a broken relay from hanging a test: every read a test makes gives up after it.
const ioBound = 5 * time.Second

// tcpEnds are the two ends of one real loopback TCP connection. Never net.Pipe: it has no half-close
// and no reset, which are the behaviours under test.
type tcpEnds struct {
	dialled  *net.TCPConn
	accepted *net.TCPConn
}

func tcpPair(t *testing.T) tcpEnds {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	defer func() { _ = listener.Close() }()

	accepted := make(chan net.Conn, 1)

	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			close(accepted)

			return
		}

		accepted <- conn
	}()

	var dialer net.Dialer

	dialled, err := dialer.DialContext(t.Context(), "tcp4", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	other, ok := <-accepted
	if !ok {
		t.Fatal("accept failed")
	}

	t.Cleanup(func() {
		_ = dialled.Close()
		_ = other.Close()
	})

	return tcpEnds{dialled: asTCP(t, dialled), accepted: asTCP(t, other)}
}

func asTCP(t *testing.T, conn net.Conn) *net.TCPConn {
	t.Helper()

	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("%T is not a TCP connection", conn)
	}

	return tcp
}

// bound gives every read and write on conn a deadline, so a broken relay fails a test instead of
// hanging it.
func bound(t *testing.T, conn net.Conn) {
	t.Helper()

	if err := conn.SetDeadline(time.Now().Add(ioBound)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
}

func mustToken(t *testing.T) relay.Token {
	t.Helper()

	token, err := relay.NewToken()
	if err != nil {
		t.Fatalf("NewToken() error = %v", err)
	}

	return token
}

func TestPreambleRoundTrip(t *testing.T) {
	t.Parallel()

	token := mustToken(t)
	ends := tcpPair(t)

	if err := relay.WritePreamble(ends.dialled, token, 5432); err != nil {
		t.Fatalf("WritePreamble() error = %v", err)
	}

	if _, err := io.WriteString(ends.dialled, "hello"); err != nil {
		t.Fatalf("write the payload: %v", err)
	}

	accepted, err := relay.Accept(ends.accepted, token)
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}

	if got := accepted.DestinationPort(); got != 5432 {
		t.Errorf("DestinationPort() = %d, want 5432", got)
	}

	bound(t, accepted)

	payload := make([]byte, len("hello"))
	if _, err := io.ReadFull(accepted, payload); err != nil {
		t.Fatalf("read the payload after the preamble: %v", err)
	}

	if string(payload) != "hello" {
		t.Errorf("payload = %q, want %q", payload, "hello")
	}
}

// TestAcceptRefusesForeignPreambles is the host side's whole defence against a container that is not
// one of this check's relays: every listener is reachable from every container on the engine.
func TestAcceptRefusesForeignPreambles(t *testing.T) {
	t.Parallel()

	token := mustToken(t)
	wrongToken := token
	wrongToken[0] ^= 0xff

	cases := []struct {
		send     func(t *testing.T, conn *net.TCPConn)
		name     string
		minDelay time.Duration
		maxDelay time.Duration
	}{
		{
			name: "wrong magic",
			send: func(t *testing.T, conn *net.TCPConn) {
				t.Helper()

				preamble := make([]byte, relay.PreambleSize)
				copy(preamble, "HTTP")

				if _, err := conn.Write(preamble); err != nil {
					t.Errorf("write: %v", err)
				}
			},
			maxDelay: time.Second,
		},
		{
			name: "wrong token",
			send: func(t *testing.T, conn *net.TCPConn) {
				t.Helper()

				if err := relay.WritePreamble(conn, wrongToken, 5432); err != nil {
					t.Errorf("WritePreamble() error = %v", err)
				}
			},
			maxDelay: time.Second,
		},
		{
			name: "21 bytes then EOF",
			send: func(t *testing.T, conn *net.TCPConn) {
				t.Helper()

				if _, err := conn.Write(make([]byte, relay.PreambleSize-1)); err != nil {
					t.Errorf("write: %v", err)
				}

				if err := conn.CloseWrite(); err != nil {
					t.Errorf("CloseWrite() error = %v", err)
				}
			},
			maxDelay: time.Second,
		},
		{
			name:     "nothing for more than 2s",
			send:     func(*testing.T, *net.TCPConn) {},
			minDelay: 2 * time.Second,
			maxDelay: 3 * time.Second,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ends := tcpPair(t)
			testCase.send(t, ends.dialled)

			began := time.Now()
			accepted, err := relay.Accept(ends.accepted, token)
			elapsed := time.Since(began)

			if !errors.Is(err, relay.ErrForeign) {
				t.Fatalf("%s: err = %v, want ErrForeign (accepted %v)", testCase.name, err, accepted != nil)
			}

			if elapsed < testCase.minDelay || elapsed >= testCase.maxDelay {
				t.Errorf("%s: refused after %s, want within [%s, %s)",
					testCase.name, elapsed, testCase.minDelay, testCase.maxDelay)
			}
		})
	}
}

func TestWriteAckIsTheMagic(t *testing.T) {
	t.Parallel()

	ends := tcpPair(t)

	if err := relay.WriteAck(ends.accepted); err != nil {
		t.Fatalf("WriteAck() error = %v", err)
	}

	bound(t, ends.dialled)

	ack := make([]byte, 4)
	if _, err := io.ReadFull(ends.dialled, ack); err != nil {
		t.Fatalf("read the ack: %v", err)
	}

	if string(ack) != "STU\x01" {
		t.Errorf("ack = % x, want 53 54 55 01", ack)
	}
}
