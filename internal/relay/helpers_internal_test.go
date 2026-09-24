package relay

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"
)

// testWait bounds every wait an internal test makes on a relay.
const testWait = 5 * time.Second

// lockedBuffer is a buffer the relay writes from its own goroutines while the test reads it.
type lockedBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// started is one system.run in its own goroutine.
type started struct {
	stdout chan string
	stderr *lockedBuffer
	done   chan struct{}
	cancel context.CancelFunc
	code   int
}

func runSystem(t *testing.T, sys system, args []string) *started {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	outReader, outWriter := io.Pipe()

	run := &started{stdout: make(chan string, 8), stderr: &lockedBuffer{}, done: make(chan struct{}), cancel: cancel}

	go func() {
		scanner := bufio.NewScanner(outReader)
		for scanner.Scan() {
			run.stdout <- scanner.Text()
		}
	}()

	go func() {
		defer close(run.done)

		run.code = sys.run(ctx, args, outWriter, run.stderr)
		_ = outWriter.Close()
	}()

	t.Cleanup(func() {
		cancel()
		<-run.done
	})

	return run
}

// line waits for the next stdout line. It reports false when the run ended first.
func (r *started) line(t *testing.T) (string, bool) {
	t.Helper()

	select {
	case line := <-r.stdout:
		return line, true
	case <-r.done:
		return "", false
	case <-time.After(testWait):
		t.Fatalf("no stdout line within %s: %s", testWait, r.stderr)
	}

	return "", false
}

// ready waits for the ready line, failing the test if the run ends first.
func (r *started) ready(t *testing.T) netip.Addr {
	t.Helper()

	line, printed := r.line(t)
	if !printed {
		t.Fatalf("relay exited %d before ready: %s", r.code, r.stderr)
	}

	address, found := strings.CutPrefix(line, Ready+" ")
	if !found {
		t.Fatalf("first stdout line = %q, want %q", line, Ready+" <IPv4>")
	}

	return netip.MustParseAddr(address)
}

// wait waits for the run to end by itself and returns its code.
func (r *started) wait(t *testing.T) int {
	t.Helper()

	select {
	case <-r.done:
	case <-time.After(testWait):
		t.Fatalf("relay still running after %s", testWait)
	}

	return r.code
}

// fixedAddresses is an interface-address seam that reports the given addresses.
func fixedAddresses(addrs ...string) func() ([]netip.Addr, error) {
	return func() ([]netip.Addr, error) {
		parsed := make([]netip.Addr, 0, len(addrs))
		for _, addr := range addrs {
			parsed = append(parsed, netip.MustParseAddr(addr))
		}

		return parsed, nil
	}
}

func testToken(t *testing.T) Token {
	t.Helper()

	token, err := NewToken()
	if err != nil {
		t.Fatalf("NewToken() error = %v", err)
	}

	return token
}

func unusedPort(t *testing.T) uint16 {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	port := netip.MustParseAddrPort(listener.Addr().String()).Port()
	_ = listener.Close()

	return port
}

// hostListener accepts relayed connections, reads each preamble and reports it.
type hostListener struct {
	listener net.Listener
	conns    chan *Conn
	address  netip.AddrPort
}

func listenForRelay(t *testing.T, token Token) *hostListener {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	host := &hostListener{
		listener: listener,
		conns:    make(chan *Conn, 64),
		address:  netip.MustParseAddrPort(listener.Addr().String()),
	}

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			accepted, preambleErr := Accept(conn, token)
			if preambleErr != nil {
				_ = conn.Close()

				continue
			}

			host.conns <- accepted
		}
	}()

	t.Cleanup(func() { _ = listener.Close() })

	return host
}

// next waits for the next relayed connection.
func (h *hostListener) next(t *testing.T) *Conn {
	t.Helper()

	select {
	case conn := <-h.conns:
		t.Cleanup(func() { _ = conn.Close() })

		return conn
	case <-time.After(testWait):
		t.Fatal("the host listener got no relayed connection")
	}

	return nil
}

// dialFrom dials addr and reports the error rather than failing, for tests that expect a refusal. A
// connection that opens is closed when the test ends.
func dialFrom(t *testing.T, addr netip.AddrPort) error {
	t.Helper()

	dialer := net.Dialer{Timeout: testWait}

	conn, err := dialer.DialContext(t.Context(), "tcp4", addr.String())
	if err == nil {
		t.Cleanup(func() { _ = conn.Close() })
	}

	return err
}
