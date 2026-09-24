package relay_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/relay"
)

// exitWait bounds how long a test waits for a relay that should exit by itself.
const exitWait = 2 * time.Second

// syncBuffer is a buffer the relay writes from its own goroutines while the test reads it.
type syncBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// running is one relay.Run in its own goroutine.
type running struct {
	stdout chan string
	stderr *syncBuffer
	done   chan struct{}
	cancel context.CancelFunc
	code   int
}

func startRelay(t *testing.T, args []string) *running {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	outReader, outWriter := io.Pipe()

	run := &running{stdout: make(chan string, 8), stderr: &syncBuffer{}, done: make(chan struct{}), cancel: cancel}

	go func() {
		scanner := bufio.NewScanner(outReader)
		for scanner.Scan() {
			run.stdout <- scanner.Text()
		}
	}()

	go func() {
		defer close(run.done)

		run.code = relay.Run(ctx, args, outWriter, run.stderr)
		_ = outWriter.Close()
	}()

	t.Cleanup(func() {
		cancel()
		<-run.done
	})

	return run
}

// ready waits for the relay's ready line and returns the address it names.
func (r *running) ready(t *testing.T) string {
	t.Helper()

	select {
	case line := <-r.stdout:
		address, found := strings.CutPrefix(line, relay.Ready+" ")
		if !found {
			t.Fatalf("first stdout line = %q, want %q", line, relay.Ready+" <IPv4>")
		}

		return address
	case <-r.done:
		t.Fatalf("relay exited %d before ready: %s", r.code, r.stderr)
	case <-time.After(ioBound):
		t.Fatalf("relay not ready within %s: %s", ioBound, r.stderr)
	}

	return ""
}

// exit waits a while for the relay to exit by itself, then stops it, and returns its code.
func (r *running) exit() int {
	select {
	case <-r.done:
	case <-time.After(exitWait):
		r.cancel()
		<-r.done
	}

	return r.code
}

func freePort(t *testing.T) uint16 {
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

// hostSide is the host listener a relay dials: every connection's preamble is read with relay.Accept,
// and a valid one is optionally greeted, then handed to the test.
type hostSide struct {
	listener net.Listener
	conns    chan *relay.Conn
	address  netip.AddrPort
}

func listenHost(t *testing.T, token relay.Token, greeting string) *hostSide {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	host := &hostSide{
		listener: listener,
		conns:    make(chan *relay.Conn, 4),
		address:  netip.MustParseAddrPort(listener.Addr().String()),
	}

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			accepted, preambleErr := relay.Accept(conn, token)
			if preambleErr != nil {
				_ = conn.Close()

				continue
			}

			if greeting != "" {
				_, _ = io.WriteString(accepted, greeting) //nolint:errcheck // the client's read judges it.
			}

			host.conns <- accepted
		}
	}()

	t.Cleanup(func() { _ = listener.Close() })

	return host
}

func (h *hostSide) accepted(t *testing.T) *relay.Conn {
	t.Helper()

	select {
	case conn := <-h.conns:
		t.Cleanup(func() { _ = conn.Close() })
		bound(t, conn)

		return conn
	case <-time.After(ioBound):
		t.Fatal("the host listener got no relayed connection")
	}

	return nil
}

// relayed is one relay between a test's service-side client and its host listener.
type relayed struct {
	run  *running
	host *hostSide
	at   netip.AddrPort
}

func startRelayed(t *testing.T, greeting string) relayed {
	t.Helper()

	token := mustToken(t)
	host := listenHost(t, token, greeting)
	port := freePort(t)

	spec := relay.Spec{
		Bind:      netip.MustParsePrefix("127.0.0.1/32"),
		Listeners: []relay.Listener{{Upstream: host.address, Port: port}},
		Token:     token,
	}

	run := startRelay(t, spec.Args())
	if got := run.ready(t); got != "127.0.0.1" {
		t.Fatalf("ready address = %q, want 127.0.0.1", got)
	}

	return relayed{run: run, host: host, at: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)}
}

func (r relayed) dial(t *testing.T) *net.TCPConn {
	t.Helper()

	var dialer net.Dialer

	conn, err := dialer.DialContext(t.Context(), "tcp4", r.at.String())
	if err != nil {
		t.Fatalf("dial the relay: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })
	bound(t, conn)

	return asTCP(t, conn)
}

// TestRelayTransport holds the relay to transparency: whatever the service and its dependency would
// have seen on a direct connection, they see through the relay — half-closes, resets, a server that
// speaks first, and bytes.
func TestRelayTransport(t *testing.T) {
	t.Parallel()

	t.Run("half-close-client-first", func(t *testing.T) {
		t.Parallel()

		through := startRelayed(t, "")
		client := through.dial(t)
		host := through.host.accepted(t)

		send(t, client, "ping")
		closeWrite(t, client)

		if request := drain(t, host); request != "ping" {
			t.Fatalf("host read = %q, want %q", request, "ping")
		}

		if _, err := io.WriteString(host, "pong"); err != nil {
			t.Fatalf("host write: %v", err)
		}

		_ = host.Close()

		if reply := drain(t, client); reply != "pong" {
			t.Errorf("reply = %q, want %q", reply, "pong")
		}
	})

	t.Run("half-close-server-first", func(t *testing.T) {
		t.Parallel()

		through := startRelayed(t, "")
		client := through.dial(t)
		host := through.host.accepted(t)

		if _, err := io.WriteString(host, hello); err != nil {
			t.Fatalf("host write: %v", err)
		}

		if err := host.CloseWrite(); err != nil {
			t.Fatalf("host CloseWrite() error = %v", err)
		}

		if greeting := drain(t, client); greeting != hello {
			t.Fatalf("client read = %q, want %q", greeting, hello)
		}

		send(t, client, "more")
		closeWrite(t, client)

		if rest := drain(t, host); rest != "more" {
			t.Errorf("host read after its own half-close = %q, want %q", rest, "more")
		}
	})

	t.Run("reset-crosses-as-reset", func(t *testing.T) {
		t.Parallel()

		for _, direction := range []string{"service aborts", "dependency aborts"} {
			through := startRelayed(t, "")
			client := through.dial(t)
			host := through.host.accepted(t)
			hostTCP := asTCP(t, host.Conn)

			aborting, peer := client, net.Conn(host)
			if direction == "dependency aborts" {
				aborting, peer = hostTCP, client
			}

			abort(t, aborting)

			if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
				t.Errorf("%s: err = %v, want ECONNRESET", direction, err)
			}
		}
	})

	t.Run("server-first-greeting", func(t *testing.T) {
		t.Parallel()

		through := startRelayed(t, "INFO")
		client := through.dial(t)

		if err := client.SetReadDeadline(time.Now().Add(exitWait)); err != nil {
			t.Fatalf("SetReadDeadline() error = %v", err)
		}

		greeting := make([]byte, len("INFO"))
		if _, err := io.ReadFull(client, greeting); err != nil {
			t.Fatalf("read timed out, want INFO (%v)", err)
		}

		if string(greeting) != "INFO" {
			t.Errorf("greeting = %q, want INFO", greeting)
		}
	})

	t.Run("mebibyte-each-way", func(t *testing.T) {
		t.Parallel()

		through := startRelayed(t, "")
		client := through.dial(t)
		host := through.host.accepted(t)
		up, down := randomBytes(t), randomBytes(t)

		received, err := exchange(spliced{client: client, server: asTCP(t, host.Conn)}, up, down)
		if err != nil {
			t.Fatalf("exchange: %v", err)
		}

		// The host leg's preamble was consumed by Accept: any byte of it beyond the 22 would lead the
		// payload here.
		if !bytes.Equal(received.byServer, up) {
			t.Errorf("host received %d bytes after the preamble, not the service's 1 MiB", len(received.byServer))
		}

		if !bytes.Equal(received.byClient, down) {
			t.Errorf("service received %d bytes, not the dependency's 1 MiB", len(received.byClient))
		}
	})

	t.Run("refused-upstream", func(t *testing.T) {
		t.Parallel()

		refused := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), freePort(t))
		port := freePort(t)
		spec := relay.Spec{
			Bind:      netip.MustParsePrefix("127.0.0.1/32"),
			Listeners: []relay.Listener{{Upstream: refused, Port: port}},
			Token:     mustToken(t),
		}

		run := startRelay(t, spec.Args())
		run.ready(t)

		client := relayed{run: run, at: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)}.dial(t)

		if _, err := client.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
			t.Errorf("client read err = %v, want ECONNRESET", err)
		}

		if code := run.exit(); code != 4 {
			t.Errorf("exit = %d, want 4", code)
		}

		stderr := strings.TrimSpace(run.stderr.String())
		if lines := strings.Count(stderr, "\n") + 1; stderr == "" || lines != 1 {
			t.Errorf("stderr = %q, want exactly one line", stderr)
		}

		if !strings.Contains(stderr, refused.String()) {
			t.Errorf("stderr %q does not name the upstream %s", stderr, refused)
		}
	})
}

// TestAnUpstreamResetReachesTheServiceAsAReset is the host side's half of passing a reset through: a
// dependency that resets is reported to the service as a reset, never as a clean close, and the
// other way round.
func TestAnUpstreamResetReachesTheServiceAsAReset(t *testing.T) {
	t.Parallel()

	through := startRelayed(t, "")
	client := through.dial(t)
	host := through.host.accepted(t)

	send(t, client, hello)

	first := make([]byte, len(hello))
	if _, err := io.ReadFull(host, first); err != nil {
		t.Fatalf("host read: %v", err)
	}

	if err := host.Abort(); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}

	_, err := client.Read(make([]byte, 1))
	t.Logf("service read: %v", err)

	if !errors.Is(err, syscall.ECONNRESET) {
		t.Errorf("service read: %v, want ECONNRESET", err)
	}

	mirror := startRelayed(t, "")
	service := mirror.dial(t)
	dependency := mirror.host.accepted(t)

	send(t, service, hello)

	if _, err := io.ReadFull(dependency, first); err != nil {
		t.Fatalf("host read: %v", err)
	}

	abort(t, service)

	if _, err := dependency.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
		t.Errorf("host read after the service aborted: %v, want ECONNRESET", err)
	}
}

// TestTheRelayExitsCleanlyWhenStopped is the signal path: a stopped relay exits 0 at once.
func TestTheRelayExitsCleanlyWhenStopped(t *testing.T) {
	t.Parallel()

	through := startRelayed(t, "")
	through.dial(t)
	through.host.accepted(t)

	began := time.Now()

	through.run.cancel()
	<-through.run.done

	if through.run.code != 0 || time.Since(began) > time.Second {
		t.Errorf("stopped relay: exit %d after %s, want 0 at once (stderr %q)",
			through.run.code, time.Since(began), through.run.stderr)
	}
}
