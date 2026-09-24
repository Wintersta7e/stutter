package pg_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// Postgres' encryption request codes, as a client sends them before its startup message.
const (
	sslRequestCode    = 80877103
	gssEncRequestCode = 80877104
)

// encryptionRequest is the 8-byte request a client sends to ask for TLS or GSSAPI encryption.
func encryptionRequest(code uint32) []byte {
	request := binary.BigEndian.AppendUint32(nil, 8)

	return binary.BigEndian.AppendUint32(request, code)
}

// sShim stands between the proxy and a real database as a TLS-capable Postgres would: it answers an
// SSLRequest with S itself, a GSSENCRequest with N, and pipes everything else to the database. Without
// it the test database, which has TLS off, would answer N to a forwarded request and a proxy that still
// forwarded one would pass unnoticed.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type sShim struct {
	listener    net.Listener
	database    string
	wg          sync.WaitGroup
	connections atomic.Int64
	sslRequests atomic.Int64
}

func startShim(t *testing.T, database string) *sShim {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the shim: %v", err)
	}

	shim := &sShim{listener: listener, database: database}

	shim.wg.Go(func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			shim.connections.Add(1)
			shim.wg.Go(func() { shim.serve(t, conn) })
		}
	})

	t.Cleanup(func() {
		_ = listener.Close()

		shim.wg.Wait()
	})

	return shim
}

func (s *sShim) addr() string { return s.listener.Addr().String() }

// serve answers encryption requests until the first other message, then pipes the connection.
func (s *sShim) serve(t *testing.T, conn net.Conn) {
	t.Helper()

	defer func() { _ = conn.Close() }()

	for {
		first := make([]byte, 8)
		if _, err := io.ReadFull(conn, first); err != nil {
			return
		}

		switch binary.BigEndian.Uint32(first[4:]) {
		case sslRequestCode:
			s.sslRequests.Add(1)

			// A TLS-capable server would start its handshake now; the shim has nothing to offer.
			_, _ = conn.Write([]byte{'S'}) //nolint:errcheck // the connection ends either way.

			return
		case gssEncRequestCode:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
		default:
			s.pipe(t, conn, first)

			return
		}
	}
}

// pipe hands the connection to the database, the bytes already read first.
func (s *sShim) pipe(t *testing.T, conn net.Conn, first []byte) {
	t.Helper()

	var dialer net.Dialer

	upstream, err := dialer.DialContext(t.Context(), "tcp", s.database)
	if err != nil {
		return
	}

	defer func() { _ = upstream.Close() }()

	if _, err := upstream.Write(first); err != nil {
		return
	}

	var copies sync.WaitGroup

	copies.Go(func() {
		_, _ = io.Copy(upstream, conn) //nolint:errcheck // the copy ends when either side does.
		_ = upstream.Close()
	})

	_, _ = io.Copy(conn, upstream) //nolint:errcheck // as above.
	_ = conn.Close()

	copies.Wait()
}

// database is the test database's DSN; the test is skipped without it, as through is.
func database(t *testing.T) *url.URL {
	t.Helper()

	dsn := os.Getenv(envPostgres)
	if dsn == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse %s: %v", envPostgres, err)
	}

	return parsed
}

// served is a running proxy and the channel its Serve result arrives on.
type served struct {
	proxy *pg.Proxy
	done  chan error
}

func serveProxy(t *testing.T, upstream string, sink pg.Sink) served {
	t.Helper()

	proxy, err := pg.Listen(t.Context(), "127.0.0.1:0", upstream, sink)
	if err != nil {
		t.Fatalf("pg.Listen() error = %v", err)
	}

	running := served{proxy: proxy, done: make(chan error, 1)}

	go func() { running.done <- proxy.Serve(t.Context()) }()

	t.Cleanup(func() { _ = proxy.Close() })

	return running
}

// closeServed closes the proxy and returns what Serve returned.
func closeServed(t *testing.T, running served) error {
	t.Helper()

	if err := running.proxy.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case err := <-running.done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return within 2s of Close")

		return nil
	}
}

// queryThrough connects pgx through the proxy under sslmode (empty is pgx's default) and runs a short
// statement sequence, with the recorder's window open throughout.
func queryThrough(t *testing.T, proxyAddr string, dsn *url.URL, sslmode string, recorder *effect.Recorder) {
	t.Helper()

	through := *dsn
	through.Host = proxyAddr

	query := through.Query()
	query.Del("sslmode")

	if sslmode != "" {
		query.Set("sslmode", sslmode)
	}

	through.RawQuery = query.Encode()

	// The window opens before the connect, so a negotiation that became an effect would be counted.
	recorder.Open("probe", 1, nil)

	conn, err := pgx.Connect(t.Context(), through.String())
	if err != nil {
		t.Fatalf("connect through the proxy (sslmode %q): %v", sslmode, err)
	}

	defer func() { _ = conn.Close(context.Background()) }()

	for _, statement := range []string{
		"CREATE TEMP TABLE negotiated (id int)",
		"INSERT INTO negotiated (id) VALUES (1)",
		"SELECT id FROM negotiated",
	} {
		if _, err := conn.Exec(t.Context(), statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

// requireEffects fails unless the recorder holds statements, and none of them is about TLS.
func requireEffects(t *testing.T, recorder *effect.Recorder) {
	t.Helper()

	observed := recorder.Effects()

	queries := 0

	for _, item := range observed {
		if strings.Contains(strings.ToLower(item.Canonical), "tls") {
			t.Errorf("effect %q is about TLS: a negotiation is never an effect", item.Canonical)
		}

		if item.Kind == effect.KindPostgres {
			queries++
		}
	}

	if queries == 0 {
		t.Errorf("recorded %d effects and no %s: %v", len(observed), effect.KindPostgres, observed)
	}
}

// TestSSLRequestIsAnsweredByTheProxy keeps a TLS-capable database from turning a session into TLS the
// proxy cannot read: the proxy answers both encryption requests N itself and forwards neither.
func TestSSLRequestIsAnsweredByTheProxy(t *testing.T) {
	t.Parallel()

	dsn := database(t)
	shim := startShim(t, dsn.Host)

	t.Run("raw", func(t *testing.T) {
		t.Parallel()

		running := serveProxy(t, shim.addr(), &countingSink{})

		var dialer net.Dialer

		client, err := dialer.DialContext(t.Context(), "tcp4", running.proxy.Addr())
		if err != nil {
			t.Fatalf("dial the proxy: %v", err)
		}

		defer func() { _ = client.Close() }()

		for _, code := range []uint32{gssEncRequestCode, sslRequestCode} {
			if _, err := client.Write(encryptionRequest(code)); err != nil {
				t.Fatalf("write request %d: %v", code, err)
			}

			reply := make([]byte, 1)

			if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}

			if _, err := io.ReadFull(client, reply); err != nil {
				t.Fatalf("request %d: read the reply: %v", code, err)
			}

			if reply[0] != 'N' {
				t.Fatalf("request %d: reply = %q, want \"N\"", code, reply)
			}
		}
	})

	t.Run("pgx prefer", func(t *testing.T) {
		t.Parallel()

		recorder := effect.NewRecorder(effect.NewCanonicaliser(), make([]byte, 32))
		running := serveProxy(t, shim.addr(), recorder)

		queryThrough(t, running.proxy.Addr(), dsn, "prefer", recorder)
		requireEffects(t, recorder)

		if err := closeServed(t, running); err != nil {
			t.Errorf("Serve() = %v, want nil", err)
		}
	})

	t.Cleanup(func() {
		if count := shim.sslRequests.Load(); count != 0 {
			t.Errorf("the database received %d SSLRequests, want 0: the proxy forwarded the negotiation", count)
		}
	})
}

// TestPreferClientIsNotAFailure runs pgx's default mode against a database that would take TLS: the
// proxy's N makes it continue in cleartext, its statements are effects, and the run does not fail.
func TestPreferClientIsNotAFailure(t *testing.T) {
	t.Parallel()

	dsn := database(t)
	shim := startShim(t, dsn.Host)

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), make([]byte, 32))
	running := serveProxy(t, shim.addr(), recorder)

	queryThrough(t, running.proxy.Addr(), dsn, "", recorder)
	requireEffects(t, recorder)

	if err := closeServed(t, running); err != nil {
		t.Errorf("Serve() = %v, want nil: a client that settled for cleartext is not a failure", err)
	}

	// A shim a refactor bypassed would make every assertion above hold vacuously.
	if shim.connections.Load() == 0 {
		t.Fatal("the shim saw no connection: the proxy did not reach the database through it")
	}

	if count := shim.sslRequests.Load(); count != 0 {
		t.Errorf("the database received %d SSLRequests, want 0", count)
	}
}

// drainingUpstream is a database stand-in that reads whatever reaches it and counts the bytes.
func drainingUpstream(t *testing.T) (string, *atomic.Int64) {
	t.Helper()

	var (
		config net.ListenConfig
		read   atomic.Int64
		wg     sync.WaitGroup
	)

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the upstream: %v", err)
	}

	wg.Go(func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			wg.Go(func() {
				defer func() { _ = conn.Close() }()

				n, _ := io.Copy(io.Discard, conn) //nolint:errcheck // the count is what matters.
				read.Add(n)
			})
		}
	})

	t.Cleanup(func() {
		_ = listener.Close()

		wg.Wait()
	})

	return listener.Addr().String(), &read
}

// TestDirectTLSStopsTheRun stops the run on a client that opens with a TLS ClientHello: its first
// byte is no startup length, and dropping it silently would read as a handler that did nothing.
func TestDirectTLSStopsTheRun(t *testing.T) {
	t.Parallel()

	upstream, read := drainingUpstream(t)
	sink := &countingSink{}
	running := serveProxy(t, upstream, sink)

	var dialer net.Dialer

	client, err := dialer.DialContext(t.Context(), "tcp4", running.proxy.Addr())
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}

	defer func() { _ = client.Close() }()

	if _, err := client.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x2f, 0x01, 0x00, 0x00}); err != nil {
		t.Fatalf("write the ClientHello: %v", err)
	}

	select {
	case serveErr := <-running.done:
		if !errors.Is(serveErr, pg.ErrDirectTLS) {
			t.Errorf("Serve() = %v, want ErrDirectTLS", serveErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() = <still running>, want ErrDirectTLS within 2s")
	}

	if count := sink.records.Load(); count != 0 {
		t.Errorf("the sink holds %d observations, want 0", count)
	}

	if count := read.Load(); count != 0 {
		t.Errorf("the upstream read %d bytes, want 0: nothing of a TLS client is forwarded", count)
	}
}

// TestRequireClientFailsTheRun fails a start whose client would only take TLS: every connection it
// makes is refused, no statement is ever sent, and the handler would read as having done nothing.
func TestRequireClientFailsTheRun(t *testing.T) {
	t.Parallel()

	for _, client := range []struct {
		connect func(t *testing.T, addr string)
		name    string
	}{
		{name: "libpq", connect: func(t *testing.T, addr string) {
			t.Helper()

			var dialer net.Dialer

			conn, err := dialer.DialContext(t.Context(), "tcp4", addr)
			if err != nil {
				t.Fatalf("dial the proxy: %v", err)
			}

			defer func() { _ = conn.Close() }()

			if _, err := conn.Write(encryptionRequest(sslRequestCode)); err != nil {
				t.Fatalf("write the SSLRequest: %v", err)
			}

			if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}

			reply := make([]byte, 1)
			if _, err := io.ReadFull(conn, reply); err != nil || reply[0] != 'N' {
				t.Fatalf("reply = %q, %v; want N within 1s", reply, err)
			}
		}},
		{name: "pgx", connect: func(t *testing.T, addr string) {
			t.Helper()

			conn, err := pgx.Connect(t.Context(),
				"postgres://stutter:stutter@"+addr+"/stutter?sslmode=require&connect_timeout=2")
			if err == nil {
				_ = conn.Close(context.Background())

				t.Fatal("pgx connected with sslmode=require through a proxy that refuses TLS")
			}
		}},
	} {
		t.Run(client.name, func(t *testing.T) {
			t.Parallel()

			upstream, _ := drainingUpstream(t)
			sink := &countingSink{}
			running := serveProxy(t, upstream, sink)

			client.connect(t, running.proxy.Addr())

			serveErr := closeServed(t, running)
			if !errors.Is(serveErr, pg.ErrTLSRequired) {
				t.Fatalf("Serve() = %v, want ErrTLSRequired", serveErr)
			}

			for _, want := range []string{"sslmode=require or stricter", "not supported", "sslmode must change"} {
				if !strings.Contains(serveErr.Error(), want) {
					t.Errorf("Serve() = %q, want it to say %q", serveErr, want)
				}
			}

			if count := sink.records.Load(); count != 0 {
				t.Errorf("the sink holds %d observations, want 0", count)
			}
		})
	}
}
