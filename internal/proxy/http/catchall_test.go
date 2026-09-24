package http_test

import (
	"bufio"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

// catchAllSilence is the stub's silence bound on the catch-all entry.
const catchAllSilence = 5 * time.Second

// taggedConn is a connection that knows the port its client dialled, as a relayed one does.
type taggedConn struct {
	net.Conn

	port uint16
}

func (c taggedConn) DestinationPort() uint16 { return c.port }

// taggedListener tags each connection it accepts with the next of its ports, in accept order; the last
// port tags every connection after it.
type taggedListener struct {
	net.Listener

	ports []uint16
	next  int
	mu    sync.Mutex
}

var _ proxyhttp.Tagged = taggedConn{}

func (l *taggedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	port := l.ports[min(l.next, len(l.ports)-1)]
	l.next++

	return taggedConn{Conn: conn, port: port}, nil
}

// catchAllStub starts a fresh stub whose only entry is a catch-all tagging its connections with ports.
func catchAllStub(t *testing.T, ports ...uint16) *stubUnderTest {
	t.Helper()

	return entriesStub(t, func(catchAll net.Listener) proxyhttp.Entries {
		return proxyhttp.Entries{CatchAll: &taggedListener{Listener: catchAll, ports: ports}}
	})
}

// entriesStub starts a fresh stub over what entries builds around a catch-all listener, trusted
// through a self-signed certificate for serverName.
func entriesStub(t *testing.T, entries func(catchAll net.Listener) proxyhttp.Entries) *stubUnderTest {
	t.Helper()

	certificate, trust := selfSigned(t)
	present := func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &certificate, nil }
	under := &stubUnderTest{sink: &sink{}, trust: trust}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	catchAll := listen(t)
	under.catchAll = catchAll.Addr().String()

	proxy, err := proxyhttp.New(entries(catchAll), "logical.test", under.sink, script, present)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- proxy.Serve(t.Context()) }()

	under.proxy, under.done = proxy, done

	return begun(t, under, script)
}

// catchAllTunnel completes a handshake on the catch-all entry, asking for serverName.
func catchAllTunnel(t *testing.T, under *stubUnderTest, protocols ...string) (*rawClient, error) {
	t.Helper()

	conn, err := dialTLS(t, under.catchAll, under.trust, protocols...)
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() { _ = conn.Close() })

	return &rawClient{conn: conn, reader: bufio.NewReader(conn)}, nil
}

// silent expects a silent stop naming port, prints how long it took from started, and returns the
// stop's text.
func silent(t *testing.T, under *stubUnderTest, port uint16, started time.Time) string {
	t.Helper()

	select {
	case err := <-under.done:
		t.Logf("stopped after %s: %v", time.Since(started), err)

		stop := asStop(t, err)
		if stop.Class != proxyhttp.StopSilent || stop.Port != port {
			t.Errorf("stop = %v, want silent naming port %d", stop, port)
		}

		under.abort(t)

		return stop.Error()
	case <-time.After(catchAllSilence + stopWait):
		t.Fatalf("no stop within %s", catchAllSilence+stopWait)

		return ""
	}
}

// TestCatchAllOutcomes: a connection to any other port the service dials is served when it speaks
// HTTP/1.1, over TLS or not, and otherwise stops the run naming the port — a mail client waiting for a
// greeting included — except one that sent nothing and was still open when the service went away.
func TestCatchAllOutcomes(t *testing.T) {
	t.Parallel()

	for _, row := range catchAllOutcomes() {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			under := catchAllStub(t, row.port)
			row.client(t, under)
		})
	}

	t.Run("an untagged connection", func(t *testing.T) {
		t.Parallel()

		under := entriesStub(t, func(catchAll net.Listener) proxyhttp.Entries {
			return proxyhttp.Entries{CatchAll: catchAll}
		})

		_ = dialRaw(t, under.catchAll)

		err := awaitServe(t, under.done)
		if stop, isStop := errors.AsType[*proxyhttp.EgressStop](err); err == nil || isStop {
			t.Errorf("Serve() = %v (stop %v), want an error that is not an egress stop", err, stop)
		}

		under.abort(t)
	})
}

type catchAllRow struct {
	client func(t *testing.T, under *stubUnderTest)
	name   string
	port   uint16
}

func catchAllOutcomes() []catchAllRow {
	return append(catchAllServed(), catchAllSilent()...)
}

func catchAllServed() []catchAllRow {
	return []catchAllRow{
		{name: "8080 HTTP/1.1", port: 8080, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			request := "GET /rates HTTP/1.1\r\nHost: api.example.test:8080\r\n\r\n"
			dialRaw(t, under.catchAll).roundTrip(t, request)

			effects := under.sink.all()
			if len(effects) != 1 || !strings.HasPrefix(effects[0].Printable, "GET api.example.test:8080/rates") {
				t.Errorf("effects = %+v, want one GET api.example.test:8080/rates", effects)
			}

			outcome{effects: 1}.expect(t, under)
		}},
		{name: "8443 TLS HTTP/1.1", port: 8443, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			client, err := catchAllTunnel(t, under)
			if err != nil {
				t.Fatal(err)
			}

			client.roundTrip(t, getRequest("/rates"))

			outcome{effects: 1}.expect(t, under)
		}},
		{name: "8443 TLS then not HTTP", port: 8443, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			client, err := catchAllTunnel(t, under)
			if err != nil {
				t.Fatal(err)
			}

			client.write(t, "NOT HTTP\r\n\r\n")

			outcome{stop: proxyhttp.StopNotHTTP}.expect(t, under)
		}},
		{name: "8443 h2 only", port: 8443, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			if client, err := catchAllTunnel(t, under, "h2"); err == nil {
				_ = client.conn.Close()
			}

			outcome{stop: proxyhttp.StopALPN}.expect(t, under)
		}},
	}
}

func catchAllSilent() []catchAllRow {
	return []catchAllRow{
		{name: "8025 silent past the bound", port: 8025, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			started := time.Now()
			held := dialRaw(t, under.catchAll)

			silent(t, under, 8025, started)

			if elapsed := time.Since(started); elapsed < catchAllSilence {
				t.Errorf("stopped after %s, want at the bound of %s", elapsed, catchAllSilence)
			}

			_ = held.conn.Close()
		}},
		{name: "8025 closed with zero bytes", port: 8025, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			started := time.Now()

			_ = dialRaw(t, under.catchAll).conn.Close()

			silent(t, under, 8025, started)
		}},
		{name: "587 silent names SMTP", port: 587, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			started := time.Now()
			held := dialRaw(t, under.catchAll)

			if stop := silent(t, under, 587, started); !strings.Contains(stop, "SMTP") {
				t.Errorf("stop = %s, want it to name SMTP", stop)
			}

			_ = held.conn.Close()
		}},
		{name: "byte-less open at the teardown point", port: 8025, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			held := dialRaw(t, under.catchAll)

			time.Sleep(quietFor)
			under.proxy.MarkTeardown()

			_ = held.conn.Close()

			outcome{}.expect(t, under)
		}},
		{name: "TLS idle at the teardown point", port: 8443, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			started := time.Now()

			if _, err := catchAllTunnel(t, under); err != nil {
				t.Fatal(err)
			}

			under.proxy.MarkTeardown()

			silent(t, under, 8443, started)

			if elapsed := time.Since(started); elapsed >= catchAllSilence {
				t.Errorf("stopped after %s, want at the teardown point, before the bound", elapsed)
			}
		}},
	}
}

// catchAllPooledShapes are a Go client's pools on catch-all ports.
func catchAllPooledShapes() []pooledShape {
	cleartext := func(t *testing.T) *stubUnderTest {
		t.Helper()

		return catchAllStub(t, 8080)
	}
	secure := func(t *testing.T) *stubUnderTest {
		t.Helper()

		return catchAllStub(t, 8443)
	}

	return []pooledShape{
		{
			name:   "catch-all cleartext warm1 fan2 held past the bound",
			stub:   cleartext,
			url:    "http://api.example.test:8080/fan",
			warm:   1,
			fan:    2,
			settle: catchAllSilence + time.Second,
		},
		{
			name: "catch-all TLS warm2 fan3 overflow close", stub: secure, url: "https://api.example.test:8443/fan",
			warm: 2, fan: 3, settle: poolSettle,
		},
		{
			name:   "catch-all cleartext warm3 fan6 overflow close",
			stub:   cleartext,
			url:    "http://api.example.test:8080/fan",
			warm:   3,
			fan:    6,
			settle: poolSettle,
		},
	}
}

// TestServedIsKeyedByPort: a request on one port exempts nothing on another, even for the same name.
func TestServedIsKeyedByPort(t *testing.T) {
	t.Parallel()

	t.Run("443 then catch-all 8443", func(t *testing.T) {
		t.Parallel()

		under := entriesStub(t, func(catchAll net.Listener) proxyhttp.Entries {
			return proxyhttp.Entries{
				TLS:      listen(t),
				CatchAll: &taggedListener{Listener: catchAll, ports: []uint16{8443}},
			}
		})

		tunnelServed(t, under)

		client, err := catchAllTunnel(t, under)
		if err != nil {
			t.Fatal(err)
		}

		_ = client.conn.Close()

		outcome{stop: proxyhttp.StopNoRequest, effects: 1}.expect(t, under)
	})

	t.Run("catch-all 8080 then 8081", func(t *testing.T) {
		t.Parallel()

		under := catchAllStub(t, 8080, 8081)

		dialRaw(t, under.catchAll).roundTrip(t, "GET / HTTP/1.1\r\nHost: api.example.test:8080\r\n\r\n")

		_ = dialRaw(t, under.catchAll).conn.Close()

		silent(t, under, 8081, time.Now())

		if len(under.sink.all()) != 1 {
			t.Errorf("effects = %d, want the 8080 request only", len(under.sink.all()))
		}
	})
}

// runPooled runs shape against pooledRuns fresh stubs at once and returns how many stopped.
func runPooled(t *testing.T, shape pooledShape) int {
	t.Helper()

	var stopped atomic.Int64

	t.Run("runs", func(t *testing.T) {
		for run := range pooledRuns {
			t.Run(strconv.Itoa(run), func(t *testing.T) {
				t.Parallel()

				if shape.stopsOn(t) {
					stopped.Add(1)
				}
			})
		}
	})

	return int(stopped.Load())
}
