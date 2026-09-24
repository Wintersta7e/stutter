package http_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// poolSettle is how long a shape's connections are left alone after its last response before they are
// counted: long enough for an overflow close to reach the server, short of any idle timeout.
const poolSettle = 200 * time.Millisecond

// dialFunc reaches an address the way a client's own resolver would not, as the stub's clients must.
type dialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// countedConn counts the raw bytes a server read from one connection, before any TLS layer.
type countedConn struct {
	net.Conn

	read atomic.Int64
}

func (c *countedConn) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	c.read.Add(int64(n))

	return n, err
}

// countingListener keeps every connection it accepted, keyed by the client's address, which the TLS
// layer above it leaves unchanged.
type countingListener struct {
	net.Listener

	accepted map[string]*countedConn
	mu       sync.Mutex
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	counted := &countedConn{Conn: conn}

	l.mu.Lock()
	l.accepted[conn.RemoteAddr().String()] = counted
	l.mu.Unlock()

	return counted, nil
}

// counting is a server that tells which of its connections ever carried a request.
type counting struct {
	server   *httptest.Server
	listener *countingListener
	active   map[string]bool
	closed   map[string]bool
	mu       sync.Mutex
}

// tally is what one shape left behind: connections that never carried a request, how many of those
// the server read no byte from, and how many of those the client had already closed.
type tally struct {
	unused, zeroByte, closed int
}

// countingServer starts a server that answers at once and records, per connection, whether a request
// ever arrived on it and whether it has closed. start is (*httptest.Server).Start or StartTLS.
func countingServer(t *testing.T, start func(*httptest.Server)) *counting {
	t.Helper()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	current := &counting{
		server:   server,
		listener: &countingListener{Listener: server.Listener, accepted: make(map[string]*countedConn)},
		active:   make(map[string]bool),
		closed:   make(map[string]bool),
	}

	server.Listener = current.listener
	server.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		current.mu.Lock()
		defer current.mu.Unlock()

		if state == http.StateActive {
			current.active[conn.RemoteAddr().String()] = true
		}

		if state == http.StateClosed || state == http.StateHijacked {
			current.closed[conn.RemoteAddr().String()] = true
		}
	}

	start(server)

	t.Cleanup(server.Close)

	return current
}

// unused counts the connections that never carried a request.
func (c *counting) unused() tally {
	c.listener.mu.Lock()
	defer c.listener.mu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	var counted tally

	for address, conn := range c.listener.accepted {
		if c.active[address] {
			continue
		}

		counted.unused++

		if conn.read.Load() == 0 {
			counted.zeroByte++
		}

		if c.closed[address] {
			counted.closed++
		}
	}

	return counted
}

// goClient is a client as a Go service builds one: the default transport, trusting trust when it is
// set, and dialling through dial when it is.
func goClient(trust *x509.CertPool, dial dialFunc) (*http.Client, *http.Transport) {
	defaults, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		panic("http.DefaultTransport is not an *http.Transport")
	}

	transport := defaults.Clone()
	if trust != nil {
		transport.TLSClientConfig = &tls.Config{RootCAs: trust, MinVersion: tls.VersionTLS12}
	}

	if dial != nil {
		transport.DialContext = dial
	}

	return &http.Client{Transport: transport, Timeout: stopWait}, transport
}

// fanOut issues warm concurrent GETs, then fan more, each body drained: a service that has made a
// call or two and then makes several at once.
func fanOut(t *testing.T, client *http.Client, url string, warm, fan int) {
	t.Helper()

	burst(t, client, url, warm)
	burst(t, client, url, fan)
}

// TestAGoClientPoolsAConnectionItNeverUsed records the premise the stub's served-connection exemption
// rests on. Go's http.Transport dials for every waiting request; a request that another's freed
// connection serves first leaves its dial unused, in the idle pool or, past MaxIdleConnsPerHost,
// closed at once. Such a connection carried no request, and a stub that stopped on it would stop every
// correct Go service that makes concurrent calls.
func TestAGoClientPoolsAConnectionItNeverUsed(t *testing.T) {
	t.Parallel()

	const iterations = 20

	for _, shape := range []struct {
		name       string
		warm, fan  int
		secure     bool
		mustUnused bool
	}{
		{name: "cleartext warm1 fan2", warm: 1, fan: 2},
		{name: "cleartext warm2 fan3", warm: 2, fan: 3},
		{name: "TLS warm1 fan2", warm: 1, fan: 2, secure: true, mustUnused: true},
		{name: "TLS warm2 fan3", warm: 2, fan: 3, secure: true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			var runs, total tally

			for range iterations {
				var trust *x509.CertPool

				start := (*httptest.Server).Start
				if shape.secure {
					start = (*httptest.Server).StartTLS
				}

				server := countingServer(t, start)

				if shape.secure {
					trust = x509.NewCertPool()
					trust.AddCert(server.server.Certificate())
				}

				client, transport := goClient(trust, nil)
				fanOut(t, client, server.server.URL+"/probe", shape.warm, shape.fan)

				time.Sleep(poolSettle)

				counted := server.unused()

				transport.CloseIdleConnections()
				server.server.Close()

				total.unused += counted.unused
				total.zeroByte += counted.zeroByte
				total.closed += counted.closed

				if counted.unused > 0 {
					runs.unused++
				}
			}

			t.Logf("shape=%s runs-with-unused=%d/%d unused=%d zero-byte=%d closed=%d",
				shape.name, runs.unused, iterations, total.unused, total.zeroByte, total.closed)

			if shape.mustUnused && runs.unused < 1 {
				t.Errorf("runs-with-unused=0: Go no longer pools an unused dial; "+
					"the stub's served-connection exemption may be narrowed (shape %s)", shape.name)
			}
		})
	}
}
