package http_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

const (
	// heldFor outlasts the header bound, the point at which the stub used to stop on a silent client.
	heldFor = 1500 * time.Millisecond
	// quietFor is how long a row that must not stop is watched after its client is done.
	quietFor = 300 * time.Millisecond
	// isolated is how quickly a request is answered beside a held connection: half the first-request
	// bound, which is what a held connection judged in Accept would cost it. A 200 ms bound failed at
	// 256 ms under load with nothing held up.
	isolated = stubHeaderBound / 2
)

// stubUnderTest is one fresh stub, its run begun.
type stubUnderTest struct {
	proxy *proxyhttp.Proxy
	done  <-chan error
	sink  *sink
	trust *x509.CertPool
	run   *proxyhttp.Run
	// catchAll is the catch-all entry's address, when the stub has one.
	catchAll string
}

// dialled is where a client of the stub connects: its catch-all entry when it has one, else its own
// address.
func (s *stubUnderTest) dialled() string {
	if s.catchAll != "" {
		return s.catchAll
	}

	return s.proxy.Addr()
}

// cleartextStub starts a fresh cleartext stub.
func cleartextStub(t *testing.T) *stubUnderTest {
	t.Helper()

	under := &stubUnderTest{sink: &sink{}}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	under.proxy, under.done = startProxy(t, under.sink, script)

	return begun(t, under, script)
}

// tlsStub starts a fresh TLS stub.
func tlsStub(t *testing.T) *stubUnderTest {
	t.Helper()

	under := &stubUnderTest{sink: &sink{}}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	under.proxy, under.done, under.trust = startTLSProxy(t, under.sink, script)

	return begun(t, under, script)
}

// begun begins the stub's run.
func begun(t *testing.T, under *stubUnderTest, script *proxyhttp.Script) *stubUnderTest {
	t.Helper()

	run, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	under.run = run

	return under
}

// abort ends the stub's run without freezing anything.
func (s *stubUnderTest) abort(t *testing.T) {
	t.Helper()

	if err := s.run.Abort(); err != nil {
		t.Fatal(err)
	}
}

// outcome is one row of the connection-outcome table: what a client does, and whether the run stops
// on it — and as which class — and how many effects it leaves.
type outcome struct {
	client  func(t *testing.T, under *stubUnderTest)
	name    string
	stop    proxyhttp.StopClass
	effects int
}

// expect runs a row's verdict against the stub its client has finished with.
func (o outcome) expect(t *testing.T, under *stubUnderTest) {
	t.Helper()

	if o.stop != 0 {
		stop := asStop(t, awaitServe(t, under.done))
		if stop.Class != o.stop {
			t.Errorf("stop = %v, want class %s", stop, o.stop)
		}

		_ = under.proxy.Close()
	} else {
		select {
		case err := <-under.done:
			t.Fatalf("Serve() stopped on a row that must not stop: %v", err)
		case <-time.After(quietFor):
		}

		closeProxy(t, under.proxy, under.done)
	}

	under.abort(t)

	if got := len(under.sink.all()); got != o.effects {
		t.Errorf("effects = %d, want %d", got, o.effects)
	}
}

// rawClient is one connection to the stub, spoken to a byte at a time.
type rawClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func dialRaw(t *testing.T, address string) *rawClient {
	t.Helper()

	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return &rawClient{conn: conn, reader: bufio.NewReader(conn)}
}

func (c *rawClient) write(t *testing.T, payload string) {
	t.Helper()

	if _, err := c.conn.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
}

// roundTrip writes one request and reads its response's status.
func (c *rawClient) roundTrip(t *testing.T, request string) int {
	t.Helper()

	c.write(t, request)

	if err := c.conn.SetReadDeadline(time.Now().Add(stopWait)); err != nil {
		t.Fatal(err)
	}

	response, err := http.ReadResponse(c.reader, nil)
	if err != nil {
		t.Fatalf("read the response to %q: %v", request, err)
	}

	_ = response.Body.Close()

	return response.StatusCode
}

func getRequest(path string) string {
	return "GET " + path + " HTTP/1.1\r\nHost: " + serverName + "\r\n\r\n"
}

// served requires a request on a connection of its own to be answered.
func served(t *testing.T, address string) {
	t.Helper()

	if status := dialRaw(t, address).roundTrip(t, getRequest("/other")); status != http.StatusOK {
		t.Errorf("a request beside the row was answered %d", status)
	}
}

// dialTunnel completes a handshake with the TLS stub, asking for serverName.
func dialTunnel(t *testing.T, under *stubUnderTest) *rawClient {
	t.Helper()

	conn := mustDialTLS(t, under.proxy.Addr(), under.trust)

	t.Cleanup(func() { _ = conn.Close() })

	return &rawClient{conn: conn, reader: bufio.NewReader(conn)}
}

// tunnelServed requires a request over a TLS connection of its own to be answered.
func tunnelServed(t *testing.T, under *stubUnderTest) {
	t.Helper()

	if status := dialTunnel(t, under).roundTrip(t, getRequest("/other")); status != http.StatusOK {
		t.Errorf("a request beside the row was answered %d", status)
	}
}

const connectRequest = "CONNECT api.example.test:443 HTTP/1.1\r\nHost: api.example.test:443\r\n\r\n"

func cleartextOutcomes() []outcome {
	return []outcome{
		{name: "O1 closed before its first byte", client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			_ = dialRaw(t, under.proxy.Addr()).conn.Close()

			served(t, under.proxy.Addr())
		}, effects: 1},
		{name: "O2 open with no first byte", client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			held := dialRaw(t, under.proxy.Addr())

			time.Sleep(heldFor)
			served(t, under.proxy.Addr())

			_ = held.conn.Close()
		}, effects: 1},
		{name: "O3 opens TLS", stop: proxyhttp.StopTLSOnCleartext, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			dialRaw(t, under.proxy.Addr()).write(t, "\x16\x03\x01\x00\x00")
		}},
		{name: "O4 O12 a request", effects: 1, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			served(t, under.proxy.Addr())
		}},
		{name: "O10 malformed", stop: proxyhttp.StopNotHTTP, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			dialRaw(t, under.proxy.Addr()).write(t, "NOT HTTP\r\n\r\n")
		}},
		{name: "O10 preface", stop: proxyhttp.StopNotHTTP, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			dialRaw(t, under.proxy.Addr()).write(t, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
		}},
		{name: "O10 header incomplete", stop: proxyhttp.StopNotHTTP, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			dialRaw(t, under.proxy.Addr()).write(t, "GET / HTTP/1.1\r\n")
		}},
		{name: "O11 CONNECT", stop: proxyhttp.StopConnect, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			dialRaw(t, under.proxy.Addr()).write(t, connectRequest)
		}},
		{name: "O13 idle between requests", effects: 2, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			client := dialRaw(t, under.proxy.Addr())
			client.roundTrip(t, getRequest("/first"))
			time.Sleep(heldFor)

			if status := client.roundTrip(t, getRequest("/second")); status != http.StatusOK {
				t.Errorf("the request after an idle spell was answered %d", status)
			}
		}},
		{
			name:    "O14 a later request net/http cannot parse",
			effects: 1,
			client: func(t *testing.T, under *stubUnderTest) {
				t.Helper()

				client := dialRaw(t, under.proxy.Addr())
				client.roundTrip(t, getRequest("/first"))

				if status := client.roundTrip(t, "NOT HTTP\r\n\r\n"); status != http.StatusBadRequest {
					t.Errorf("a malformed later request was answered %d, want net/http's 400", status)
				}
			},
		},
	}
}

// TestStubOutcomeTable: every way a connection can reach the stub has one outcome — recorded, ignored,
// held, or a stop naming its class — so a correct client never stops the run and an unobservable one
// always does.
func TestStubOutcomeTable(t *testing.T) {
	t.Parallel()

	t.Run("80", func(t *testing.T) {
		t.Parallel()

		for _, row := range cleartextOutcomes() {
			t.Run(row.name, func(t *testing.T) {
				t.Parallel()

				under := cleartextStub(t)
				row.client(t, under)
				row.expect(t, under)
			})
		}
	})

	t.Run("443", func(t *testing.T) {
		t.Parallel()

		for _, row := range tlsOutcomes() {
			t.Run(row.name, func(t *testing.T) {
				t.Parallel()

				under := tlsStub(t)
				row.client(t, under)
				row.expect(t, under)
			})
		}
	})
}

func tlsOutcomes() []outcome {
	inTunnel := func(payload string) func(t *testing.T, under *stubUnderTest) {
		return func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			dialTunnel(t, under).write(t, payload)
		}
	}

	return append([]outcome{
		{name: "O1 closed before its first byte", effects: 1, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			_ = dialRaw(t, under.proxy.Addr()).conn.Close()

			tunnelServed(t, under)
		}},
		{name: "O2 open with no first byte", effects: 1, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			held := dialRaw(t, under.proxy.Addr())

			time.Sleep(heldFor)
			tunnelServed(t, under)

			_ = held.conn.Close()
		}},
		{name: "O4 cleartext", stop: proxyhttp.StopCleartextOnTLS, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			dialRaw(t, under.proxy.Addr()).write(t, getRequest("/"))
		}},
		{name: "O5 handshake stalls", stop: proxyhttp.StopHandshake, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			dialRaw(t, under.proxy.Addr()).write(t, "\x16")
		}},
		{
			name: "O5 certificate rejected",
			stop: proxyhttp.StopHandshake,
			client: func(t *testing.T, under *stubUnderTest) {
				t.Helper()

				if conn, err := dialTLS(t, under.proxy.Addr(), x509.NewCertPool()); err == nil {
					_ = conn.Close()
				}
			},
		},
		{name: "O6 h2 only", stop: proxyhttp.StopALPN, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			if conn, err := dialTLS(t, under.proxy.Addr(), under.trust, "h2"); err == nil {
				_ = conn.Close()
			}
		}},
	}, tlsHandshakenOutcomes(inTunnel)...)
}

// tlsHandshakenOutcomes are the rows of a TLS connection whose handshake completed.
func tlsHandshakenOutcomes(inTunnel func(string) func(*testing.T, *stubUnderTest)) []outcome {
	return []outcome{
		{name: "O7 idle after its handshake", effects: 1, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			idle := dialTunnel(t, under)

			time.Sleep(heldFor)
			tunnelServed(t, under)

			_ = idle.conn.Close()
		}},
		{
			name: "O8 closed with no request",
			stop: proxyhttp.StopNoRequest,
			client: func(t *testing.T, under *stubUnderTest) {
				t.Helper()

				_ = dialTunnel(t, under).conn.Close()
			},
		},
		{name: "O9 open at the teardown point", client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			open := dialTunnel(t, under)

			under.proxy.MarkTeardown()

			_ = open.conn.Close()
		}},
		{name: "O10 malformed", stop: proxyhttp.StopNotHTTP, client: inTunnel("NOT HTTP\r\n\r\n")},
		{name: "O10 preface", stop: proxyhttp.StopNotHTTP, client: inTunnel("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")},
		{name: "O10 header incomplete", stop: proxyhttp.StopNotHTTP, client: inTunnel("GET / HTTP/1.1\r\n")},
		{name: "O11 CONNECT", stop: proxyhttp.StopConnect, client: inTunnel(connectRequest)},
		{name: "O12 a request", effects: 1, client: func(t *testing.T, under *stubUnderTest) {
			t.Helper()

			tunnelServed(t, under)
		}},
	}
}

// TestAHeldConnectionDelaysNoOtherRequest: a connection that has sent nothing is judged on its own
// goroutine, never in Accept, so a request beside it is answered at once.
func TestAHeldConnectionDelaysNoOtherRequest(t *testing.T) {
	t.Parallel()

	t.Run("80", func(t *testing.T) {
		t.Parallel()

		under := cleartextStub(t)
		held := dialRaw(t, under.proxy.Addr())
		started := time.Now()

		served(t, under.proxy.Addr())

		elapsed := time.Since(started)

		t.Logf("answered beside a held connection in %s", elapsed)

		if elapsed >= isolated {
			t.Errorf("a request beside a held connection took %s, want under %s", elapsed, isolated)
		}

		_ = held.conn.Close()

		outcome{effects: 1}.expect(t, under)
	})

	for _, shape := range []struct {
		hold func(t *testing.T, under *stubUnderTest) *rawClient
		name string
	}{
		{name: "443 before its first byte", hold: func(t *testing.T, under *stubUnderTest) *rawClient {
			t.Helper()

			return dialRaw(t, under.proxy.Addr())
		}},
		{name: "443 after its handshake", hold: dialTunnel},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			under := tlsStub(t)
			held := shape.hold(t, under)
			started := time.Now()

			tunnelServed(t, under)

			elapsed := time.Since(started)

			t.Logf("answered beside a held connection in %s", elapsed)

			if elapsed >= isolated {
				t.Errorf("a request beside a held connection took %s, want under %s", elapsed, isolated)
			}

			_ = held.conn.Close()

			outcome{effects: 1}.expect(t, under)
		})
	}
}

// TestAConnectRequestStopsTheRun: a CONNECT tunnel hides the request inside it, so recording the
// CONNECT would be a wrong observation. It stops the run, first request or later.
func TestAConnectRequestStopsTheRun(t *testing.T) {
	t.Parallel()

	for _, shape := range []struct {
		name    string
		before  int
		effects int
	}{{name: "first request", before: 0, effects: 0}, {name: "after a request", before: 1, effects: 1}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			under := cleartextStub(t)
			client := dialRaw(t, under.proxy.Addr())

			for range shape.before {
				client.roundTrip(t, getRequest("/before"))
			}

			client.write(t, connectRequest)

			stop := asStop(t, awaitServe(t, under.done))
			if stop.Class != proxyhttp.StopConnect || stop.Name != "api.example.test:443" {
				t.Errorf("stop = %v, want connect naming api.example.test:443", stop)
			}

			if got := len(under.sink.all()); got != shape.effects {
				t.Errorf("effects = %d, want %d", got, shape.effects)
			}

			under.abort(t)
		})
	}
}

// TestCloseContextNeverWaitsOnAHeldConnection: a connection the stub is holding is closed with it, so
// an idle one never presents as a teardown that hangs. net/http would wait 5 s for one that finished
// its handshake and sent nothing.
func TestCloseContextNeverWaitsOnAHeldConnection(t *testing.T) {
	t.Parallel()

	for _, shape := range []struct {
		start func(t *testing.T) *stubUnderTest
		hold  func(t *testing.T, under *stubUnderTest) *rawClient
		serve func(t *testing.T, under *stubUnderTest)
		name  string
	}{
		{
			name:  "80",
			start: cleartextStub,
			hold: func(t *testing.T, under *stubUnderTest) *rawClient {
				t.Helper()

				return dialRaw(t, under.proxy.Addr())
			},
			serve: func(t *testing.T, under *stubUnderTest) {
				t.Helper()

				served(t, under.proxy.Addr())
			},
		},
		{name: "443 after its handshake", start: tlsStub, hold: dialTunnel, serve: tunnelServed},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			under := shape.start(t)
			held := shape.hold(t, under)

			// Accepted before the close begins.
			shape.serve(t, under)

			closeHolding(t, under, held)
		})
	}
}

// closeHolding closes the stub while held is open, and requires the close to be prompt and to close
// held too.
func closeHolding(t *testing.T, under *stubUnderTest, held *rawClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	started := time.Now()
	err := under.proxy.CloseContext(ctx)
	elapsed := time.Since(started)

	t.Logf("CloseContext returned after %s", elapsed)

	if err != nil || elapsed >= time.Second {
		t.Errorf("CloseContext() = %v after %s, want nil within 1s", err, elapsed)
	}

	if err := held.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	if _, err := held.reader.ReadByte(); err == nil || isTimeout(err) {
		t.Errorf("the held connection read %v after CloseContext, want it closed", err)
	}

	under.abort(t)
}

// TestTheTeardownPointEndsZeroRequestStops: a connection still open when the service is removed
// carried no request and hides nothing; one that closed before, with no request, may be a client
// that cannot talk to the stub.
func TestTheTeardownPointEndsZeroRequestStops(t *testing.T) {
	t.Parallel()

	for _, shape := range []struct {
		name  string
		stop  proxyhttp.StopClass
		after bool
	}{{name: "closed after the mark", after: true}, {name: "closed before the mark", stop: proxyhttp.StopNoRequest}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			under := tlsStub(t)
			open := dialTunnel(t, under)

			if shape.after {
				under.proxy.MarkTeardown()
			}

			_ = open.conn.Close()

			outcome{stop: shape.stop}.expect(t, under)
		})
	}
}

// TestATLSClientResumesItsSession: every connection shares one TLS configuration, so a client that
// keeps sessions resumes one, as it would against the real dependency.
func TestATLSClientResumesItsSession(t *testing.T) {
	t.Parallel()

	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			t.Parallel()

			under := tlsStub(t)
			client := &http.Client{Timeout: stopWait, Transport: &http.Transport{
				DisableKeepAlives: true,
				TLSClientConfig: &tls.Config{
					RootCAs:            under.trust,
					ServerName:         serverName,
					MinVersion:         version,
					MaxVersion:         version,
					ClientSessionCache: tls.NewLRUClientSessionCache(1),
				},
			}}

			resumed := make([]bool, 0, 2)

			for range 2 {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
					"https://"+under.proxy.Addr()+"/session", nil)
				if err != nil {
					t.Fatal(err)
				}

				response, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}

				_ = response.Body.Close()

				resumed = append(resumed, response.TLS.DidResume)
			}

			if !resumed[1] {
				t.Errorf("DidResume = %v, want the second connection to resume the first's session", resumed)
			}

			outcome{effects: 2}.expect(t, under)
		})
	}
}

// TestAnALPNStopNamesTheServerName: TLS 1.3 records the server name only after ALPN is settled, so a
// stop on an h2-only client names it only because the name is taken from the hello itself.
func TestAnALPNStopNamesTheServerName(t *testing.T) {
	t.Parallel()

	for _, version := range []uint16{tls.VersionTLS13, tls.VersionTLS12} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			t.Parallel()

			under := tlsStub(t)
			dialer := tls.Dialer{
				NetDialer: &net.Dialer{Timeout: time.Second},
				Config: &tls.Config{
					RootCAs:    under.trust,
					ServerName: serverName,
					NextProtos: []string{"h2"},
					MinVersion: version,
					MaxVersion: version,
				},
			}

			if conn, err := dialer.DialContext(t.Context(), "tcp", under.proxy.Addr()); err == nil {
				_ = conn.Close()
			}

			stop := asStop(t, awaitServe(t, under.done))
			if stop.Class != proxyhttp.StopALPN || stop.Name != serverName || !strings.Contains(stop.Detail, "[h2]") {
				t.Errorf("stop = %v, want alpn naming %s and the offered [h2]", stop, serverName)
			}

			under.abort(t)
		})
	}
}

func isTimeout(err error) bool {
	network, isNetwork := errors.AsType[net.Error](err)

	return isNetwork && network.Timeout()
}

const (
	// pooledRuns is how many fresh stubs each pooled shape is tried against.
	pooledRuns = 20
	// stopWatch is how long a pooled run is watched for a stop once the client has closed its pool: a
	// close is judged at once, so a stop it causes is already on its way.
	stopWatch = 100 * time.Millisecond
)

// pooledShape is a Go client's traffic against one fresh stub: warm requests, then fan concurrent
// ones, then idle for settle with the client's pool as the Transport left it.
type pooledShape struct {
	stub        func(t *testing.T) *stubUnderTest
	name        string
	url         string
	idleTimeout time.Duration
	settle      time.Duration
	warm, fan   int
}

// stopsOn runs shape against one fresh stub and reports whether the stub stopped the run.
func (shape pooledShape) stopsOn(t *testing.T) bool {
	t.Helper()

	under := shape.stub(t)
	address := under.dialled()
	client, transport := goClient(under.trust, func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, address)
	})

	if shape.idleTimeout > 0 {
		transport.IdleConnTimeout = shape.idleTimeout
	}

	fanOut(t, client, shape.url, shape.warm, shape.fan)
	time.Sleep(shape.settle)
	transport.CloseIdleConnections()

	defer under.abort(t)

	select {
	case err := <-under.done:
		t.Logf("stopped: %v", err)

		return true
	case <-time.After(stopWatch):
		closeProxy(t, under.proxy, under.done)

		return false
	}
}

// TestAGoClientsPooledConnectionsNeverStopTheRun: Go's http.Transport leaves a connection it dialled
// and never used in its pool, and past its idle limit closes one at once. Neither carried a request,
// and beside a served connection to the same name neither hides one, so neither stops the run.
func TestAGoClientsPooledConnectionsNeverStopTheRun(t *testing.T) {
	t.Parallel()

	tlsURL := "https://" + serverName + "/fan"

	for _, shape := range append([]pooledShape{
		{name: "443 warm1 fan2 pooled idle", stub: tlsStub, url: tlsURL, warm: 1, fan: 2, settle: poolSettle},
		{name: "443 warm2 fan3 overflow close", stub: tlsStub, url: tlsURL, warm: 2, fan: 3, settle: poolSettle},
		{
			name: "443 idle timeout mid-run", stub: tlsStub, url: tlsURL, warm: 1, fan: 2,
			idleTimeout: 200 * time.Millisecond, settle: 500 * time.Millisecond,
		},
	}, catchAllPooledShapes()...) {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			stopped := runPooled(t, shape)

			t.Logf("shape=%s stopped=%d/%d", shape.name, stopped, pooledRuns)

			if stopped != 0 {
				t.Errorf("stopped=%d/%d: a Go client's pooled connections stopped the run", stopped, pooledRuns)
			}
		})
	}

	t.Run("443 control: no request for the name", func(t *testing.T) {
		t.Parallel()

		under := tlsStub(t)

		_ = dialTunnel(t, under).conn.Close()

		outcome{stop: proxyhttp.StopNoRequest}.expect(t, under)
	})
}

// TestServedIsForgottenBetweenRuns: that a name was served is known for one run only. A later run's
// stub starts knowing nothing, so a client there that never gets a request through still stops it.
func TestServedIsForgottenBetweenRuns(t *testing.T) {
	t.Parallel()

	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	first := begun(t, &stubUnderTest{sink: &sink{}}, script)
	first.proxy, first.done, first.trust = startTLSProxy(t, first.sink, script)

	tunnelServed(t, first)
	closeProxy(t, first.proxy, first.done)

	if err := first.run.Commit(); err != nil {
		t.Fatal(err)
	}

	second := begun(t, &stubUnderTest{sink: &sink{}}, script)
	second.proxy, second.done, second.trust = startTLSProxy(t, second.sink, script)

	_ = dialTunnel(t, second).conn.Close()

	outcome{stop: proxyhttp.StopNoRequest}.expect(t, second)
}
