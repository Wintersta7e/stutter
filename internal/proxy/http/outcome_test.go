package http_test

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

const (
	// heldFor outlasts the header bound, the point at which the stub used to stop on a silent client.
	heldFor = 1500 * time.Millisecond
	// quietFor is how long a row that must not stop is watched after its client is done.
	quietFor = 300 * time.Millisecond
	// isolated is how quickly a request is answered beside a held connection.
	isolated = 200 * time.Millisecond
)

// stubUnderTest is one fresh stub, its run begun.
type stubUnderTest struct {
	proxy *proxyhttp.Proxy
	done  <-chan error
	sink  *sink
	run   *proxyhttp.Run
}

// cleartextStub starts a fresh cleartext stub.
func cleartextStub(t *testing.T) *stubUnderTest {
	t.Helper()

	under := &stubUnderTest{sink: &sink{}}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	under.proxy, under.done = startProxy(t, under.sink, script)

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
// an idle one never presents as a teardown that hangs.
func TestCloseContextNeverWaitsOnAHeldConnection(t *testing.T) {
	t.Parallel()

	under := cleartextStub(t)
	held := dialRaw(t, under.proxy.Addr())

	// Accepted before the close begins.
	served(t, under.proxy.Addr())

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

func isTimeout(err error) bool {
	network, isNetwork := errors.AsType[net.Error](err)

	return isNetwork && network.Timeout()
}
