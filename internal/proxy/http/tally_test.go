package http_test

import (
	"bufio"
	"crypto/tls"
	"net/http"
	"reflect"
	"testing"

	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

// Hosts the tally tests call.
const (
	hostA = "a.test"
	hostB = "b.test"
)

// tallyStub is a stub with a cleartext and a TLS entry over script, and the TLS entry's address.
func tallyStub(t *testing.T, script *proxyhttp.Script) (*stubUnderTest, string) {
	t.Helper()

	certificate, trust := selfSigned(t)
	present := func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &certificate, nil }
	secure := listen(t)
	under := &stubUnderTest{sink: &sink{}, trust: trust}

	proxy, err := proxyhttp.New(
		proxyhttp.Entries{Cleartext: listen(t), TLS: secure}, "logical.test", under.sink, script, present,
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- proxy.Serve(t.Context()) }()

	under.proxy, under.done = proxy, done

	return under, secure.Addr().String()
}

// call makes one request for host and path over a connection of its own.
func call(t *testing.T, client *rawClient, host, path string) {
	t.Helper()

	if status := client.roundTrip(t, "GET "+path+" HTTP/1.1\r\nHost: "+host+"\r\n\r\n"); status != http.StatusOK {
		t.Errorf("GET %s%s = %d", host, path, status)
	}
}

// TestTallyCountsTheCaptureRunAndEveryOffScriptCall: the report names every external host the stub
// answered — how often, how many from a declared route, how many off the script — from what the check
// kept: the capture run, and every committed run's off-script calls. An aborted run counts nothing.
func TestTallyCountsTheCaptureRunAndEveryOffScriptCall(t *testing.T) {
	t.Parallel()

	script := proxyhttp.NewScript(proxyhttp.Response{}, []proxyhttp.Route{
		{Host: hostA, Path: "/routed", Response: proxyhttp.Response{StatusCode: http.StatusOK}},
	})
	under, secure := tallyStub(t, script)

	capture := begun(t, under, script)
	call(t, dialRaw(t, under.proxy.Addr()), hostA, "/routed")
	call(t, dialRaw(t, under.proxy.Addr()), hostA, "/plain")
	tunnel := mustDialTLS(t, secure, under.trust)
	call(t, &rawClient{conn: tunnel, reader: bufio.NewReader(tunnel)}, hostB, "/secure")
	_ = tunnel.Close()

	if err := capture.run.Commit(); err != nil {
		t.Fatal(err)
	}

	aborted := begun(t, under, script)
	for range 3 {
		call(t, dialRaw(t, under.proxy.Addr()), hostA, "/aborted")
	}

	aborted.abort(t)

	replay := begun(t, under, script)
	call(t, dialRaw(t, under.proxy.Addr()), hostA, "/off-script")

	if err := replay.run.Commit(); err != nil {
		t.Fatal(err)
	}

	closeProxy(t, under.proxy, under.done)

	want := []proxyhttp.HostTally{
		{Host: hostA, Calls: 2, Routed: 1, OffScript: 1},
		{Host: hostB, Calls: 1, TLS: true},
	}
	if got := script.Tally(); !reflect.DeepEqual(got, want) {
		t.Errorf("Tally() = %+v, want %+v", got, want)
	}
}

// TestSumTalliesAddsPerHost: a check's tally is its consumer checks' added up per host, TLS if any of
// them saw it, in host order.
func TestSumTalliesAddsPerHost(t *testing.T) {
	t.Parallel()

	got := proxyhttp.SumTallies(
		[]proxyhttp.HostTally{{Host: hostB, Calls: 1, Routed: 1}},
		nil,
		[]proxyhttp.HostTally{
			{Host: hostA, Calls: 2, OffScript: 1, TLS: true},
			{Host: hostB, Calls: 2, OffScript: 3},
		},
	)
	want := []proxyhttp.HostTally{
		{Host: hostA, Calls: 2, OffScript: 1, TLS: true},
		{Host: hostB, Calls: 3, Routed: 1, OffScript: 3},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("SumTallies() = %+v, want %+v", got, want)
	}
}
