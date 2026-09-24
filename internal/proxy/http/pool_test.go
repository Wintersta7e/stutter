package http_test

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

// Go's http.Transport dials a connection for a request that is waiting, and when an earlier request
// frees its connection first the waiting request takes that one instead. The fresh connection has
// completed its handshake and goes into the idle pool having sent nothing. Once another connection to
// the same server name has been served, such a connection is the client's own bookkeeping, not a
// client that cannot talk to the stub, and stopping the run on it fails every service that makes
// concurrent HTTPS calls.
func TestAnUnusedPooledConnectionIsNotAStop(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done, trust := startTLSProxy(t, current, script)

	run, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	served := sendInTunnel(t, proxy.Addr(), trust,
		"GET /served HTTP/1.1\r\nHost: "+serverName+"\r\nConnection: close\r\n\r\n")
	_ = served.Close()

	unused := mustDialTLS(t, proxy.Addr(), trust)

	select {
	case serveErr := <-done:
		t.Fatalf("Serve() stopped on a pooled connection after a served one: %v", serveErr)
	case <-time.After(stopWait):
	}

	_ = unused.Close()

	// A connection that closes having sent nothing is judged at once, so a stop it caused is here by now.
	select {
	case serveErr := <-done:
		t.Fatalf("Serve() stopped when a pooled connection closed after a served one: %v", serveErr)
	case <-time.After(quietFor):
	}

	if abortErr := run.Abort(); abortErr != nil {
		t.Fatal(abortErr)
	}

	closeProxy(t, proxy, done)

	if got := len(current.all()); got != 1 {
		t.Errorf("observations = %d, want only the served request", got)
	}
}

// The same case through a real Go client: a warm request, then concurrent ones to the same host.
// Measured against the stub before this rule: every one of these shapes stopped the run.
func TestConcurrentHTTPSFromAGoClientIsServed(t *testing.T) {
	t.Parallel()

	for _, shape := range []struct {
		name       string
		warm, fans int
	}{{"one then two", 1, 2}, {"two then four", 2, 4}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()

			script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
			proxy, done, trust := startTLSProxy(t, &sink{}, script)

			run, err := script.Begin()
			if err != nil {
				t.Fatal(err)
			}

			transport := &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: trust, ServerName: serverName, MinVersion: tls.VersionTLS12},
			}
			client := &http.Client{Transport: transport, Timeout: stopWait}

			burst(t, client, "https://"+proxy.Addr()+"/fan", shape.warm)
			burst(t, client, "https://"+proxy.Addr()+"/fan", shape.fans)

			select {
			case serveErr := <-done:
				t.Fatalf("Serve() stopped a Go client fanning out: %v", serveErr)
			case <-time.After(stopWait):
			}

			transport.CloseIdleConnections()

			if abortErr := run.Abort(); abortErr != nil {
				t.Fatal(abortErr)
			}

			closeProxy(t, proxy, done)
		})
	}
}

// A stop's text is compared across runs, so it must not name the connection's ephemeral port.
func TestATLSStopNamesNoPeerAddress(t *testing.T) {
	t.Parallel()

	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done, _ := startTLSProxy(t, &sink{}, script)

	// A handshake that opens and then stalls, so the stop comes from inside crypto/tls, whose errors
	// carry the connection's addresses.
	stalled, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", proxy.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stalled.Close() }()

	if _, err := stalled.Write([]byte{0x16}); err != nil {
		t.Fatal(err)
	}

	select {
	case serveErr := <-done:
		if serveErr == nil || strings.Contains(serveErr.Error(), "127.0.0.1") {
			t.Fatalf("Serve() error = %v, want a stop that names no peer address", serveErr)
		}
	case <-time.After(stopWait + stubHeaderBound):
		t.Fatalf("Serve did not stop within %v", stopWait+stubHeaderBound)
	}
}

// burst issues n concurrent GETs and waits for all of them, failing on any error.
func burst(t *testing.T, client *http.Client, url string, n int) {
	t.Helper()

	var requests sync.WaitGroup

	for range n {
		requests.Go(func() {
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
			if err != nil {
				t.Error(err)

				return
			}

			response, err := client.Do(request)
			if err != nil {
				t.Error(err)

				return
			}

			if _, err := io.Copy(io.Discard, response.Body); err != nil {
				t.Error(err)
			}

			_ = response.Body.Close()
		})
	}

	requests.Wait()
}
