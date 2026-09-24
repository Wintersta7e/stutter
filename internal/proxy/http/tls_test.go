package http_test

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

const (
	// serverName is the SNI every client in these tests sends; a stop has to name it.
	serverName = "api.example.test"
	// stubHeaderBound is the stub's first-request bound.
	stubHeaderBound = time.Second
	// stopWait outlasts the header bound, so a stop that is coming has arrived.
	stopWait = 3 * stubHeaderBound
	// notHTTP is the reason a first request that is not HTTP/1.x stops the run.
	notHTTP = "not parseable HTTP/1.1"
)

// A client that reaches the TLS stub and then goes quiet, hangs up, or speaks something other than
// HTTP/1.1 used to leave no trace at all: Serve returned nothing, no effect was recorded, and the
// handler read as one that did nothing. Every one of these has to stop the run, naming the host the
// client asked for.
func TestTLSStubStopsOnSilentOutcomes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		client func(t *testing.T, address string, trust *x509.CertPool) net.Conn
		name   string
		want   string
	}{
		{
			name: "h2-only ALPN",
			want: "unsupported application protocols",
			client: func(t *testing.T, address string, trust *x509.CertPool) net.Conn {
				t.Helper()

				// An h2-only client hangs up when no protocol is agreed, as HTTP/2 RPC clients do.
				connection, err := dialTLS(t, address, trust, "h2")
				if err != nil {
					return nil
				}

				_ = connection.Close()

				return nil
			},
		},
		{
			name: "PRI preface",
			want: notHTTP,
			client: func(t *testing.T, address string, trust *x509.CertPool) net.Conn {
				t.Helper()

				return sendInTunnel(t, address, trust, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
			},
		},
		{
			name: "malformed request",
			want: notHTTP,
			client: func(t *testing.T, address string, trust *x509.CertPool) net.Conn {
				t.Helper()

				return sendInTunnel(t, address, trust, "NOT HTTP\r\n\r\n")
			},
		},
		{
			name: "handshake then close",
			want: "sent no request",
			client: func(t *testing.T, address string, trust *x509.CertPool) net.Conn {
				t.Helper()

				connection := mustDialTLS(t, address, trust)
				_ = connection.Close()

				return nil
			},
		},
		{
			name: "handshake then idle",
			want: "sent no request",
			client: func(t *testing.T, address string, trust *x509.CertPool) net.Conn {
				t.Helper()

				// Held open past the header bound; closed only after the stop is seen.
				return mustDialTLS(t, address, trust)
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			current := &sink{}
			script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
			proxy, done, trust := startTLSProxy(t, current, script)

			run, err := script.Begin()
			if err != nil {
				t.Fatal(err)
			}

			held := testCase.client(t, proxy.Addr(), trust)

			awaitStop(t, done, testCase.want)

			if held != nil {
				_ = held.Close()
			}

			if abortErr := run.Abort(); abortErr != nil {
				t.Fatal(abortErr)
			}

			if closeErr := proxy.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}

			if got := len(current.all()); got != 0 {
				t.Errorf("recorded %d effects for a connection that should stop the run", got)
			}
		})
	}

	t.Run("control", func(t *testing.T) {
		t.Parallel()

		current := &sink{}
		script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
		proxy, done, trust := startTLSProxy(t, current, script)

		run, err := script.Begin()
		if err != nil {
			t.Fatal(err)
		}

		// A client offering both protocols is served HTTP/1.1 and recorded like any other request.
		connection, err := dialTLS(t, proxy.Addr(), trust, "h2", "http/1.1")
		if err != nil {
			t.Fatal(err)
		}

		if got := connection.ConnectionState().NegotiatedProtocol; got != "http/1.1" {
			t.Errorf("negotiated protocol = %q, want http/1.1", got)
		}

		request := "GET /control HTTP/1.1\r\nHost: " + serverName + "\r\nConnection: close\r\n\r\n"
		if _, writeErr := connection.Write([]byte(request)); writeErr != nil {
			t.Fatal(writeErr)
		}

		response, err := http.ReadResponse(bufio.NewReader(connection), nil)
		if err != nil {
			t.Fatal(err)
		}

		_ = response.Body.Close()
		_ = connection.Close()

		if response.StatusCode != http.StatusOK || response.ProtoMajor != 1 {
			t.Errorf("response = %s %d, want HTTP/1.x 200", response.Proto, response.StatusCode)
		}

		if commitErr := run.Commit(); commitErr != nil {
			t.Fatal(commitErr)
		}

		closeProxy(t, proxy, done)

		if got := len(current.all()); got != 1 {
			t.Errorf("observations = %d, want 1", got)
		}
	})
}

// A client that completes its handshake and goes quiet must not hold up anyone else: its check runs
// on its own connection, never in Accept, so a concurrent request is served while it waits.
func TestTLSIdleClientDoesNotDelayAnother(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done, trust := startTLSProxy(t, current, script)

	run, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	idle := mustDialTLS(t, proxy.Addr(), trust)
	defer func() { _ = idle.Close() }()

	client := &http.Client{
		Timeout: stopWait,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs:    trust,
			ServerName: serverName,
			MinVersion: tls.VersionTLS12,
		}},
	}
	defer client.CloseIdleConnections()

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+proxy.Addr()+"/busy", nil)
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	response, err := client.Do(request)
	elapsed := time.Since(started)

	if err != nil {
		t.Fatalf("concurrent request failed after %v: %v", elapsed, err)
	}

	_ = response.Body.Close()

	if response.StatusCode != http.StatusOK || elapsed >= stubHeaderBound {
		t.Fatalf("concurrent request = %d after %v, want 200 inside the %v header bound",
			response.StatusCode, elapsed, stubHeaderBound)
	}

	client.CloseIdleConnections()
	awaitStop(t, done, "sent no request")

	if abortErr := run.Abort(); abortErr != nil {
		t.Fatal(abortErr)
	}

	if got := len(current.all()); got != 1 {
		t.Errorf("observations = %d, want only the concurrent request", got)
	}
}

// awaitStop waits for Serve to stop the run and checks that the stop names the client's SNI and
// carries the reason want.
func awaitStop(t *testing.T, done <-chan error, want string) {
	t.Helper()

	select {
	case serveErr := <-done:
		if serveErr == nil || !strings.Contains(serveErr.Error(), serverName) ||
			!strings.Contains(serveErr.Error(), want) {
			t.Fatalf("Serve() error = %v, want a stop naming %q for %q", serveErr, serverName, want)
		}
	case <-time.After(stopWait):
		t.Fatalf("Serve did not stop within %v", stopWait)
	}
}

// sendInTunnel completes a handshake, writes payload inside it, and waits once for any answer.
func sendInTunnel(t *testing.T, address string, trust *x509.CertPool, payload string) net.Conn {
	t.Helper()

	connection := mustDialTLS(t, address, trust)
	if _, err := connection.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}

	if err := connection.SetReadDeadline(time.Now().Add(stopWait)); err != nil {
		t.Fatal(err)
	}

	// Either a reply or the stub hanging up; the caller judges which by what Serve returns.
	if _, err := connection.Read(make([]byte, 512)); err != nil {
		t.Logf("the stub answered by closing: %v", err)
	}

	return connection
}

func mustDialTLS(t *testing.T, address string, trust *x509.CertPool) *tls.Conn {
	t.Helper()

	connection, err := dialTLS(t, address, trust)
	if err != nil {
		t.Fatal(err)
	}

	return connection
}

func dialTLS(t *testing.T, address string, trust *x509.CertPool, protocols ...string) (*tls.Conn, error) {
	t.Helper()

	dialer := tls.Dialer{
		NetDialer: &net.Dialer{Timeout: time.Second},
		Config: &tls.Config{
			RootCAs:    trust,
			ServerName: serverName,
			NextProtos: protocols,
			MinVersion: tls.VersionTLS12,
		},
	}

	connection, err := dialer.DialContext(t.Context(), "tcp", address)
	if err != nil {
		return nil, err
	}

	established, ok := connection.(*tls.Conn)
	if !ok {
		t.Fatalf("tls.Dialer returned %T", connection)
	}

	return established, nil
}

func startTLSProxy(
	t *testing.T,
	current proxyhttp.Sink,
	script *proxyhttp.Script,
) (*proxyhttp.Proxy, <-chan error, *x509.CertPool) {
	t.Helper()

	certificate, trust := selfSigned(t)

	proxy, err := proxyhttp.ListenTLS(t.Context(), "127.0.0.1:0", "logical.test", current, script, certificate)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- proxy.Serve(t.Context()) }()

	return proxy, done, trust
}

// selfSigned mints a certificate for serverName and the pool that trusts it.
func selfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: serverName},
		DNSNames:              []string{serverName},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	trust := x509.NewCertPool()
	trust.AddCert(leaf)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, trust
}
