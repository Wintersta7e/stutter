package http_test

import (
	"crypto/x509"
	"errors"
	"net"
	"testing"
	"time"

	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

// stopRow is one golden rendering of an egress stop. drive, when set, provokes the stop on a fresh
// stub and returns what its Serve returned; a row without one is checked as constructed, until the
// stub has a way to raise it.
type stopRow struct {
	drive     func(t *testing.T) error
	construct *proxyhttp.EgressStop
	name      string
	want      string
	class     proxyhttp.StopClass
}

// TestEgressStopRendersNoAddress: a stop's text is compared between runs and shown to the user, so it
// is the same whichever connection raised it — no peer address, port, time or payload byte — and says
// what the client did.
func TestEgressStopRendersNoAddress(t *testing.T) {
	t.Parallel()

	rows := stopRows()

	covered := make(map[proxyhttp.StopClass]bool, len(rows))
	for _, row := range rows {
		covered[row.class] = true
	}

	classes := 0
	for class := proxyhttp.StopClass(1); class.String() != "unknown"; class++ {
		classes++

		if !covered[class] {
			t.Errorf("class %s has no golden rendering", class)
		}
	}

	t.Logf("classes checked: %d", classes)

	if classes == 0 || classes != len(covered) {
		t.Fatalf("classes checked: %d, golden classes: %d", classes, len(covered))
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			if row.drive == nil {
				if got := row.construct.Error(); got != row.want {
					t.Errorf("Error()\n got: %s\nwant: %s", got, row.want)
				}

				return
			}

			// Two fresh connections, so two peer ports: the text must not tell them apart.
			for attempt := range 2 {
				stop := asStop(t, row.drive(t))

				if stop.Class != row.class {
					t.Errorf("attempt %d: class = %s, want %s", attempt, stop.Class, row.class)
				}

				if got := stop.Error(); got != row.want {
					t.Errorf("attempt %d: Error()\n got: %s\nwant: %s", attempt, got, row.want)
				}
			}
		})
	}
}

func stopRows() []stopRow {
	return []stopRow{
		{
			name:  "tls-on-cleartext",
			class: proxyhttp.StopTLSOnCleartext,
			want: "egress stop tls-on-cleartext on port 80: the client used TLS where only cleartext " +
				"HTTP/1.1 is served",
			drive: cleartextStop([]byte{0x16, 0x03, 0x01, 0x00, 0x00}),
		},
		{
			name:  "not-http malformed",
			class: proxyhttp.StopNotHTTP,
			want:  "egress stop not-http on port 80: not parseable HTTP/1.1",
			drive: cleartextStop([]byte("NOT HTTP\r\n\r\n")),
		},
		{
			name:  "not-http preface",
			class: proxyhttp.StopNotHTTP,
			want:  "egress stop not-http on port 80: the HTTP/2 connection preface, not parseable HTTP/1.1",
			drive: cleartextStop([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")),
		},
		{
			name:  "handshake",
			class: proxyhttp.StopHandshake,
			want: `egress stop handshake on port 443 for "api.example.test": the TLS handshake failed: ` +
				`the client rejected the certificate (tls: bad certificate)`,
			drive: tlsStop(func(t *testing.T, address string, _ *x509.CertPool) {
				t.Helper()

				// A client that trusts nothing rejects the stub's certificate.
				if connection, err := dialTLS(t, address, x509.NewCertPool()); err == nil {
					_ = connection.Close()
				}
			}),
		},
		{
			name:  "alpn",
			class: proxyhttp.StopALPN,
			want: `egress stop alpn on port 443 for "api.example.test": the client offered [h2]; ` +
				`only http/1.1 is served`,
			drive: tlsStop(func(t *testing.T, address string, trust *x509.CertPool) {
				t.Helper()

				if connection, err := dialTLS(t, address, trust, "h2"); err == nil {
					_ = connection.Close()
				}
			}),
		},
		{
			name:  "no-request",
			class: proxyhttp.StopNoRequest,
			want: `egress stop no-request on port 443 for "api.example.test": the client completed a TLS ` +
				`handshake and sent no request, and no earlier connection to this host did either: it likely ` +
				`pins certificates or speaks a protocol the stub does not serve`,
			drive: tlsStop(func(t *testing.T, address string, trust *x509.CertPool) {
				t.Helper()

				_ = mustDialTLS(t, address, trust).Close()
			}),
		},
		{
			name:  "connect",
			class: proxyhttp.StopConnect,
			want: `egress stop connect on port 80 for "api.example.test:443": a CONNECT tunnel hides the ` +
				`request inside it, which cannot be observed`,
			drive: cleartextStop([]byte(connectRequest)),
		},
		{
			name:      "cleartext-on-tls",
			class:     proxyhttp.StopCleartextOnTLS,
			want:      "egress stop cleartext-on-tls on port 443: the client sent cleartext where TLS is served",
			construct: &proxyhttp.EgressStop{Class: proxyhttp.StopCleartextOnTLS, Port: 443},
		},
		{
			name:  "silent",
			class: proxyhttp.StopSilent,
			want: "egress stop silent on port 587: closed before sending a byte; SMTP is server-first and " +
				"Stutter has no SMTP stub",
			construct: &proxyhttp.EgressStop{
				Class:  proxyhttp.StopSilent,
				Port:   587,
				Detail: "closed before sending a byte",
			},
		},
		{
			name:  "srv",
			class: proxyhttp.StopSRV,
			want: `egress stop srv for "_imap._tcp.example.test": an SRV lookup precedes a connection Stutter ` +
				`cannot observe`,
			construct: &proxyhttp.EgressStop{Class: proxyhttp.StopSRV, Name: "_imap._tcp.example.test"},
		},
	}
}

// cleartextStop sends payload on a fresh connection to a fresh cleartext stub and returns what Serve
// returned.
func cleartextStop(payload []byte) func(t *testing.T) error {
	return func(t *testing.T) error {
		t.Helper()

		script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
		proxy, done := startProxy(t, &sink{}, script)

		connection, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", proxy.Addr())
		if err != nil {
			t.Fatal(err)
		}

		if _, err := connection.Write(payload); err != nil {
			t.Fatal(err)
		}

		_ = connection.Close()

		return awaitServe(t, done)
	}
}

// tlsStop runs client against a fresh TLS stub and returns what Serve returned.
func tlsStop(client func(t *testing.T, address string, trust *x509.CertPool)) func(t *testing.T) error {
	return func(t *testing.T) error {
		t.Helper()

		script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
		proxy, done, trust := startTLSProxy(t, &sink{}, script)

		client(t, proxy.Addr(), trust)

		return awaitServe(t, done)
	}
}

// awaitServe waits for Serve to return.
func awaitServe(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(stopWait):
		t.Fatalf("Serve did not stop within %v", stopWait)

		return nil
	}
}

// asStop requires err to carry an egress stop and returns it.
func asStop(t *testing.T, err error) *proxyhttp.EgressStop {
	t.Helper()

	stop, isStop := errors.AsType[*proxyhttp.EgressStop](err)
	if !isStop {
		t.Fatalf("Serve() error = %v, want an egress stop", err)
	}

	return stop
}
