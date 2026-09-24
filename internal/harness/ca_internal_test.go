package harness

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/netip"
	"testing"
	"time"
)

// exampleName is a server name a service dials.
const exampleName = "api.example.test"

// localConn is a connection that arrived on 127.0.0.1, which is all a hello's Conn is read for.
type localConn struct{ net.Conn }

func (localConn) LocalAddr() net.Addr { return &net.TCPAddr{IP: net.ParseIP(loopback), Port: 443} }

// mintedFor is the leaf authority presents for hello, parsed.
func mintedFor(t *testing.T, minted *authority, hello *tls.ClientHelloInfo) *x509.Certificate {
	t.Helper()

	leaf, err := minted.certificate(hello)
	if err != nil {
		t.Fatalf("certificate(%q) error = %v", hello.ServerName, err)
	}

	parsed, err := x509.ParseCertificate(leaf.Certificate[0])
	if err != nil {
		t.Fatalf("parse the leaf: %v", err)
	}

	return parsed
}

// verify checks leaf against the authority's CA alone, for name, at when.
func verify(minted *authority, leaf *x509.Certificate, name string, when time.Time) error {
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(minted.pem)

	_, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: name, CurrentTime: when})

	return err
}

func testAuthority(t *testing.T, advertise string) *authority {
	t.Helper()

	minted, err := newAuthority("logical.invalid", advertise)
	if err != nil {
		t.Fatalf("newAuthority() error = %v", err)
	}

	return minted
}

// TestALeafIsMintedOncePerServerName: every name the service dials verifies against the one CA it
// trusts, and every run presents the same leaf for a name.
func TestALeafIsMintedOncePerServerName(t *testing.T) {
	t.Parallel()

	minted := testAuthority(t, "")

	first := mintedFor(t, minted, &tls.ClientHelloInfo{ServerName: exampleName})
	again := mintedFor(t, minted, &tls.ClientHelloInfo{ServerName: "API.Example.Test"})
	other := mintedFor(t, minted, &tls.ClientHelloInfo{ServerName: "other.example.test"})

	if !bytes.Equal(first.Raw, again.Raw) {
		t.Error("the same server name, in another case, got a different leaf")
	}

	now := time.Now()

	for _, check := range []struct {
		leaf *x509.Certificate
		name string
	}{{first, exampleName}, {other, "other.example.test"}} {
		if err := verify(minted, check.leaf, check.name, now); err != nil {
			t.Errorf("the leaf for %s does not verify: %v", check.name, err)
		}
	}

	if err := verify(minted, first, "other.example.test", now); err == nil {
		t.Error("the leaf for api.example.test verifies for other.example.test")
	}

	literal := mintedFor(t, minted, &tls.ClientHelloInfo{ServerName: "192.0.2.7"})
	if err := verify(minted, literal, "192.0.2.7", now); err != nil {
		t.Errorf("an IP-literal server name does not verify: %v", err)
	}
}

// TestTheNoSNILeafCoversTheAdvertisedAddress: a client dialling an address sends no server name, and
// verifies the address it was told to dial, not the one the connection arrived on.
func TestTheNoSNILeafCoversTheAdvertisedAddress(t *testing.T) {
	t.Parallel()

	minted := testAuthority(t, "192.0.2.10")
	leaf := mintedFor(t, minted, &tls.ClientHelloInfo{Conn: localConn{}})

	for _, name := range []string{"192.0.2.10", loopback, "logical.invalid"} {
		if err := verify(minted, leaf, name, time.Now()); err != nil {
			t.Errorf("the no-SNI leaf does not verify for %s: %v", name, err)
		}
	}
}

// TestTheCAWindowCoversAThirtyMinuteSkew: a container's clock behind the host's still accepts the
// leaf, and a check lasting weeks never outlives it.
func TestTheCAWindowCoversAThirtyMinuteSkew(t *testing.T) {
	t.Parallel()

	minted := testAuthority(t, "")
	leaf := mintedFor(t, minted, &tls.ClientHelloInfo{ServerName: exampleName})

	for _, when := range []time.Time{time.Now().Add(-30 * time.Minute), time.Now().Add(29 * 24 * time.Hour)} {
		if err := verify(minted, leaf, exampleName, when); err != nil {
			t.Errorf("the leaf does not verify at %s: %v", when, err)
		}
	}
}

// TestTheCAPEMIsOneCertificateBlock: the CA file holds exactly the certificate, and never the key.
func TestTheCAPEMIsOneCertificateBlock(t *testing.T) {
	t.Parallel()

	block, rest := pem.Decode(testAuthority(t, "").pem)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		t.Errorf("pem = one %v block and %d bytes more, want one CERTIFICATE block and nothing else",
			block != nil && block.Type == "CERTIFICATE", len(rest))
	}
}

// TestTheSetsAdvertisedAddressReachesTheNoSNILeaf: the check's CA is minted before the address
// containers dial is known, and a client without a server name verifies that address, so the leaf for
// such a client covers it once the set learns it.
func TestTheSetsAdvertisedAddressReachesTheNoSNILeaf(t *testing.T) {
	t.Parallel()

	set := openTestSet(t, testListenerConfig(t))
	hello := &tls.ClientHelloInfo{Conn: localConn{}}

	if err := verify(set.authority, mintedFor(t, set.authority, hello), "192.0.2.20", time.Now()); err == nil {
		t.Fatal("the no-SNI leaf covers an address the set was never told")
	}

	set.SetAdvertise(netip.MustParseAddr("192.0.2.20"))

	if err := verify(set.authority, mintedFor(t, set.authority, hello), "192.0.2.20", time.Now()); err != nil {
		t.Errorf("after SetAdvertise the no-SNI leaf does not verify for the advertised address: %v", err)
	}
}
