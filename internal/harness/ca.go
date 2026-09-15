package harness

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

// certificateLifetime outlives any plausible run without being long enough to be worth reusing.
const certificateLifetime = 24 * time.Hour

// serialBits sizes the random certificate serial. x509 wants a positive integer that will not
// collide; 128 bits is what public CAs use for the same reason.
const serialBits = 128

// authority is a throwaway certificate authority for one sandbox.
//
// The HTTP stub has to be reachable over TLS because most real dependencies are, and a service that
// cannot reach its dependency produces no effects at all — which reads as a handler that did
// nothing, the most dangerous wrong answer this tool can give. The CA lives and dies with the run
// and is handed to the service under test to trust; it is never written anywhere a real trust store
// could pick it up.
type authority struct {
	// leaf is what the stub serves.
	leaf tls.Certificate
	// pem is the CA certificate, which the service under test must trust to reach the stub.
	pem []byte
}

// newAuthority mints a CA and one leaf certificate valid for host.
//
// The leaf also covers 127.0.0.1, because the stub is reached at a loopback address while it
// answers under a stable logical name — without the address in the SAN list every connection fails
// verification and no effect is ever observed.
func newAuthority(host string) (*authority, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate the sandbox CA key: %w", err)
	}

	caTemplate, err := certificateTemplate("Stutter sandbox CA")
	if err != nil {
		return nil, err
	}

	caTemplate.IsCA = true
	caTemplate.BasicConstraintsValid = true
	caTemplate.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create the sandbox CA certificate: %w", err)
	}

	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse the sandbox CA certificate: %w", err)
	}

	leaf, err := issueLeaf(host, caCertificate, caKey)
	if err != nil {
		return nil, err
	}

	return &authority{
		leaf: leaf,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
	}, nil
}

func issueLeaf(host string, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate the stub key: %w", err)
	}

	template, err := certificateTemplate(host)
	if err != nil {
		return tls.Certificate{}, err
	}

	template.DNSNames = []string{host, "localhost"}
	template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback}
	template.KeyUsage = x509.KeyUsageDigitalSignature
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}

	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create the stub certificate: %w", err)
	}

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

func certificateTemplate(name string) (*x509.Certificate, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), serialBits)

	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate a certificate serial: %w", err)
	}

	now := time.Now()

	return &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(certificateLifetime),
	}, nil
}
