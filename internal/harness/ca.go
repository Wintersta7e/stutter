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
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	// validity is how long the CA and every leaf stay valid from the moment the CA is minted: longer than
	// any check, so no run ever meets an expired certificate.
	validity = 30 * 24 * time.Hour
	// backdate starts every certificate's validity before the CA was minted, so a client whose clock runs
	// behind the host's still accepts it.
	backdate = time.Hour
	// serialBits sizes the random certificate serial. x509 wants a positive integer that will not
	// collide; 128 bits is what public CAs use for the same reason.
	serialBits = 128
	// noServerName keys the leaf for a client that sent no server name, per local address.
	noServerName = "ip:"
)

// authority is the certificate authority the HTTP stub's TLS entry presents: one per check on the
// listener set, one per Sandbox otherwise.
//
// The stub has to be reachable over TLS because most real dependencies are, and a service that cannot
// reach its dependency produces no effects at all — which reads as a handler that did nothing, the most
// dangerous wrong answer this tool can give. The CA lives and dies with the check (listener set) or the
// Sandbox (Go API); its key is held in memory only, never written anywhere a real trust store could
// pick it up.
//
// Every server name a client asks for gets a leaf of its own, minted on first use and kept for the
// CA's life, so every run presents the same leaf for a name.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type authority struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	// leaves are the leaves minted so far, by lower-cased server name, or by local address for a client
	// that sent none.
	leaves map[string]*tls.Certificate
	// minted is when the CA was made; every certificate's window is measured from it.
	minted time.Time
	// logical is the stub's logical host; advertise is the host the service is told to dial.
	logical   string
	advertise string
	// pem is the CA certificate, which the service under test must trust to reach the stub.
	pem []byte
	mu  sync.Mutex
}

// newAuthority mints a CA. Leaves are minted as clients ask for them.
func newAuthority(logicalHost, advertiseHost string) (*authority, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate the stub CA key: %w", err)
	}

	minted := time.Now()

	caTemplate, err := certificateTemplate("Stutter stub CA", minted)
	if err != nil {
		return nil, err
	}

	caTemplate.IsCA = true
	caTemplate.BasicConstraintsValid = true
	caTemplate.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create the stub CA certificate: %w", err)
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, fmt.Errorf("parse the stub CA certificate: %w", err)
	}

	return &authority{
		caCert:    caCert,
		caKey:     caKey,
		leaves:    make(map[string]*tls.Certificate),
		minted:    minted,
		logical:   logicalHost,
		advertise: advertiseHost,
		pem:       pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
	}, nil
}

// certificate is the leaf for a client's hello: its server name's, or, for a client that sent none,
// one covering every address the stub is reached at on the connection's local address.
func (a *authority) certificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	name := strings.ToLower(hello.ServerName)

	var local netip.Addr

	key := name
	if name == "" {
		local = localAddr(hello.Conn)
		key = noServerName + local.String()
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if leaf, minted := a.leaves[key]; minted {
		return leaf, nil
	}

	template, err := certificateTemplate(name, a.minted)
	if err != nil {
		return nil, err
	}

	if name == "" {
		a.coverUnnamed(template, local)
	} else {
		cover(template, name)
	}

	leaf, err := a.issue(template)
	if err != nil {
		return nil, err
	}

	a.leaves[key] = leaf

	return leaf, nil
}

// coverUnnamed makes a leaf for a client that sent no server name: it verifies one of the addresses it
// was told to dial, or the logical host, so every such address is on it.
func (a *authority) coverUnnamed(template *x509.Certificate, local netip.Addr) {
	template.Subject.CommonName = a.logical
	template.DNSNames = []string{a.logical, "localhost"}
	template.IPAddresses = []net.IP{net.ParseIP(loopback), net.IPv6loopback}

	if local.IsValid() {
		cover(template, local.String())
	}

	if a.advertise != "" {
		cover(template, a.advertise)
	}
}

// setAdvertise records the host the service is told to dial, once it is known — after the authority
// was minted, on the listener set. A leaf already minted for a client without a server name did not
// cover it, so those are minted again.
func (a *authority) setAdvertise(host string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.advertise = host

	for key := range a.leaves {
		if strings.HasPrefix(key, noServerName) {
			delete(a.leaves, key)
		}
	}
}

// cover adds a name to a leaf, once: as an IP address when it is one, else as a DNS name.
func cover(template *x509.Certificate, name string) {
	address, err := netip.ParseAddr(name)
	if err != nil {
		if !slices.Contains(template.DNSNames, name) {
			template.DNSNames = append(template.DNSNames, name)
		}

		return
	}

	if !slices.ContainsFunc(template.IPAddresses, func(known net.IP) bool { return known.Equal(address.AsSlice()) }) {
		template.IPAddresses = append(template.IPAddresses, address.AsSlice())
	}
}

// issue signs a leaf with the CA.
func (a *authority) issue(template *x509.Certificate) (*tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate a stub leaf key: %w", err)
	}

	template.KeyUsage = x509.KeyUsageDigitalSignature
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}

	der, err := x509.CreateCertificate(rand.Reader, template, a.caCert, &key.PublicKey, a.caKey)
	if err != nil {
		return nil, fmt.Errorf("create a stub leaf: %w", err)
	}

	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// localAddr is the address a connection arrived on, the zero address when it has none.
func localAddr(conn net.Conn) netip.Addr {
	if conn == nil {
		return netip.Addr{}
	}

	address, err := netip.ParseAddrPort(conn.LocalAddr().String())
	if err != nil {
		return netip.Addr{}
	}

	return address.Addr().Unmap()
}

func certificateTemplate(name string, minted time.Time) (*x509.Certificate, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), serialBits)

	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate a certificate serial: %w", err)
	}

	return &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    minted.Add(-backdate),
		NotAfter:     minted.Add(validity),
	}, nil
}
