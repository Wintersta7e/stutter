package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

// preRunOrder is the pre-run order: every refusal, then the engine and the ledger, the corpus and the
// model, the classification and the start fingerprint before the first mutation, the images, the
// networks and the classification containers, the frozen endpoint split, then the listeners, the
// verified address, the CA file, the relays and the bus.
var preRunOrder = []string{
	"preconditions", "static-self", "host-networking", "open", "corpus", "model", "classify", "fingerprint",
	"refusals", "images", "reclassify", "bus-urls", "relay-image", "networks", stepClassificationContainers,
	stepDependencies, stepLayout, "listeners", "verify", "ca-file", "relays", "bus",
}

// Step names several ordering rules name.
const (
	stepClassificationContainers = "classification-containers"
	stepDependencies             = "dependencies"
	stepLayout                   = "layout"
)

func stepNames(steps []step) []string {
	names := make([]string, 0, len(steps))
	for _, each := range steps {
		names = append(names, each.name)
	}

	return names
}

// TestThePreRunOrderIsTheSpecOrder: the steps run in the order the spec rules, and each ordering rule
// it states holds.
func TestThePreRunOrderIsTheSpecOrder(t *testing.T) {
	t.Parallel()

	names := stepNames(newComposeCheck(composeRun{}, nil).prerunSteps())

	if !slices.Equal(names, preRunOrder) {
		t.Errorf("pre-run steps =\n  %v\nwant\n  %v", names, preRunOrder)
	}

	at := func(name string) int {
		index := slices.Index(names, name)
		if index < 0 {
			t.Fatalf("no %q step", name)
		}

		return index
	}

	rules := []struct {
		why    string
		before []string
		after  []string
	}{
		{"every host refusal precedes the engine and its ledger", []string{
			"preconditions", "static-self", "host-networking",
		}, []string{"open"}},
		{"the start walk and the key refusals precede the first mutation", []string{
			"fingerprint", "refusals",
		}, []string{"images"}},
		{"a TLS bus URL is refused before anything starts", []string{"bus-urls"}, []string{"relay-image"}},
		{"classification runs between the networks and the listeners", []string{"networks"}, []string{
			stepClassificationContainers, stepDependencies, stepLayout,
		}},
		{"the endpoint split is frozen before any listener", []string{
			stepClassificationContainers, stepDependencies, stepLayout,
		}, []string{"listeners"}},
		{"the CA file follows the verified address", []string{"verify"}, []string{"ca-file"}},
		{"the bus opens after the relays", []string{"relays"}, []string{"bus"}},
	}

	for _, rule := range rules {
		for _, before := range rule.before {
			for _, after := range rule.after {
				if at(before) >= at(after) {
					t.Errorf("%s: %s runs at %d, %s at %d", rule.why, before, at(before), after, at(after))
				}
			}
		}

		t.Logf("checked: %s", rule.why)
	}
}

// TestTheUpstreamSourceServesEveryKey: every attach reads the latest restore and where the bus is now,
// never an address kept from before a restart.
func TestTheUpstreamSourceServesEveryKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	bus, err := corpus.Open(t.Context(), filepath.Join(dir, "store"), "ORDERS")
	if err != nil {
		t.Fatalf("open the bus: %v", err)
	}

	t.Cleanup(bus.Close)

	type call struct {
		restored     map[string]netip.AddrPort
		bus, monitor netip.AddrPort
	}

	var calls []call

	check := &composeCheck{bus: bus, upstreamMap: func(
		restored map[string]netip.AddrPort, busAddr, monitor netip.AddrPort,
	) (map[string]netip.AddrPort, error) {
		calls = append(calls, call{restored: restored, bus: busAddr, monitor: monitor})

		return restored, nil
	}}

	first := map[string]netip.AddrPort{"db:5432": netip.MustParseAddrPort("127.0.0.1:49153")}
	check.setRestored(first)

	if _, err = check.upstreams(t.Context()); err != nil {
		t.Fatalf("upstreams: %v", err)
	}

	checkpoint, err := bus.Checkpoint(t.Context(), filepath.Join(dir, "B0"))
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	if err = bus.Restore(t.Context(), checkpoint); err != nil {
		t.Fatalf("restore: %v", err)
	}

	second := map[string]netip.AddrPort{"db:5432": netip.MustParseAddrPort("127.0.0.1:49170")}
	check.setRestored(second)

	if _, err = check.upstreams(t.Context()); err != nil {
		t.Fatalf("upstreams after the restore: %v", err)
	}

	if len(calls) != 2 {
		t.Fatalf("the mapper was called %d times, want 2", len(calls))
	}

	for index, want := range []map[string]netip.AddrPort{first, second} {
		if calls[index].restored["db:5432"] != want["db:5432"] {
			t.Errorf("call %d got restore %v, want %v", index, calls[index].restored, want)
		}
	}

	nowBus, err := addrPortOf(bus.URL())
	if err != nil {
		t.Fatalf("the bus URL %q: %v", bus.URL(), err)
	}

	if calls[1].bus != nowBus || calls[1].monitor != bus.MonitorAddr() {
		t.Errorf("after the restore the mapper got bus %v monitor %v, want %v and %v",
			calls[1].bus, calls[1].monitor, nowBus, bus.MonitorAddr())
	}

	if calls[0].bus == calls[1].bus {
		t.Errorf("the bus kept port %v across a restart, so this proves nothing about re-reading it", calls[0].bus)
	}
}

// selfSigned is one PEM-encoded certificate.
func selfSigned(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "stutter test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// TestTheCAFileIsOnePemBlockReadOnly: the file every target is pointed at holds the check's CA and
// nothing else, and nothing in a container can change it.
func TestTheCAFileIsOnePemBlockReadOnly(t *testing.T) {
	t.Parallel()

	certificate := selfSigned(t)
	path := filepath.Join(t.TempDir(), "ca.pem")

	if err := writeCA(path, certificate); err != nil {
		t.Fatalf("writeCA: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	block, rest := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" || strings.TrimSpace(string(rest)) != "" {
		t.Errorf("the CA file is not exactly one CERTIFICATE block: %q", data)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if mode := info.Mode().Perm(); mode != 0o444 {
		t.Errorf("the CA file's mode = %o, want 444", mode)
	}

	twice := filepath.Join(t.TempDir(), "ca.pem")
	if err := writeCA(twice, append(slices.Clone(certificate), certificate...)); err == nil {
		t.Error("two certificates were written as the check's CA")
	}
}
