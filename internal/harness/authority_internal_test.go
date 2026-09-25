package harness

import (
	"bytes"
	"context"
	"net/netip"
	"testing"
)

// TestOneAuthorityServesEveryConsumerCheck gives a service one CA to trust for the whole check: the
// CA file is written once, and a consumer check whose stub presented a different CA would be a
// service that cannot reach the stub at all.
func TestOneAuthorityServesEveryConsumerCheck(t *testing.T) {
	t.Parallel()

	set := openTestSet(t, testListenerConfig(t))
	start := func(context.Context, Addresses) (Consumer, error) { return nil, nil } //nolint:nilnil // never called.

	first, err := New(Config{Start: start, Listeners: set})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	second, err := New(Config{Start: start, Listeners: set})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if first.certificates == nil || first.certificates != second.certificates {
		t.Fatal("consumer checks present different CAs")
	}

	if !bytes.Equal(set.CAPEM(), first.certificates.pem) {
		t.Error("CAPEM() is not the CA the consumer checks present")
	}

	set.SetAdvertise(netip.MustParseAddr("192.168.65.254"))

	if got := set.advertised(); got != netip.MustParseAddr("192.168.65.254") {
		t.Errorf("advertised host = %s, want 192.168.65.254", got)
	}

	own, err := New(Config{Start: start})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if own.certificates == nil || own.certificates == first.certificates {
		t.Error("a sandbox without the listener set did not mint its own CA")
	}
}

// advertised is the verified address containers dial the listeners at; zero before verification.
func (s *ListenerSet) advertised() netip.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.advertise
}
