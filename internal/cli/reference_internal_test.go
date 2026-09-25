package cli

import (
	"strings"
	"testing"
)

// TestEachReferenceInvocationHasItsOwnFixture: two reference checks against one database never reset
// each other's stock row.
func TestEachReferenceInvocationHasItsOwnFixture(t *testing.T) {
	t.Parallel()

	first, err := newReferenceSKU()
	if err != nil {
		t.Fatal(err)
	}

	second, err := newReferenceSKU()
	if err != nil {
		t.Fatal(err)
	}

	if first == second {
		t.Errorf("two invocations share the fixture row %q", first)
	}

	for _, sku := range []string{first, second} {
		if !strings.HasPrefix(sku, referenceSKU) {
			t.Errorf("fixture row %q is not recognisably Stutter's", sku)
		}
	}
}
