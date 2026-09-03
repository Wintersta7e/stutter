package effect_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/effect"
)

func TestNewProvenanceReportsExtractionMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		want    effect.Mode
		payload []byte
	}{
		{name: "json object", payload: []byte(`{"order_id":"ORD-99001"}`), want: effect.ModeJSON},
		{name: "json scalar", payload: []byte(`"bare string"`), want: effect.ModeJSON},
		{name: "not json", payload: []byte{0x08, 0x96, 0x01, 0x12, 0x04}, want: effect.ModeOpaque},
		{name: "empty", payload: nil, want: effect.ModeNone},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := effect.NewProvenance(testCase.payload).Mode(); got != testCase.want {
				t.Errorf("Mode() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestSubstituteReplacesValuesByPath(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"order":{"id":"ORD-99001","lines":[{"sku":"WIDGET-7"}]}}`)
	prov := effect.NewProvenance(payload)

	got := prov.Substitute(`UPDATE stock SET qty = qty - 3 WHERE sku = 'WIDGET-7' AND ref = 'ORD-99001'`)
	want := `UPDATE stock SET qty = qty - 3 WHERE sku = '<msg:order.lines.0.sku>' AND ref = '<msg:order.id>'`

	if got != want {
		t.Errorf("Substitute()\n got: %s\nwant: %s", got, want)
	}
}

func TestSubstituteSkipsValuesTooShortToMatchSafely(t *testing.T) {
	t.Parallel()

	// "12" would rewrite the "12" inside "1234", so short values are deliberately not substituted.
	prov := effect.NewProvenance([]byte(`{"qty":12,"total":1234}`))

	got := prov.Substitute("qty=12 total=1234")
	want := "qty=12 total=<msg:total>"

	if got != want {
		t.Errorf("Substitute() = %q, want %q", got, want)
	}
}

func TestSubstitutePrefersLongerValues(t *testing.T) {
	t.Parallel()

	// "ORDER-1234" is a prefix of "ORDER-1234-EXT". Replacing the short one first would corrupt the
	// long one into "<msg:short>-EXT".
	prov := effect.NewProvenance([]byte(`{"short":"ORDER-1234","long":"ORDER-1234-EXT"}`))

	got := prov.Substitute("ref=ORDER-1234-EXT parent=ORDER-1234")
	want := "ref=<msg:long> parent=<msg:short>"

	if got != want {
		t.Errorf("Substitute() = %q, want %q", got, want)
	}
}

// TestSubstituteIsDeterministic guards the gate itself. JSON objects decode into a Go map, whose
// iteration order is randomised per run, so replacements arrive in a random order. Without a total
// ordering, two clean runs over an identical payload can canonicalise differently and the
// determinism gate flaps for a reason that has nothing to do with the service under test.
//
// The values are chosen so that order genuinely decides the result: "ABAB" and "BABA" are the same
// length, so the length sort cannot separate them, and they overlap inside "ABABABA", so whichever
// is substituted first consumes the other's match. Equal-length values that do not overlap would
// make this test pass no matter how the replacements were ordered.
func TestSubstituteIsDeterministic(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"first":"ABAB","second":"BABA"}`)

	const input = "ABABABA"

	want := effect.NewProvenance(payload).Substitute(input)

	for attempt := range 500 {
		if got := effect.NewProvenance(payload).Substitute(input); got != want {
			t.Fatalf("attempt %d produced a different canonical form:\n got: %s\nwant: %s", attempt, got, want)
		}
	}
}

func TestSubstituteOnUnparseablePayloadMatchesWhole(t *testing.T) {
	t.Parallel()

	prov := effect.NewProvenance([]byte("raw-binary-blob"))

	if got := prov.Substitute("body=raw-binary-blob"); got != "body=<msg:payload>" {
		t.Errorf("Substitute() = %q, want %q", got, "body=<msg:payload>")
	}
}

func TestSubstituteOnEmptyPayloadChangesNothing(t *testing.T) {
	t.Parallel()

	const input = "INSERT INTO ledger (amount) VALUES (4200)"

	if got := effect.NewProvenance(nil).Substitute(input); got != input {
		t.Errorf("Substitute() = %q, want it unchanged", got)
	}
}
