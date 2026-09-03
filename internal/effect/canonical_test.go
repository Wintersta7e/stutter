package effect_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/effect"
)

// TestCanonicalisePreservesMessageDerivedIdentifiers is the central claim of the normaliser. An
// identifier that arrived in the message is signal and must survive as itself; one the handler
// invented is noise and must be flattened. Getting this backwards either makes the determinism gate
// unpassable or hides a real divergence.
func TestCanonicalisePreservesMessageDerivedIdentifiers(t *testing.T) {
	t.Parallel()

	const (
		fromMessage = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
		invented    = "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
	)

	prov := effect.NewProvenance([]byte(`{"order_id":"` + fromMessage + `"}`))
	got := effect.NewCanonicaliser().Canonicalise(
		"INSERT INTO events (order_id, trace_id) VALUES ('"+fromMessage+"', '"+invented+"')",
		prov,
	)
	want := "INSERT INTO events (order_id, trace_id) VALUES ('<msg:order_id>', '<uuid>')"

	if got != want {
		t.Errorf("Canonicalise()\n got: %s\nwant: %s", got, want)
	}
}

func TestCanonicaliseFlattensNondeterministicTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "uuid",
			input: "trace=9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
			want:  "trace=<uuid>",
		},
		{
			name:  "rfc3339 timestamp",
			input: "created_at='2026-09-03T15:22:07Z'",
			want:  "created_at='<ts>'",
		},
		{
			name:  "timestamp with offset and fraction",
			input: "created_at='2026-09-03 15:22:07.481+01:00'",
			want:  "created_at='<ts>'",
		},
		{
			name:  "no nondeterminism",
			input: "UPDATE stock SET qty = qty - 3",
			want:  "UPDATE stock SET qty = qty - 3",
		},
	}

	canon := effect.NewCanonicaliser()

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := canon.Canonicalise(testCase.input, nil); got != testCase.want {
				t.Errorf("Canonicalise(%q) = %q, want %q", testCase.input, got, testCase.want)
			}
		})
	}
}

// TestCanonicaliseAgreesAcrossRuns is the determinism gate reduced to a unit test: the same handler
// behaviour, expressed with different invented identifiers and wall-clock stamps on each run, must
// canonicalise to one form.
func TestCanonicaliseAgreesAcrossRuns(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"order_id":"ORD-99001","qty":3}`)
	canon := effect.NewCanonicaliser()

	runA := canon.Canonicalise(
		"INSERT INTO audit (id, ref, at) VALUES ('9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d', 'ORD-99001', "+
			"'2026-09-03T15:22:07Z')",
		effect.NewProvenance(payload),
	)
	runB := canon.Canonicalise(
		"INSERT INTO audit (id, ref, at) VALUES ('c9a0f1e2-77bb-4a10-8f3e-1d2c4b5a6e70', 'ORD-99001', "+
			"'2026-09-03T15:31:44Z')",
		effect.NewProvenance(payload),
	)

	if runA != runB {
		t.Errorf("two clean runs disagree:\n A: %s\n B: %s", runA, runB)
	}
}

// TestCanonicaliseDetectsDoubleApplication is the counterpart: normalisation must not be so
// aggressive that it erases the bug the tool exists to find.
func TestCanonicaliseDetectsDoubleApplication(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"order_id":"ORD-99001","qty":3}`)
	canon := effect.NewCanonicaliser()

	clean := canon.Canonicalise("UPDATE stock SET qty = 11 WHERE ref = 'ORD-99001'", effect.NewProvenance(payload))
	mutated := canon.Canonicalise("UPDATE stock SET qty = 8 WHERE ref = 'ORD-99001'", effect.NewProvenance(payload))

	if clean == mutated {
		t.Errorf("normalisation erased a real divergence; both runs canonicalised to %s", clean)
	}
}
