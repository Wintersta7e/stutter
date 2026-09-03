package gate_test

import (
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/gate"
)

func seq(canonical ...string) []effect.Effect {
	out := make([]effect.Effect, len(canonical))
	for index, text := range canonical {
		out[index] = effect.Effect{Seq: index, Kind: effect.KindPostgres, Canonical: text}
	}

	return out
}

func TestCompareClassifiesFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		wantClass gate.Class
		reference []effect.Effect
		compared  []effect.Effect
		wantIndex int
	}{
		{
			name:      "identical sequences hold the gate",
			reference: seq("SELECT qty", "UPDATE qty = 11"),
			compared:  seq("SELECT qty", "UPDATE qty = 11"),
			wantClass: gate.ClassMatch,
			wantIndex: -1,
		},
		{
			name:      "same effects reordered",
			reference: seq("INSERT audit", "UPDATE stock"),
			compared:  seq("UPDATE stock", "INSERT audit"),
			wantClass: gate.ClassReordered,
			wantIndex: 0,
		},
		{
			name:      "identical structure differing values is the normaliser's fault",
			reference: seq("SELECT qty", "UPDATE stock SET qty = 11"),
			compared:  seq("SELECT qty", "UPDATE stock SET qty = 82"),
			wantClass: gate.ClassFieldDrift,
			wantIndex: 1,
		},
		{
			name:      "different statements are a divergent set",
			reference: seq("SELECT qty", "UPDATE stock SET qty = 11"),
			compared:  seq("SELECT qty", "DELETE FROM stock"),
			wantClass: gate.ClassDivergentSet,
			wantIndex: 1,
		},
		{
			name:      "compared run stopped early",
			reference: seq("SELECT qty", "UPDATE stock"),
			compared:  seq("SELECT qty"),
			wantClass: gate.ClassDivergentSet,
			wantIndex: 1,
		},
		{
			name:      "compared run did extra work",
			reference: seq("SELECT qty"),
			compared:  seq("SELECT qty", "UPDATE stock"),
			wantClass: gate.ClassDivergentSet,
			wantIndex: 1,
		},
		{
			name:      "both empty",
			reference: nil,
			compared:  nil,
			wantClass: gate.ClassMatch,
			wantIndex: -1,
		},
	}

	comparer := gate.NewComparer()

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := comparer.Compare(testCase.reference, testCase.compared)
			if got.Class != testCase.wantClass {
				t.Errorf("Class = %q, want %q (%s)", got.Class, testCase.wantClass, got.Describe())
			}

			if got.Index != testCase.wantIndex {
				t.Errorf("Index = %d, want %d", got.Index, testCase.wantIndex)
			}

			if got.OK() != (testCase.wantClass == gate.ClassMatch) {
				t.Errorf("OK() = %v, inconsistent with class %q", got.OK(), got.Class)
			}
		})
	}
}

// TestCompareDistinguishesProtocols guards against two effects that happen to carry the same text
// over different protocols being treated as a match.
func TestCompareDistinguishesProtocols(t *testing.T) {
	t.Parallel()

	reference := []effect.Effect{{Kind: effect.KindPostgres, Canonical: "charge"}}
	compared := []effect.Effect{{Kind: effect.KindHTTP, Canonical: "charge"}}

	if got := gate.NewComparer().Compare(reference, compared); got.OK() {
		t.Error("Compare() held the gate across different protocols carrying identical text")
	}
}

func TestDescribeNamesThePosition(t *testing.T) {
	t.Parallel()

	const diverged = 12

	reference := make([]effect.Effect, diverged+1)
	compared := make([]effect.Effect, diverged+1)

	for index := range reference {
		reference[index] = effect.Effect{Kind: effect.KindPostgres, Canonical: "SELECT 1"}
		compared[index] = effect.Effect{Kind: effect.KindPostgres, Canonical: "SELECT 1"}
	}

	compared[diverged].Canonical = "DELETE FROM everything"

	got := gate.NewComparer().Compare(reference, compared)
	if got.Index != diverged {
		t.Fatalf("Index = %d, want %d", got.Index, diverged)
	}

	// A two-digit position: an earlier draft rendered only the last digit of the index.
	if want := "position 12"; !strings.Contains(got.Describe(), want) {
		t.Errorf("Describe() = %q, want it to contain %q", got.Describe(), want)
	}
}
