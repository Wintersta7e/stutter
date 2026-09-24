package testgate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

func TestTheRowManifestLoads(t *testing.T) {
	t.Parallel()

	rows, err := testgate.LoadRows()
	if err != nil {
		t.Fatal(err)
	}

	if len(rows.Exits) == 0 || len(rows.RefusedKeys) == 0 {
		t.Fatalf("exits=%d refused keys=%d; want both > 0", len(rows.Exits), len(rows.RefusedKeys))
	}

	seen := map[string]bool{}

	for _, row := range slices.Concat(rows.Exits, rows.RefusedKeys) {
		if seen[row.ID] {
			t.Errorf("row %s appears twice", row.ID)
		}

		seen[row.ID] = true

		if !row.Reachable && strings.TrimSpace(row.Reason) == "" {
			t.Errorf("row %s is unreachable from a fixture and gives no reason", row.ID)
		}
	}

	for _, row := range rows.Exits {
		if !strings.HasPrefix(row.ID, "E") {
			t.Errorf("exit row %q is not an E-row", row.ID)
		}
	}

	for _, row := range rows.RefusedKeys {
		if !strings.HasPrefix(row.ID, "K") {
			t.Errorf("refused-key row %q is not a K-class", row.ID)
		}
	}
}

func TestAnUncoveredReachableRowFails(t *testing.T) {
	t.Parallel()

	rows := testgate.Rows{
		Exits: []testgate.Row{
			{ID: "E1", Reachable: true}, {ID: "E2", Reachable: true}, {ID: "E38", Reason: "a signal"},
		},
		RefusedKeys: []testgate.Row{{ID: "K1", Reachable: true}},
	}

	if got := testgate.Uncovered(rows, []string{"E1", "K1"}); !slices.Equal(got, []string{"E2"}) {
		t.Fatalf("uncovered = %q; want the reachable row without a case, E2", got)
	}

	if got := testgate.Uncovered(rows, []string{"E1", "E2", "K1"}); len(got) != 0 {
		t.Fatalf("uncovered = %q; an unreachable row never needs a case", got)
	}
}
