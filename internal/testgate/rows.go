package testgate

import (
	"bytes"
	_ "embed" // rows.json is compiled in: CI reads the manifest without the specification beside it.
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// rowsJSON is the committed manifest of exit rows and refused-key classes that the variant suite
// must cover. A change that adds a row adds it here in the same commit; a local scanner keeps the
// manifest in step with the specification.
//
//go:embed rows.json
var rowsJSON []byte

// errRows means the row manifest is malformed.
var errRows = errors.New("row manifest")

// Row is one exit row or refused-key class. An unreachable row names why no fixture reaches it.
type Row struct {
	ID        string `json:"id"`
	Reason    string `json:"reason,omitempty"`
	Reachable bool   `json:"reachable"`
}

// Rows is the manifest: the exit rows, in the exit table's order, and the refused-key classes.
type Rows struct {
	Exits       []Row `json:"exits"`
	RefusedKeys []Row `json:"refused_keys"`
}

// LoadRows returns the committed manifest.
func LoadRows() (Rows, error) {
	var rows Rows

	decoder := json.NewDecoder(bytes.NewReader(rowsJSON))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&rows); err != nil {
		return Rows{}, fmt.Errorf("%w: %w", errRows, err)
	}

	return rows, nil
}

// Uncovered returns every reachable row, exit rows first, that no case in cases names. An
// unreachable row never needs a case.
func Uncovered(rows Rows, cases []string) []string {
	var uncovered []string

	for _, row := range slices.Concat(rows.Exits, rows.RefusedKeys) {
		if row.Reachable && !slices.Contains(cases, row.ID) {
			uncovered = append(uncovered, row.ID)
		}
	}

	return uncovered
}
