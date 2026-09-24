package testgate

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
)

// Deadcode compares the functions deadcode reports unreachable from the CLI with the committed
// baseline, by name: <import path>.<Func>, or <import path>.<Recv>.<Method>, never by line.
type Deadcode struct {
	// Reported is every name deadcode reported.
	Reported []string
	// New is every reported name the baseline lacks: a function nothing calls yet.
	New []string
	// Gone is every baseline name no longer reported: wired or deleted, and the baseline not updated.
	Gone []string
}

// deadPackage is one package of deadcode -json's report.
type deadPackage struct {
	Path  string     `json:"Path"`  //nolint:tagliatelle // the tool's own field name
	Funcs []deadFunc `json:"Funcs"` //nolint:tagliatelle // the tool's own field name
}

// deadFunc is one unreachable function of deadcode -json's report.
type deadFunc struct {
	Name string `json:"Name"` //nolint:tagliatelle // the tool's own field name
}

// CompareDeadcode reads deadcode -json from report and compares it with baseline, one name per line;
// blank lines and everything after a # are ignored. It passes only when New and Gone are both empty.
func CompareDeadcode(report io.Reader, baseline []string) (Deadcode, error) {
	var packages []deadPackage
	if err := json.NewDecoder(report).Decode(&packages); err != nil {
		return Deadcode{}, fmt.Errorf("reading deadcode -json: %w", err)
	}

	var d Deadcode

	for _, p := range packages {
		for _, f := range p.Funcs {
			d.Reported = append(d.Reported, p.Path+"."+f.Name)
		}
	}

	var want []string

	for _, line := range baseline {
		name, _, _ := strings.Cut(line, "#")
		if name = strings.TrimSpace(name); name != "" {
			want = append(want, name)
		}
	}

	slices.Sort(d.Reported)

	for _, name := range d.Reported {
		if !slices.Contains(want, name) {
			d.New = append(d.New, name)
		}
	}

	for _, name := range want {
		if !slices.Contains(d.Reported, name) {
			d.Gone = append(d.Gone, name)
		}
	}

	return d, nil
}
