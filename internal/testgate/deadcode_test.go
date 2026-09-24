package testgate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// deadcodeReport is deadcode -json's shape: packages, each with its unreachable functions.
const deadcodeReport = `[
	{"Name": "nats", "Path": "example.test/proxy/nats", "Funcs": [
		{"Name": "Listen", "Position": {"File": "/src/nats/proxy.go", "Line": 40, "Col": 6}, "Generated": false}
	]},
	{"Name": "report", "Path": "example.test/report", "Funcs": [
		{"Name": "Report.String", "Position": {"File": "/src/report/render.go", "Line": 9, "Col": 17},
		 "Generated": false}
	]}
]`

func TestDeadcodeMustEqualTheBaseline(t *testing.T) {
	t.Parallel()

	baseline := []string{
		"# the functions unreachable from the CLI",
		"",
		"example.test/proxy/nats.Listen # the CLI's compose branch calls it",
		"  example.test/report.Report.String",
	}

	cases := []struct {
		name     string
		baseline []string
		newNames []string
		gone     []string
	}{
		{name: "equal", baseline: baseline},
		{name: "a new name", baseline: baseline[:3], newNames: []string{"example.test/report.Report.String"}},
		{
			name: "a gone name", baseline: append(slices.Clone(baseline), "example.test/toy.Qty"),
			gone: []string{"example.test/toy.Qty"},
		},
	}

	for _, tc := range cases {
		got, err := testgate.CompareDeadcode(strings.NewReader(deadcodeReport), tc.baseline)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}

		if len(got.Reported) != 2 || !slices.Equal(got.New, tc.newNames) || !slices.Equal(got.Gone, tc.gone) {
			t.Errorf("%s: reported %q new %q gone %q; want 2 reported, new %q, gone %q", tc.name, got.Reported,
				got.New, got.Gone, tc.newNames, tc.gone)
		}
	}

	if _, err := testgate.CompareDeadcode(strings.NewReader("[{not json"), baseline); err == nil {
		t.Fatal("a malformed report was compared")
	}
}
