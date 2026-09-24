package testgate_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

func TestAuditLinesFollowTheGrammar(t *testing.T) {
	t.Parallel()

	cases := []struct {
		kind   testgate.AuditKind
		check  string
		want   string
		counts []int
	}{
		{
			kind: testgate.AuditRecorder, check: "c1", counts: []int{4, 2, 0, 0},
			want: "STUTTER-AUDIT recorder check=c1 calls=4 mutating=2 prune=0 foreign-touched=0",
		},
		{
			kind: testgate.AuditTags, check: "c1", counts: []int{7, 0},
			want: "STUTTER-AUDIT tags check=c1 preexisting=7 moved=0",
		},
		{
			kind: testgate.AuditResidue, check: "c1", counts: []int{5, 0},
			want: "STUTTER-AUDIT residue check=c1 created=5 remaining=0",
		},
		{
			kind: testgate.AuditDecoys, check: "c1", counts: []int{6, 6},
			want: "STUTTER-AUDIT decoys check=c1 planted=6 survived=6",
		},
		{
			kind: testgate.AuditDSNDecoy, check: "c1", counts: []int{0},
			want: "STUTTER-AUDIT dsn-decoy check=c1 connections=0",
		},
		{
			kind: testgate.AuditSecrets, check: "c1", counts: []int{0, 3, 12},
			want: "STUTTER-AUDIT secrets check=c1 sentinel-hits=0 scanned-files=3 scanned-argv=12",
		},
		{
			kind: testgate.AuditTree, counts: []int{0},
			want: "STUTTER-AUDIT tree untracked-or-modified=0",
		},
		{
			kind: testgate.AuditStatic, counts: []int{90, 1, 1, 2, 6, 4, 2, 3},
			want: "STUTTER-AUDIT static files-go=90 files-workflow=1 files-make=1 files-script=2 spawners=6 " +
				"verbs=4 compose-sites=2 mutators=3",
		},
	}

	for _, tc := range cases {
		line, err := testgate.FormatAudit(tc.kind, tc.check, tc.counts...)
		if err != nil || line != tc.want {
			t.Errorf("FormatAudit(%s) = %q, %v; want %q", tc.kind, line, err, tc.want)

			continue
		}

		audit, ok := testgate.ParseAudit("    x_test.go:9: " + line)
		if !ok || audit.Kind != tc.kind || audit.Check != tc.check || len(audit.Counts) != len(tc.counts) {
			t.Errorf("ParseAudit(%q) = %+v, %v; want the line back", line, audit, ok)
		}

		if back, err := testgate.FormatAudit(audit.Kind, audit.Check, countsOf(t, tc.kind, audit)...); err != nil ||
			back != line {
			t.Errorf("round trip of %q gave %q, %v", line, back, err)
		}
	}
}

// countsOf returns an audit's counts in its kind's key order, read back through the grammar itself.
func countsOf(t *testing.T, kind testgate.AuditKind, audit testgate.Audit) []int {
	t.Helper()

	keys := testgate.AuditKeys(kind)
	counts := make([]int, 0, len(keys))

	for _, key := range keys {
		counts = append(counts, audit.Counts[key])
	}

	if len(counts) != len(audit.Counts) {
		t.Fatalf("%s: keys %v do not cover %v", kind, keys, audit.Counts)
	}

	return counts
}

func TestAMalformedAuditIsRefused(t *testing.T) {
	t.Parallel()

	refusals := []struct {
		kind   testgate.AuditKind
		check  string
		counts []int
	}{
		{kind: "wibble", check: "c", counts: []int{1}},
		{kind: testgate.AuditRecorder, check: "c", counts: []int{1, 2, 3}},
		{kind: testgate.AuditRecorder, check: "", counts: []int{1, 2, 3, 4}},
		{kind: testgate.AuditRecorder, check: "a b", counts: []int{1, 2, 3, 4}},
		{kind: testgate.AuditTree, check: "c", counts: []int{0}},
		{kind: testgate.AuditTree, counts: []int{-1}},
	}

	for _, r := range refusals {
		if line, err := testgate.FormatAudit(r.kind, r.check, r.counts...); err == nil {
			t.Errorf("FormatAudit(%q, %q, %v) = %q; want a refusal", r.kind, r.check, r.counts, line)
		}
	}

	for _, line := range []string{
		"STUTTER-AUDIT tree created=1",
		"STUTTER-AUDIT tree untracked-or-modified=x",
		"STUTTER-AUDIT recorder calls=1 mutating=0 prune=0 foreign-touched=0",
		"STUTTER-AUDIT recorder check=c mutating=0 calls=1 prune=0 foreign-touched=0",
		"STUTTER-AUDIT wibble check=c n=1",
		"no audit here",
	} {
		if audit, ok := testgate.ParseAudit(line); ok {
			t.Errorf("ParseAudit(%q) = %+v; want not an audit line", line, audit)
		}
	}
}
