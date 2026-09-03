package pg

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

// testStatement is the prepared-statement name used across these fixtures.
const testStatement = "stmt1"

func cstr(value string) []byte {
	return append([]byte(value), 0)
}

func i16(value int16) []byte {
	out := make([]byte, 2)
	binary.BigEndian.PutUint16(out, uint16(value)) //nolint:gosec // test fixture, values are small.

	return out
}

func i32(value int32) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(value)) //nolint:gosec // test fixture.

	return out
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}

	return out
}

func TestParseStatementReadsSQLAndTypes(t *testing.T) {
	t.Parallel()

	body := concat(
		cstr(testStatement),
		cstr("SELECT qty FROM stock WHERE sku = $1"),
		i16(1),
		i32(25), // text
	)

	name, parsed, ok := parseStatement(body)
	if !ok {
		t.Fatal("parseStatement() reported failure on a well-formed message")
	}

	if name != testStatement {
		t.Errorf("name = %q, want %q", name, testStatement)
	}

	if parsed.sql != "SELECT qty FROM stock WHERE sku = $1" {
		t.Errorf("sql = %q", parsed.sql)
	}

	if len(parsed.oids) != 1 || parsed.oids[0] != 25 {
		t.Errorf("oids = %v, want [25]", parsed.oids)
	}
}

// TestParseStatementWithoutTypes covers the case that actually occurs in practice: clients send
// Parse with no parameter types and let the server infer them, which is why the backend's
// ParameterDescription has to be read at all.
func TestParseStatementWithoutTypes(t *testing.T) {
	t.Parallel()

	body := concat(cstr("stmt2"), cstr("INSERT INTO audit VALUES ($1)"), i16(0))

	_, parsed, ok := parseStatement(body)
	if !ok {
		t.Fatal("parseStatement() reported failure")
	}

	if len(parsed.oids) != 0 {
		t.Errorf("oids = %v, want empty", parsed.oids)
	}
}

func TestParseDescribeOnlyAcceptsStatements(t *testing.T) {
	t.Parallel()

	name, ok := parseDescribe(concat([]byte{describeStatement}, cstr(testStatement)))
	if !ok || name != testStatement {
		t.Errorf("parseDescribe(statement) = %q, %v; want %q, true", name, ok, testStatement)
	}

	// A portal describe answers with row types, not parameter types, so it must be ignored.
	if _, ok := parseDescribe(concat([]byte{'P'}, cstr("portal1"))); ok {
		t.Error("parseDescribe() accepted a portal describe")
	}
}

func TestParseParameterDescription(t *testing.T) {
	t.Parallel()

	oids, ok := parseParameterDescription(concat(i16(3), i32(25), i32(oidTimestampTZ), i32(23)))
	if !ok {
		t.Fatal("parseParameterDescription() reported failure")
	}

	want := []uint32{25, oidTimestampTZ, 23}
	if len(oids) != len(want) {
		t.Fatalf("oids = %v, want %v", oids, want)
	}

	for index := range want {
		if oids[index] != want[index] {
			t.Errorf("oids[%d] = %d, want %d", index, oids[index], want[index])
		}
	}
}

func TestParseBindReadsParameters(t *testing.T) {
	t.Parallel()

	body := concat(
		cstr("portal1"),
		cstr(testStatement),
		i16(2), i16(textFormat), i16(binaryFormat),
		i16(3),
		i32(6), []byte("WIDGET"),
		i32(4), []byte{0x00, 0x00, 0x00, 0x03},
		i32(-1),
	)

	name, bound, ok := parseBind(body)
	if !ok {
		t.Fatal("parseBind() reported failure on a well-formed message")
	}

	if name != "portal1" || bound.statement != testStatement {
		t.Errorf("portal = %q, statement = %q", name, bound.statement)
	}

	if len(bound.params) != 3 {
		t.Fatalf("params = %d, want 3", len(bound.params))
	}

	if got := renderParam(bound.params[0], 0); got != "WIDGET" {
		t.Errorf("text param = %q, want %q", got, "WIDGET")
	}

	if got := renderParam(bound.params[1], 23); got != "0x00000003" {
		t.Errorf("binary param = %q, want %q", got, "0x00000003")
	}

	if got := renderParam(bound.params[2], 0); got != "NULL" {
		t.Errorf("null param = %q, want NULL", got)
	}
}

// TestRenderParamDecodesBinaryTimestamps is the fix for the first real determinism gate failure: a
// wall-clock stamp arrived as an opaque 8-byte blob, which the text-based normaliser could not see,
// so two clean runs could never agree.
func TestRenderParamDecodesBinaryTimestamps(t *testing.T) {
	t.Parallel()

	// 2026-09-03T15:00:00Z expressed as microseconds since the Postgres epoch.
	epoch := time.Date(postgresEpochYear, time.January, 1, 0, 0, 0, 0, time.UTC)
	target := time.Date(2026, time.September, 3, 15, 0, 0, 0, time.UTC)
	micros := target.Sub(epoch).Microseconds()

	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, uint64(micros)) //nolint:gosec // positive by construction.

	got := renderParam(boundParam{raw: raw, format: binaryFormat}, oidTimestampTZ)
	if !strings.HasPrefix(got, "2026-09-03T15:00:00") {
		t.Errorf("renderParam() = %q, want an RFC 3339 timestamp", got)
	}

	// The same bytes under a non-temporal type must stay opaque rather than be misread as a time.
	if got := renderParam(boundParam{raw: raw, format: binaryFormat}, 20); !strings.HasPrefix(got, "0x") {
		t.Errorf("renderParam() on a bigint = %q, want hex", got)
	}
}

func TestFormatForResolvesCodes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		codes []int16
		index int
		want  int16
	}{
		{name: "no codes means text", codes: nil, index: 3, want: textFormat},
		{name: "one code applies to all", codes: []int16{binaryFormat}, index: 5, want: binaryFormat},
		{name: "positional", codes: []int16{textFormat, binaryFormat}, index: 1, want: binaryFormat},
		{
			name:  "out of range falls back to text",
			codes: []int16{binaryFormat, binaryFormat},
			index: 9,
			want:  textFormat,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := formatFor(testCase.codes, testCase.index); got != testCase.want {
				t.Errorf("formatFor() = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestRenderCollapsesWhitespace(t *testing.T) {
	t.Parallel()

	got := render("UPDATE stock\n  SET qty = $1\n  WHERE sku = $2", []string{"3", "WIDGET-7"})
	want := "UPDATE stock SET qty = $1 WHERE sku = $2 -- args: 3, WIDGET-7"

	if got != want {
		t.Errorf("render() = %q, want %q", got, want)
	}
}

// TestParsersRejectTruncatedMessages guards the property the whole design rests on: a message
// Stutter cannot parse costs an effect, but must never panic or corrupt the connection.
func TestParsersRejectTruncatedMessages(t *testing.T) {
	t.Parallel()

	full := concat(cstr("portal1"), cstr(testStatement), i16(1), i16(textFormat), i16(1), i32(2), []byte("hi"))

	for cut := range full {
		truncated := full[:cut]

		if _, _, ok := parseBind(truncated); ok && cut < len(full) {
			t.Errorf("parseBind() accepted a message truncated to %d bytes", cut)
		}

		parseStatement(truncated)
		parseDescribe(truncated)
		parseParameterDescription(truncated)
	}
}
