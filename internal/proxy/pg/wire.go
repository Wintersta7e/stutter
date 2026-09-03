package pg

import (
	"encoding/binary"
	"encoding/hex"
	"strings"
	"time"
)

// Frontend message types Stutter parses. Everything else is forwarded without inspection.
const (
	msgQuery    = 'Q'
	msgParse    = 'P'
	msgBind     = 'B'
	msgExecute  = 'E'
	msgDescribe = 'D'
)

// msgParameterDescription is the one backend message Stutter reads, to learn the parameter types a
// client left for the server to infer.
const msgParameterDescription = 't'

// describeStatement is the Describe target byte for a prepared statement, as opposed to a portal.
const describeStatement = 'S'

// parseDescribe reads a Describe message, returning the prepared statement being described.
//
// Portal describes are ignored: parameter types belong to the statement, and a portal's description
// answers with row types instead.
func parseDescribe(body []byte) (string, bool) {
	if len(body) == 0 || body[0] != describeStatement {
		return "", false
	}

	return (&reader{buf: body, pos: 1}).cstring()
}

// parseParameterDescription reads the server's resolved parameter types.
func parseParameterDescription(body []byte) ([]uint32, bool) {
	msg := &reader{buf: body}

	count, ok := msg.int16()
	if !ok || count < 0 {
		return nil, false
	}

	oids := make([]uint32, 0, count)

	for range int(count) {
		oid, ok := msg.uint32()
		if !ok {
			return nil, false
		}

		oids = append(oids, oid)
	}

	return oids, true
}

// Startup request codes that precede a normal startup message.
const (
	sslRequest    = 80877103
	gssEncRequest = 80877104
)

const (
	textFormat   int16 = 0
	binaryFormat int16 = 1
)

// Postgres type OIDs whose binary encoding is a count of microseconds.
//
// These are decoded rather than hex-rendered because a wall-clock stamp is the single most common
// source of run-to-run drift, and hex hides it from the normaliser: the timestamp patterns work on
// text. Decoding turns an opaque 0x0002fd95… into something the canonicaliser can recognise.
const (
	oidTimestamp   = 1114
	oidTimestampTZ = 1184
)

// postgresEpochYear is the origin of Postgres' binary temporal encoding.
const postgresEpochYear = 2000

// statement is a parsed statement and the parameter types it declared.
type statement struct {
	sql  string
	oids []uint32
}

// boundParam is one parameter as it arrived on the wire, held undecoded because the type that
// decodes it is not known until Execute names the statement.
type boundParam struct {
	raw    []byte
	format int16
	isNull bool
}

// portal is a bound statement waiting to be executed.
type portal struct {
	statement string
	params    []boundParam
}

// reader walks a message body, reporting failure rather than panicking on a truncated or malformed
// message. A message Stutter cannot parse is forwarded untouched and simply produces no effect;
// losing an effect is recoverable, corrupting the connection is not.
type reader struct {
	buf []byte
	pos int
}

func (r *reader) cstring() (string, bool) {
	end := r.pos

	for end < len(r.buf) && r.buf[end] != 0 {
		end++
	}

	if end >= len(r.buf) {
		return "", false
	}

	value := string(r.buf[r.pos:end])
	r.pos = end + 1

	return value, true
}

func (r *reader) int16() (int16, bool) {
	const width = 2
	if r.pos+width > len(r.buf) {
		return 0, false
	}

	// The protocol defines this field as signed; the conversion is the decode, not a truncation.
	value := int16(binary.BigEndian.Uint16(r.buf[r.pos:])) //nolint:gosec // signed by protocol.
	r.pos += width

	return value, true
}

func (r *reader) int32() (int32, bool) {
	const width = 4
	if r.pos+width > len(r.buf) {
		return 0, false
	}

	// Signed by protocol definition: a length of -1 is how Postgres encodes a NULL parameter.
	value := int32(binary.BigEndian.Uint32(r.buf[r.pos:])) //nolint:gosec // signed by protocol.
	r.pos += width

	return value, true
}

func (r *reader) uint32() (uint32, bool) {
	const width = 4
	if r.pos+width > len(r.buf) {
		return 0, false
	}

	value := binary.BigEndian.Uint32(r.buf[r.pos:])
	r.pos += width

	return value, true
}

func (r *reader) bytes(count int) ([]byte, bool) {
	if count < 0 || r.pos+count > len(r.buf) {
		return nil, false
	}

	value := r.buf[r.pos : r.pos+count]
	r.pos += count

	return value, true
}

// parseStatement reads a Parse message, returning the prepared statement name and its definition.
func parseStatement(body []byte) (string, statement, bool) {
	msg := &reader{buf: body}

	name, ok := msg.cstring()
	if !ok {
		return "", statement{}, false
	}

	sql, ok := msg.cstring()
	if !ok {
		return "", statement{}, false
	}

	count, ok := msg.int16()
	if !ok || count < 0 {
		// A Parse without a readable parameter count still carries usable SQL; the parameters are
		// simply rendered without type information.
		return name, statement{sql: sql}, true
	}

	oids := make([]uint32, 0, count)

	for range int(count) {
		oid, ok := msg.uint32()
		if !ok {
			return name, statement{sql: sql}, true
		}

		oids = append(oids, oid)
	}

	return name, statement{sql: sql, oids: oids}, true
}

// parseBind reads a Bind message, returning the portal name and its undecoded parameters.
func parseBind(body []byte) (string, portal, bool) {
	msg := &reader{buf: body}

	name, ok := msg.cstring()
	if !ok {
		return "", portal{}, false
	}

	target, ok := msg.cstring()
	if !ok {
		return "", portal{}, false
	}

	formats, ok := readFormatCodes(msg)
	if !ok {
		return "", portal{}, false
	}

	params, ok := readParams(msg, formats)
	if !ok {
		return "", portal{}, false
	}

	return name, portal{statement: target, params: params}, true
}

func readFormatCodes(msg *reader) ([]int16, bool) {
	count, ok := msg.int16()
	if !ok || count < 0 {
		return nil, false
	}

	codes := make([]int16, 0, count)

	for range int(count) {
		code, ok := msg.int16()
		if !ok {
			return nil, false
		}

		codes = append(codes, code)
	}

	return codes, true
}

func readParams(msg *reader, formats []int16) ([]boundParam, bool) {
	count, ok := msg.int16()
	if !ok || count < 0 {
		return nil, false
	}

	params := make([]boundParam, 0, count)

	for index := range int(count) {
		size, ok := msg.int32()
		if !ok {
			return nil, false
		}

		if size < 0 {
			params = append(params, boundParam{isNull: true})

			continue
		}

		raw, ok := msg.bytes(int(size))
		if !ok {
			return nil, false
		}

		params = append(params, boundParam{raw: raw, format: formatFor(formats, index)})
	}

	return params, true
}

// formatFor resolves the wire format of one parameter. No codes means every parameter is text; a
// single code applies to all of them; otherwise codes are positional.
func formatFor(codes []int16, index int) int16 {
	switch {
	case len(codes) == 0:
		return textFormat
	case len(codes) == 1:
		return codes[0]
	case index < len(codes):
		return codes[index]
	default:
		return textFormat
	}
}

// renderParams turns bound parameters into comparable text, using the statement's declared types
// where they matter.
func renderParams(params []boundParam, oids []uint32) []string {
	out := make([]string, 0, len(params))

	for index, param := range params {
		out = append(out, renderParam(param, oidAt(oids, index)))
	}

	return out
}

func oidAt(oids []uint32, index int) uint32 {
	if index < len(oids) {
		return oids[index]
	}

	return 0
}

// renderParam turns one bound parameter into comparable text.
//
// Text parameters pass through. Binary temporal parameters are decoded, because leaving them as hex
// hides the commonest source of run-to-run drift from the normaliser. Everything else binary is
// hex-encoded: the value only has to be stable and distinguishable, and decoding the rest would
// mean carrying Postgres' whole type catalogue for no gain in the comparison.
func renderParam(param boundParam, oid uint32) string {
	if param.isNull {
		return "NULL"
	}

	if param.format != binaryFormat {
		return string(param.raw)
	}

	if decoded, ok := decodeBinaryTime(param.raw, oid); ok {
		return decoded
	}

	return "0x" + hex.EncodeToString(param.raw)
}

// decodeBinaryTime renders a binary timestamp as text so the canonicaliser's timestamp pattern can
// see it.
func decodeBinaryTime(raw []byte, oid uint32) (string, bool) {
	if oid != oidTimestamp && oid != oidTimestampTZ {
		return "", false
	}

	const width = 8

	if len(raw) != width {
		return "", false
	}

	// Signed: timestamps before the Postgres epoch are negative.
	micros := int64(binary.BigEndian.Uint64(raw)) //nolint:gosec // signed by protocol.
	epoch := time.Date(postgresEpochYear, time.January, 1, 0, 0, 0, 0, time.UTC)

	return epoch.Add(time.Duration(micros) * time.Microsecond).Format(time.RFC3339Nano), true
}

// render assembles the comparable form of a statement and its parameters.
func render(sql string, params []string) string {
	text := strings.Join(strings.Fields(sql), " ")
	if len(params) == 0 {
		return text
	}

	return text + " -- args: " + strings.Join(params, ", ")
}
