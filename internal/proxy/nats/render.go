package nats

import (
	"encoding/hex"
	"slices"
	"strings"
	"unicode/utf8"
)

// Operation names that open a rendered effect line.
const (
	opPublish = "publish"
	opCreate  = "kv.create"
	opUpdate  = "kv.update"
	opPut     = "kv.put"
	opDelete  = "kv.delete"
	opPurge   = "kv.purge"
)

const (
	// kvMarker is the segment that opens a key/value subject: $KV.<bucket>.<key>.
	kvMarker = "$KV"
	// kvSegments is how many segments a key/value subject needs after the marker: a bucket and at
	// least one segment of key.
	kvSegments = 2
	// kvOperationHeader names the two operations that cannot be told apart from the payload alone.
	kvOperationHeader = "KV-Operation"
	kvDeleteOperation = "DEL"
	kvPurgeOperation  = "PURGE"
	// expectedLastSubjectSeqHeader carries the compare-and-set the bus applies to a publish, and is
	// what makes an atomic create distinguishable from a plain put on the wire: a claim asks for
	// sequence zero, so the write commits only if nothing has ever been stored under the key. This
	// is the header that decides whether a handler's dedupe guard is visible to Stutter at all.
	expectedLastSubjectSeqHeader = "Nats-Expected-Last-Subject-Sequence"
	// createRevision is the expected sequence an atomic create asks for.
	createRevision = "0"
)

const (
	// inboxPrefix opens a reply subject a client generated for one request of its own.
	inboxPrefix = "_INBOX."
	// inboxPlaceholder stands in for such a subject in the rendered effect.
	inboxPlaceholder = "<inbox>"
)

// deleteByte is the one control character above the printable range.
const deleteByte = 0x7f

// kvSubject is a key/value subject split into the bucket and key it addresses.
type kvSubject struct {
	bucket string
	key    string
}

// render assembles the comparable, readable form of one publish.
//
// The same text serves as both: the report has to be actionable without reading Stutter's source,
// and the comparison has to be stable between runs, and nothing here needs those to differ.
func render(args publishArgs, headers []header, payload []byte) string {
	fields, rest := describe(args.subject, headers)

	if args.reply != "" {
		fields = append(fields, "reply="+renderReply(args.reply))
	}

	if text := renderHeaders(rest); text != "" {
		fields = append(fields, "headers=["+text+"]")
	}

	if len(payload) > 0 {
		fields = append(fields, "payload="+renderPayload(payload))
	}

	return strings.Join(fields, " ")
}

// describe names the operation and what it addressed, resolving a key/value subject into the bucket
// and key behind it.
//
// It also returns the headers that did not go into naming the operation, so the two the name already
// encodes are not repeated after it.
func describe(subject string, headers []header) ([]string, []header) {
	parsed, ok := splitKVSubject(subject)
	if !ok {
		return []string{opPublish, "subject=" + subject}, headers
	}

	fields := []string{kvOperationName(headers), "bucket=" + parsed.bucket, "key=" + parsed.key}

	revision, present := headerValue(headers, expectedLastSubjectSeqHeader)
	if present && revision != createRevision {
		fields = append(fields, "revision="+revision)
	}

	return fields, without(headers, kvOperationHeader, expectedLastSubjectSeqHeader)
}

// splitKVSubject splits $KV.<bucket>.<key> into its parts.
//
// The marker is located rather than assumed to be the first segment, because a bucket reached
// through a JetStream domain carries an API prefix in front of it. Everything after the bucket is
// the key, since a key may contain dots.
func splitKVSubject(subject string) (kvSubject, bool) {
	segments := strings.Split(subject, ".")

	for index, segment := range segments {
		if segment != kvMarker {
			continue
		}

		rest := segments[index+1:]
		if len(rest) < kvSegments {
			return kvSubject{}, false
		}

		return kvSubject{bucket: rest[0], key: strings.Join(rest[1:], ".")}, true
	}

	return kvSubject{}, false
}

// kvOperationName names what a publish to a key/value subject is doing.
//
// Only what the wire shows is claimed. A delete and a purge announce themselves in a header; a
// create is an atomic claim on a key nothing has written yet, which shows as a compare-and-set
// against sequence zero. Where no header settles it the write is reported as a put, because a put is
// what a bare publish to the subject is — inventing more precision than the bytes carry would put a
// distinction into a report that the report cannot support.
func kvOperationName(headers []header) string {
	if operation, present := headerValue(headers, kvOperationHeader); present {
		switch {
		case strings.EqualFold(operation, kvDeleteOperation):
			return opDelete
		case strings.EqualFold(operation, kvPurgeOperation):
			return opPurge
		default:
			// An operation name Stutter does not know still describes a write to the key, so it falls
			// through to the sequence check rather than being reported under a name invented here.
		}
	}

	revision, present := headerValue(headers, expectedLastSubjectSeqHeader)

	switch {
	case !present:
		return opPut
	case revision == createRevision:
		return opCreate
	default:
		return opUpdate
	}
}

// without drops the headers whose meaning the operation name already carries.
func without(headers []header, names ...string) []header {
	out := make([]header, 0, len(headers))

	for _, item := range headers {
		named := slices.ContainsFunc(names, func(name string) bool {
			return strings.EqualFold(name, item.name)
		})

		if named {
			continue
		}

		out = append(out, item)
	}

	return out
}

func renderHeaders(headers []header) string {
	parts := make([]string, 0, len(headers))
	for _, item := range headers {
		parts = append(parts, item.name+"="+item.value)
	}

	return strings.Join(parts, ", ")
}

// renderReply flattens a reply subject the client generated for itself.
//
// A JetStream publish is a request under the covers, and its reply subject is a fresh random inbox
// on every call. Left verbatim it differs between two runs of the same handler, so the determinism
// gate could never pass and no divergence report could ever be trusted. A reply to any other subject
// is kept: that one is the handler's own choice and is signal.
func renderReply(reply string) string {
	if strings.HasPrefix(reply, inboxPrefix) {
		return inboxPlaceholder
	}

	return reply
}

// renderPayload turns a payload into stable text.
//
// Readable text is kept as it arrived, with whitespace collapsed so that one effect stays one line.
// Keeping it verbatim is what lets provenance substitution work: it matches message-derived values
// by plain string comparison, so escaping or encoding a readable payload would hide the fact that an
// identifier came from the message and flatten real signal into noise. Anything else is hex-encoded,
// which is stable and distinguishable even though it is not readable.
func renderPayload(payload []byte) string {
	if !utf8.Valid(payload) || hasControlByte(payload) {
		return "0x" + hex.EncodeToString(payload)
	}

	return strings.Join(strings.Fields(string(payload)), " ")
}

// hasControlByte reports whether a payload carries a control character other than the whitespace
// that collapsing already handles.
func hasControlByte(payload []byte) bool {
	for _, current := range payload {
		if current == '\t' || current == '\n' || current == '\r' {
			continue
		}

		if current < ' ' || current == deleteByte {
			return true
		}
	}

	return false
}
