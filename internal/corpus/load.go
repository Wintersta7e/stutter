package corpus

import (
	"cmp"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/nats-io/nats-server/v2/server"
)

// This file owns the corpus directory grammar and its header sidecar — the published corpus format.
// Users hand-write it and a recorder will write it, so it is implemented exactly and nothing is added.

const (
	// maxMessage is the embedded server's max_payload, which a message's header block and payload
	// share. A message past it would be refused at publish, half way through staging a run.
	maxMessage = 1 << 20
	// sidecarExt names a message's header sidecar.
	sidecarExt = "headers"
	// msgIDHeader is the one server-directing header a sidecar may carry: the stream's duplicate window
	// reads it, as production's does.
	msgIDHeader = "Nats-Msg-Id"
	// serverHeaders opens every header name that directs the server rather than describing the message.
	serverHeaders = "nats-"
	// headerOpen and headerLine are the encoded header block's framing, counted against max_payload.
	headerOpen = len("NATS/1.0\r\n") + len("\r\n")
	headerLine = len(": \r\n")
	// hexDigits is how many hex digits follow a percent sign in an encoded subject.
	hexDigits = 2
	// messageExts lists the extensions a message file may carry, for an error message.
	messageExts = "bin, json, txt"
)

// ErrInvalidCorpus means the corpus directory breaks its grammar. The error names the file.
var ErrInvalidCorpus = errors.New("invalid corpus directory")

// Loaded is a corpus read from a directory.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Loaded struct {
	// Messages are the corpus in order. Each one's Seq is its rank, 1 to N.
	Messages []Message
	// Files names the file holding each message: Files[i] holds Messages[i].
	Files []string
	// Sidecars counts the header sidecars joined to a message.
	Sidecars int
	// Skipped counts the entries whose name begins with a dot.
	Skipped int
}

// corpusFile is one entry's name, read against the grammar.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type corpusFile struct {
	// name is the entry's base name.
	name string
	// stem is <order>.<subject> as spelled, which a sidecar must spell identically.
	stem string
	// order is the numeric order with its zero padding stripped.
	order   string
	subject string
	sidecar bool
}

// LoadDir reads a corpus directory: one message per file, ordered by the number its name starts with,
// with optional header sidecars.
//
// It opens nothing for writing. An absent or unreadable directory is the operating system's error,
// naming the path; every grammar error wraps ErrInvalidCorpus and names the file.
func LoadDir(dir string) (Loaded, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return Loaded{}, fmt.Errorf("read the corpus directory: %w", err)
	}

	var (
		loaded             Loaded
		messages, sidecars []corpusFile
	)

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			loaded.Skipped++

			continue
		}

		if !entry.Type().IsRegular() {
			return Loaded{}, invalid(entry.Name(), "is not a regular file")
		}

		file, parseErr := parseName(entry.Name())
		if parseErr != nil {
			return Loaded{}, parseErr
		}

		if file.sidecar {
			sidecars = append(sidecars, file)
		} else {
			messages = append(messages, file)
		}
	}

	if len(messages) == 0 {
		return Loaded{}, fmt.Errorf("%w: %s: no message files", ErrInvalidCorpus, dir)
	}

	if orderErr := order(messages); orderErr != nil {
		return Loaded{}, orderErr
	}

	headers, err := readSidecars(dir, messages, sidecars)
	if err != nil {
		return Loaded{}, err
	}

	loaded.Sidecars = len(sidecars)

	return read(dir, messages, headers, loaded)
}

// Admitted narrows messages to those a consumer's filter subjects admit, in corpus order. No filter
// admits every message.
//
// It is the one matcher: nothing else hand-rolls subject wildcards, so admission and every count
// built on it agree.
func Admitted(messages []Message, filters []string) []Message {
	if len(filters) == 0 {
		return messages
	}

	admitted := make([]Message, 0, len(messages))

	for _, message := range messages {
		if slices.ContainsFunc(filters, func(filter string) bool {
			return server.SubjectMatchesFilter(message.Subject, filter)
		}) {
			admitted = append(admitted, message)
		}
	}

	return admitted
}

// read loads each message file's payload in order and joins its headers.
func read(dir string, messages []corpusFile, headers map[string]map[string][]string, loaded Loaded) (Loaded, error) {
	loaded.Messages = make([]Message, 0, len(messages))
	loaded.Files = make([]string, 0, len(messages))

	for rank, file := range messages {
		payload, err := os.ReadFile(filepath.Join(dir, file.name))
		if err != nil {
			return Loaded{}, fmt.Errorf("read corpus file: %w", err)
		}

		header := headers[file.stem]
		if size := headerSize(header) + len(payload); size > maxMessage {
			return Loaded{}, invalid(file.name, fmt.Sprintf(
				"headers and payload are %d bytes, over the %d the bus accepts", size, maxMessage))
		}

		loaded.Messages = append(loaded.Messages, Message{
			Header:  header,
			Subject: file.subject,
			Payload: payload,
			Seq:     uint64(rank) + 1,
		})
		loaded.Files = append(loaded.Files, file.name)
	}

	return loaded, nil
}

// parseName reads <order>.<subject>.<ext>.
func parseName(name string) (corpusFile, error) {
	first, last := strings.IndexByte(name, '.'), strings.LastIndexByte(name, '.')
	if first < 0 {
		return corpusFile{}, invalid(name, "is not named <order>.<subject>.<ext>")
	}

	digits := name[:first]
	if digits == "" || strings.ContainsFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) {
		return corpusFile{}, invalid(name, "does not begin with a numeric order")
	}

	ext := name[last+1:]
	sidecar := ext == sidecarExt

	if !sidecar && !isMessageExt(ext) {
		return corpusFile{}, invalid(name, fmt.Sprintf("has extension %q, want one of %s or %s",
			ext, messageExts, sidecarExt))
	}

	subject, err := decodeSubject(name[min(first+1, last):last])
	if err != nil {
		return corpusFile{}, invalid(name, err.Error())
	}

	if !server.IsValidPublishSubject(subject) || strings.HasPrefix(subject, "$") {
		return corpusFile{}, invalid(name, fmt.Sprintf("subject %q is not one a message can be published to", subject))
	}

	return corpusFile{
		name:    name,
		stem:    name[:last],
		order:   strings.TrimLeft(digits, "0"),
		subject: subject,
		sidecar: sidecar,
	}, nil
}

// decodeSubject undoes %XX, which exists because a subject may hold characters a file name cannot.
//
// An encoded dot is refused: it would be a second spelling of the token separator, and two files
// naming one subject differently is a mistake waiting to happen.
func decodeSubject(encoded string) (string, error) {
	var decoded strings.Builder

	for at := 0; at < len(encoded); at++ {
		if encoded[at] != '%' {
			decoded.WriteByte(encoded[at])

			continue
		}

		if at+hexDigits >= len(encoded) {
			return "", fmt.Errorf("%w: a %% is not followed by two hex digits", errEncoding)
		}

		value, err := hex.DecodeString(encoded[at+1 : at+1+hexDigits])
		if err != nil {
			return "", fmt.Errorf("%w: a %% is not followed by two hex digits", errEncoding)
		}

		if value[0] == '.' {
			return "", fmt.Errorf("%w: %%2E spells a token separator a second way", errEncoding)
		}

		decoded.Write(value)

		at += hexDigits
	}

	return decoded.String(), nil
}

// errEncoding means a subject's percent-encoding is malformed.
var errEncoding = errors.New("malformed subject encoding")

// isMessageExt reports whether ext is one a message file may carry. The extension means nothing; one
// is required so a missing extension cannot silently become part of the subject.
func isMessageExt(ext string) bool {
	switch ext {
	case "bin", "json", "txt":
		return true
	default:
		return false
	}
}

// order sorts message files by their numeric order, compared as unbounded integers, and refuses two
// with the same one.
func order(messages []corpusFile) error {
	slices.SortStableFunc(messages, compareOrder)

	for at := 1; at < len(messages); at++ {
		if compareOrder(messages[at-1], messages[at]) == 0 {
			return fmt.Errorf("%w: %s and %s have the same order",
				ErrInvalidCorpus, messages[at-1].name, messages[at].name)
		}
	}

	return nil
}

// compareOrder compares two stripped digit strings as integers: the longer is larger, and equal
// lengths compare by their digits.
func compareOrder(a, b corpusFile) int {
	if byLength := cmp.Compare(len(a.order), len(b.order)); byLength != 0 {
		return byLength
	}

	return strings.Compare(a.order, b.order)
}

// readSidecars parses every sidecar and keys its headers by the stem of the message it belongs to.
func readSidecars(dir string, messages, sidecars []corpusFile) (map[string]map[string][]string, error) {
	stems := make(map[string]bool, len(messages))
	for _, message := range messages {
		stems[message.stem] = true
	}

	headers := make(map[string]map[string][]string, len(sidecars))
	// ids remembers which sidecar carried each message id: the stream's duplicate window would drop
	// the second message at Fill.
	ids := make(map[string]string)

	for _, sidecar := range sidecars {
		if !stems[sidecar.stem] {
			return nil, invalid(sidecar.name, "is a header sidecar with no message file of the same name")
		}

		content, err := os.ReadFile(filepath.Join(dir, sidecar.name))
		if err != nil {
			return nil, fmt.Errorf("read corpus file: %w", err)
		}

		header, err := parseSidecar(sidecar.name, content)
		if err != nil {
			return nil, err
		}

		for _, id := range header[msgIDHeader] {
			if other, seen := ids[id]; seen {
				return nil, fmt.Errorf("%w: %s and %s carry the same %s %q",
					ErrInvalidCorpus, other, sidecar.name, msgIDHeader, id)
			}

			ids[id] = sidecar.name
		}

		headers[sidecar.stem] = header
	}

	return headers, nil
}

// parseSidecar reads one "Name: value" per line; a name repeats to carry several values.
func parseSidecar(name string, content []byte) (map[string][]string, error) {
	if !utf8.Valid(content) {
		return nil, invalid(name, "is not UTF-8")
	}

	var header map[string][]string

	for at, line := range strings.Split(strings.ReplaceAll(string(content), "\r\n", "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}

		key, value, found := strings.Cut(line, ":")
		if !found || key == "" || strings.ContainsFunc(key, unicode.IsSpace) {
			return nil, invalid(name, fmt.Sprintf("line %d is not a Name: value header", at+1))
		}

		if strings.HasPrefix(strings.ToLower(key), serverHeaders) && key != msgIDHeader {
			return nil, invalid(name, fmt.Sprintf("line %d sets %s, which directs the server rather than "+
				"describing the message", at+1, key))
		}

		if header == nil {
			header = make(map[string][]string)
		}

		header[key] = append(header[key], strings.TrimPrefix(value, " "))
	}

	return header, nil
}

// headerSize is how many bytes a header block adds to a message on the wire; none without headers.
func headerSize(header map[string][]string) int {
	if len(header) == 0 {
		return 0
	}

	size := headerOpen

	for name, values := range header {
		for _, value := range values {
			size += len(name) + len(value) + headerLine
		}
	}

	return size
}

// invalid is a grammar error naming its file.
func invalid(name, reason string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalidCorpus, name, reason)
}
