package nats

import (
	"bufio"
	"cmp"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// Client operations Stutter parses. The protocol is case-insensitive about them, so they are matched
// as such rather than compared byte for byte.
const (
	opPub  = "PUB"
	opHPub = "HPUB"
)

const (
	// crlf terminates every control line, every header line and every message body.
	crlf = "\r\n"
	// headerVersion opens a header block, ahead of the "Name: value" lines.
	headerVersion = "NATS/1.0"
	// maxControlLine caps a protocol control line. A server's own limit is 4096 bytes by default and
	// is raised in kilobytes where it is raised at all, so a megabyte sits far above any line the
	// upstream would accept: the cap stops a runaway stream without ever breaking a legal connection.
	maxControlLine = 1 << 20
	// maxBody caps the headers and payload of one publish. The largest max_payload a server will
	// accept is 8 MiB; the headroom above that keeps a malformed count from allocating unboundedly
	// while leaving every message the upstream would have accepted parseable.
	maxBody = 16 << 20
)

// errDesynced means a publish control line carried no readable byte counts, so the start of the next
// control line cannot be found. Effects stop there; bytes do not.
var errDesynced = errors.New("publish control line is not framed as the protocol requires")

// errOversized means a control line ran past the cap without ending, which is not a NATS stream.
var errOversized = errors.New("control line exceeds the size cap")

// header is one message header as it arrived.
//
// Headers are kept as pairs rather than as a map because a map would lose a repeated name and would
// iterate in a random order, and a rendering whose field order changes between runs fails the
// determinism gate for reasons the service under test is not responsible for.
type header struct {
	name  string
	value string
}

// publishArgs is the control line of a PUB or HPUB after its arguments have been read.
//
// bodyLen counts the headers and the payload together, which is both what HPUB's second count means
// and what has to be read off the wire; headerLen is how much of that body is the header block.
type publishArgs struct {
	subject   string
	reply     string
	headerLen int
	bodyLen   int
}

// frame is one client protocol message: the exact bytes to forward, and what parsing made of them.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type frame struct {
	raw  []byte
	body []byte
	op   string
	args publishArgs
}

// readFrame reads one client message, leaving the connection positioned at the next control line.
//
// A publish whose counts do not parse returns errDesynced together with the line that was read, so
// the caller can still forward those bytes: the message costs an effect, never the connection.
func readFrame(from *bufio.Reader) (*frame, error) {
	line, err := readLine(from)
	if err != nil {
		return nil, err
	}

	op, args := splitControlLine(line)
	if !isPublish(op) {
		return &frame{raw: line, op: op}, nil
	}

	parsed, ok := parsePublishArgs(op, args)
	if !ok {
		return &frame{raw: line, op: op}, errDesynced
	}

	body := make([]byte, parsed.bodyLen+len(crlf))
	if _, readErr := io.ReadFull(from, body); readErr != nil {
		return nil, fmt.Errorf("read %s body: %w", op, readErr)
	}

	raw := make([]byte, 0, len(line)+len(body))
	raw = append(raw, line...)
	raw = append(raw, body...)

	return &frame{raw: raw, body: body[:parsed.bodyLen], args: parsed, op: op}, nil
}

// readLine reads one control line including its terminator.
//
// The line is copied out of the buffered reader rather than aliased, because the next read reuses
// that buffer and the bytes still have to be forwarded verbatim afterwards.
func readLine(from *bufio.Reader) ([]byte, error) {
	var line []byte

	for {
		chunk, err := from.ReadSlice('\n')
		line = append(line, chunk...)

		if err == nil {
			return line, nil
		}

		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, fmt.Errorf("read control line: %w", err)
		}

		if len(line) > maxControlLine {
			return nil, fmt.Errorf("%w: %d bytes", errOversized, len(line))
		}
	}
}

// splitControlLine separates the operation from its arguments.
func splitControlLine(line []byte) (string, []string) {
	fields := strings.Fields(string(line))
	if len(fields) == 0 {
		return "", nil
	}

	return fields[0], fields[1:]
}

// isPublish reports whether an operation puts a message onto the bus.
//
// Only these two are effects. A subscription, a ping and everything the bus sends back are not:
// divergence is decided by what the service did, not by what it received or asked to receive.
func isPublish(op string) bool {
	return strings.EqualFold(op, opPub) || strings.EqualFold(op, opHPub)
}

func parsePublishArgs(op string, args []string) (publishArgs, bool) {
	if strings.EqualFold(op, opHPub) {
		return parseHeaderedArgs(args)
	}

	return parsePlainArgs(args)
}

// parsePlainArgs reads "PUB <subject> [reply-to] <#bytes>", whose one count is the payload alone.
func parsePlainArgs(args []string) (publishArgs, bool) {
	const counts = 1

	parsed, sizes, ok := splitSubjectAndSizes(args, counts)
	if !ok {
		return publishArgs{}, false
	}

	size, ok := parseSize(sizes[0])
	if !ok {
		return publishArgs{}, false
	}

	parsed.bodyLen = size

	return parsed, true
}

// parseHeaderedArgs reads "HPUB <subject> [reply-to] <#header bytes> <#total bytes>".
//
// The header count includes the blank line that closes the block, and the total count covers the
// headers and the payload together — so the payload is the difference, never a third count.
func parseHeaderedArgs(args []string) (publishArgs, bool) {
	const counts = 2

	parsed, sizes, ok := splitSubjectAndSizes(args, counts)
	if !ok {
		return publishArgs{}, false
	}

	headerLen, ok := parseSize(sizes[0])
	if !ok {
		return publishArgs{}, false
	}

	total, ok := parseSize(sizes[1])
	if !ok || headerLen > total {
		return publishArgs{}, false
	}

	parsed.headerLen = headerLen
	parsed.bodyLen = total

	return parsed, true
}

// splitSubjectAndSizes separates the subject and the optional reply-to from the trailing counts.
func splitSubjectAndSizes(args []string, counts int) (publishArgs, []string, bool) {
	const (
		withoutReply = 1
		withReply    = 2
	)

	leading := len(args) - counts
	if leading != withoutReply && leading != withReply {
		return publishArgs{}, nil, false
	}

	parsed := publishArgs{subject: args[0]}
	if leading == withReply {
		parsed.reply = args[1]
	}

	return parsed, args[leading:], true
}

func parseSize(text string) (int, bool) {
	size, err := strconv.Atoi(text)
	if err != nil || size < 0 || size > maxBody {
		return 0, false
	}

	return size, true
}

// parseHeaders reads a header block: a version line, then "Name: value" lines, then the blank line
// that closes it.
//
// A line without a colon is skipped rather than guessed at, which is also how the version line and
// the closing blank line fall away.
func parseHeaders(block []byte) []header {
	lines := strings.Split(string(block), crlf)
	out := make([]header, 0, len(lines))

	for index, line := range lines {
		if index == 0 && strings.HasPrefix(line, headerVersion) {
			continue
		}

		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}

		out = append(out, header{name: strings.TrimSpace(name), value: strings.TrimSpace(value)})
	}

	slices.SortFunc(out, func(a, b header) int {
		if diff := cmp.Compare(a.name, b.name); diff != 0 {
			return diff
		}

		return cmp.Compare(a.value, b.value)
	})

	return out
}

// headerValue looks a header up by name, case-insensitively: the name is a wire token whose casing
// is the client library's choice, not the handler's.
func headerValue(headers []header, name string) (string, bool) {
	for _, item := range headers {
		if strings.EqualFold(item.name, name) {
			return item.value, true
		}
	}

	return "", false
}
