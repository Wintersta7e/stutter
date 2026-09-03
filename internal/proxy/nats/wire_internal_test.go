package nats

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
	"testing"
)

const (
	testSubject = "orders.dispatched"
	testBucket  = "sent-envelopes"
	testKey     = "12345"
	testKVSub   = kvMarker + "." + testBucket + "." + testKey
	testPayload = `{"order_id":"ORD-9001"}`
	testInbox   = inboxPrefix + "aBc9"
)

// pubFrame builds a plain publish, whose single count is the payload alone.
func pubFrame(subject, reply string) string {
	control := "PUB " + subject + " "
	if reply != "" {
		control += reply + " "
	}

	return control + strconv.Itoa(len(testPayload)) + crlf + testPayload + crlf
}

// hpubFrame builds a headered publish exactly as a client frames one: the header count includes the
// blank line that closes the block, and the total count covers the headers and the payload together.
func hpubFrame(subject, reply, headers, payload string) string {
	block := headerVersion + crlf + headers + crlf

	control := "HPUB " + subject + " "
	if reply != "" {
		control += reply + " "
	}

	control += strconv.Itoa(len(block)) + " " + strconv.Itoa(len(block)+len(payload)) + crlf

	return control + block + payload + crlf
}

func readOne(t *testing.T, wire string) *frame {
	t.Helper()

	current, err := readFrame(bufio.NewReaderSize(strings.NewReader(wire), readBuffer))
	if err != nil {
		t.Fatalf("readFrame() error = %v", err)
	}

	return current
}

func TestReadFrameReadsPlainPublish(t *testing.T) {
	t.Parallel()

	wire := pubFrame(testSubject, "")

	current := readOne(t, wire)

	if string(current.raw) != wire {
		t.Errorf("raw = %q, want the bytes as they arrived", current.raw)
	}

	if current.args.subject != testSubject || current.args.reply != "" {
		t.Errorf("subject = %q, reply = %q", current.args.subject, current.args.reply)
	}

	if current.args.headerLen != 0 || string(current.body) != testPayload {
		t.Errorf("headerLen = %d, body = %q", current.args.headerLen, current.body)
	}
}

func TestReadFrameReadsReplyTo(t *testing.T) {
	t.Parallel()

	current := readOne(t, pubFrame(testSubject, testInbox))

	if current.args.reply != testInbox {
		t.Errorf("reply = %q, want the inbox subject", current.args.reply)
	}

	if string(current.body) != testPayload {
		t.Errorf("body = %q, want %q", current.body, testPayload)
	}
}

// TestReadFrameSplitsHeadersFromPayload is the count that decides whether a key/value claim is
// readable at all: the header count includes the blank line that ends the block, so getting it wrong
// by two bytes puts the payload's first bytes into the headers and loses the claim.
func TestReadFrameSplitsHeadersFromPayload(t *testing.T) {
	t.Parallel()

	wire := hpubFrame(testKVSub, "", expectedLastSubjectSeqHeader+": 0"+crlf, testPayload)

	current := readOne(t, wire)

	if string(current.raw) != wire {
		t.Errorf("raw = %q, want the bytes as they arrived", current.raw)
	}

	if got := string(current.body[current.args.headerLen:]); got != testPayload {
		t.Errorf("payload = %q, want %q", got, testPayload)
	}

	headers := parseHeaders(current.body[:current.args.headerLen])
	if len(headers) != 1 || headers[0].name != expectedLastSubjectSeqHeader || headers[0].value != "0" {
		t.Errorf("headers = %+v, want the expected-last-sequence header alone", headers)
	}
}

// TestReadFrameIgnoresEmptyPayloads covers a delete marker, which carries headers and nothing else.
func TestReadFrameIgnoresEmptyPayloads(t *testing.T) {
	t.Parallel()

	current := readOne(t, hpubFrame(testKVSub, "", kvOperationHeader+": DEL"+crlf, ""))

	if len(current.body[current.args.headerLen:]) != 0 {
		t.Errorf("payload = %q, want empty", current.body[current.args.headerLen:])
	}
}

func TestReadFrameLeavesNonPublishUninspected(t *testing.T) {
	t.Parallel()

	for _, wire := range []string{"SUB " + testSubject + " 1" + crlf, "PING" + crlf, "PONG" + crlf} {
		current := readOne(t, wire)

		if string(current.raw) != wire {
			t.Errorf("raw = %q, want %q", current.raw, wire)
		}

		if isPublish(current.op) {
			t.Errorf("op %q was treated as a publish", current.op)
		}

		if current.body != nil {
			t.Errorf("body = %q, want nothing read past the control line", current.body)
		}
	}
}

// TestReadFrameLowercaseOperations guards against comparing the operation byte for byte: the
// protocol is case-insensitive about it, and a client that lowercases would otherwise publish
// invisibly.
func TestReadFrameLowercaseOperations(t *testing.T) {
	t.Parallel()

	wire := strings.Replace(pubFrame(testSubject, ""), "PUB ", "pub ", 1)

	current := readOne(t, wire)

	if !isPublish(current.op) {
		t.Errorf("op = %q was not recognised as a publish", current.op)
	}
}

// TestReadFrameReportsDesyncWithoutLosingBytes is the property the whole design rests on. A publish
// whose counts do not parse leaves the proxy unable to find the next control line, so it says so —
// and still hands back the bytes it read, because the connection must survive what the parser
// cannot.
func TestReadFrameReportsDesyncWithoutLosingBytes(t *testing.T) {
	t.Parallel()

	wire := "PUB " + testSubject + " notanumber" + crlf

	current, err := readFrame(bufio.NewReaderSize(strings.NewReader(wire), readBuffer))
	if !errors.Is(err, errDesynced) {
		t.Fatalf("readFrame() error = %v, want errDesynced", err)
	}

	if current == nil || string(current.raw) != wire {
		t.Error("desynced frame did not carry the bytes that were read")
	}
}

// TestReadFrameRejectsTruncatedMessages guards the same property from the other side: a message
// Stutter cannot parse costs an effect, but must never panic or read past what arrived.
func TestReadFrameRejectsTruncatedMessages(t *testing.T) {
	t.Parallel()

	full := hpubFrame(testKVSub, testInbox, kvOperationHeader+": DEL"+crlf, testPayload)

	for cut := range len(full) {
		current, err := readFrame(bufio.NewReaderSize(strings.NewReader(full[:cut]), readBuffer))
		if err == nil {
			t.Errorf("readFrame() accepted a message truncated to %d bytes", cut)

			continue
		}

		if current != nil && errors.Is(err, errDesynced) {
			t.Errorf("truncation at %d byte(s) was reported as a framing failure", cut)
		}
	}
}

func TestParsePublishArgsRejectsMalformedCounts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		op   string
		args []string
	}{
		{name: "no count", op: opPub, args: []string{testSubject}},
		{name: "too many words", op: opPub, args: []string{testSubject, "reply", "extra", "4"}},
		{name: "negative count", op: opPub, args: []string{testSubject, "-1"}},
		{name: "count above the cap", op: opPub, args: []string{testSubject, strconv.Itoa(maxBody + 1)}},
		{name: "headers without a total", op: opHPub, args: []string{testSubject, "12"}},
		{name: "headers larger than the total", op: opHPub, args: []string{testSubject, "40", "12"}},
		{name: "unparsable total", op: opHPub, args: []string{testSubject, "12", "lots"}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if _, ok := parsePublishArgs(testCase.op, testCase.args); ok {
				t.Error("parsePublishArgs() accepted a control line it cannot frame")
			}
		})
	}
}

func TestParseHeadersSortsAndKeepsRepeats(t *testing.T) {
	t.Parallel()

	block := headerVersion + crlf +
		"Nats-Msg-Id: b" + crlf +
		"KV-Operation: DEL" + crlf +
		"Nats-Msg-Id: a" + crlf +
		"malformed line without a colon" + crlf +
		crlf

	headers := parseHeaders([]byte(block))

	want := []header{
		{name: "KV-Operation", value: "DEL"},
		{name: "Nats-Msg-Id", value: "a"},
		{name: "Nats-Msg-Id", value: "b"},
	}

	if len(headers) != len(want) {
		t.Fatalf("headers = %+v, want %+v", headers, want)
	}

	for index := range want {
		if headers[index] != want[index] {
			t.Errorf("headers[%d] = %+v, want %+v", index, headers[index], want[index])
		}
	}
}

func TestParseHeadersOnAnEmptyBlock(t *testing.T) {
	t.Parallel()

	if headers := parseHeaders(nil); len(headers) != 0 {
		t.Errorf("parseHeaders(nil) = %+v, want none", headers)
	}
}

func TestSplitControlLineOnAnEmptyLine(t *testing.T) {
	t.Parallel()

	op, args := splitControlLine([]byte(crlf))
	if op != "" || len(args) != 0 {
		t.Errorf("splitControlLine() = %q, %v; want empty", op, args)
	}
}

func TestReadLineRejectsAnEndlessLine(t *testing.T) {
	t.Parallel()

	endless := strings.Repeat("x", maxControlLine+readBuffer)

	if _, err := readLine(bufio.NewReaderSize(strings.NewReader(endless), readBuffer)); !errors.Is(err, errOversized) {
		t.Errorf("readLine() error = %v, want errOversized", err)
	}
}
