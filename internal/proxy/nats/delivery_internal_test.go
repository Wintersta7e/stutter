package nats

import (
	"bufio"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestReadServerFrameReadsADelivery(t *testing.T) {
	t.Parallel()

	const line = "MSG orders.created 3 $JS.ACK.ORDERS.worker.1.7.1.1700000000000000000.0 5\r\nhello\r\n"

	got, err := readServerFrame(bufio.NewReader(strings.NewReader(line)))
	if err != nil {
		t.Fatalf("readServerFrame() error = %v", err)
	}

	if got.args.subject != "orders.created" {
		t.Errorf("subject = %q, want orders.created", got.args.subject)
	}

	// The subscription id is dropped: the client chose it, and reading it as the reply subject would
	// make every delivery look like ordinary pub/sub traffic.
	if !strings.HasPrefix(got.args.reply, ackPrefix) {
		t.Errorf("reply = %q, want the acknowledgement subject", got.args.reply)
	}

	if string(got.body) != "hello" {
		t.Errorf("body = %q, want hello", got.body)
	}

	if string(got.raw) != line {
		t.Errorf("raw = %q, want the bytes to be forwarded verbatim", got.raw)
	}
}

func TestReadServerFrameReadsAHeaderedDelivery(t *testing.T) {
	t.Parallel()

	const (
		headers = "NATS/1.0\r\nNats-Sequence: 7\r\n\r\n"
		payload = "hello"
	)

	// Counted rather than written out: HMSG's second number covers the headers AND the payload, and
	// a hand-tallied one that happened to be wrong would test the wrong thing.
	line := fmt.Sprintf(
		"HMSG orders.created 3 $JS.ACK.ORDERS.worker.1.7.1.1700000000000000000.0 %d %d\r\n%s%s\r\n",
		len(headers), len(headers)+len(payload), headers, payload,
	)

	got, err := readServerFrame(bufio.NewReader(strings.NewReader(line)))
	if err != nil {
		t.Fatalf("readServerFrame() error = %v", err)
	}

	if got.args.headerLen != len(headers) {
		t.Errorf("headerLen = %d, want %d", got.args.headerLen, len(headers))
	}

	if got := string(got.body[got.args.headerLen:]); got != payload {
		t.Errorf("payload = %q, want %q", got, payload)
	}
}

// TestReadServerFrameReportsDesyncWithoutLosingBytes is the invariant that makes parsing the bus
// side safe at all: a frame Stutter cannot read costs the window, never the connection. A service
// cut off mid-run reports as a handler that stopped producing effects, which is a far worse answer
// than a missing attribution window.
func TestReadServerFrameReportsDesyncWithoutLosingBytes(t *testing.T) {
	t.Parallel()

	// No byte count, so the start of the next control line cannot be found.
	const line = "MSG orders.created 3\r\n"

	got, err := readServerFrame(bufio.NewReader(strings.NewReader(line)))
	if !errors.Is(err, errDesynced) {
		t.Fatalf("readServerFrame() error = %v, want errDesynced", err)
	}

	if got == nil || string(got.raw) != line {
		t.Fatalf("desync lost the bytes that still have to be forwarded: %+v", got)
	}
}

func TestNonDeliveryServerLinesPassThrough(t *testing.T) {
	t.Parallel()

	for _, line := range []string{"PING\r\n", "+OK\r\n", "INFO {}\r\n"} {
		got, err := readServerFrame(bufio.NewReader(strings.NewReader(line)))
		if err != nil {
			t.Fatalf("readServerFrame(%q) error = %v", line, err)
		}

		if string(got.raw) != line {
			t.Errorf("raw = %q, want %q", got.raw, line)
		}

		if isDelivery(got.op) {
			t.Errorf("%q was read as a delivery", line)
		}
	}
}
