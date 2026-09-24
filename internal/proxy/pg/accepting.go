package pg

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"time"
)

const (
	// protocolVersion3 is the StartupMessage's protocol word: major 3, minor 0.
	protocolVersion3 = 3 << 16
	// probeUser is the role the acceptance probe names. Any role will do: a server that can serve a
	// session answers a missing one with an authentication error, which still says it accepts sessions.
	probeUser = "postgres"
	// cannotConnectNow is the SQLSTATE of a server that refuses every session for now: starting up,
	// shutting down, or in recovery.
	cannotConnectNow = "57P03"
	// errorFieldCode is the ErrorResponse field that carries the SQLSTATE.
	errorFieldCode = 'C'
	// msgTerminate is the frontend's Terminate message type.
	msgTerminate = 'X'
	// maxProbeError bounds how much of an ErrorResponse the probe reads. A "cannot connect now" error is
	// about a hundred bytes; anything longer is some other error, which means the server is serving.
	maxProbeError = 8 << 10
	// lengthWord is the size of a message's length word, which counts itself.
	lengthWord = 4
	// messageHeader is a backend message's type byte and length word.
	messageHeader = 1 + lengthWord
	// startupHeader is a StartupMessage's length word and protocol word.
	startupHeader = lengthWord + 4
)

// Accepting waits, within ctx, until the Postgres server at addr accepts sessions: it answers a
// StartupMessage with anything but SQLSTATE 57P03. It reports false when ctx ends first.
//
// Answering the SSLRequest is not enough. The postmaster answers it as soon as it listens, which is
// before it can serve a session, and a client that connects then is refused with "the database system
// is starting up". The connections are direct and nothing on them is recorded.
func Accepting(ctx context.Context, addr string) bool {
	for {
		if acceptingOnce(ctx, addr) {
			return true
		}

		timer := time.NewTimer(handshakePoll)

		select {
		case <-ctx.Done():
			timer.Stop()

			return false
		case <-timer.C:
		}
	}
}

// acceptingOnce makes one attempt: true when the server answered the StartupMessage with anything but
// a refusal of every session.
func acceptingOnce(ctx context.Context, addr string) bool {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}

	defer func() { _ = conn.Close() }()

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if _, err = conn.Write(startupMessage(probeUser)); err != nil {
		return false
	}

	if err = conn.SetReadDeadline(time.Now().Add(handshakeSilence)); err != nil {
		return false
	}

	header := make([]byte, messageHeader)
	if _, err = io.ReadFull(conn, header); err != nil {
		return false
	}

	if header[0] == msgErrorResponse && refusesSessions(conn, binary.BigEndian.Uint32(header[1:])) {
		return false
	}

	// The answer is already read; the close follows whether or not the server takes the goodbye.
	_, _ = conn.Write([]byte{msgTerminate, 0, 0, 0, lengthWord}) //nolint:errcheck // see above.

	return true
}

// refusesSessions reads an ErrorResponse's body of the given length word and reports whether its
// SQLSTATE says the server refuses every session for now. A body cut short counts as that refusal, so
// the probe asks again; one too long to be it is some other error, from a server that is serving.
func refusesSessions(conn net.Conn, length uint32) bool {
	if length < lengthWord || length-lengthWord > maxProbeError {
		return false
	}

	body := make([]byte, length-lengthWord)
	if _, err := io.ReadFull(conn, body); err != nil {
		return true
	}

	for field := range bytes.SplitSeq(body, []byte{0}) {
		if len(field) > 1 && field[0] == errorFieldCode {
			return string(field[1:]) == cannotConnectNow
		}
	}

	return false
}

// startupMessage is a protocol 3.0 StartupMessage naming user and no database, which defaults to the
// user's name.
func startupMessage(user string) []byte {
	params := []byte("user\x00" + user + "\x00\x00")
	message := make([]byte, startupHeader, startupHeader+len(params))
	binary.BigEndian.PutUint32(message[0:lengthWord], uint32(startupHeader+len(params))) //nolint:gosec // tiny.
	binary.BigEndian.PutUint32(message[lengthWord:startupHeader], protocolVersion3)

	return append(message, params...)
}
