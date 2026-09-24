package pg

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Answer is what a server made of an SSLRequest.
type Answer string

const (
	// AnswerPostgres is a server that answered as Postgres does: one byte, `S` or `N`.
	AnswerPostgres Answer = "pg"
	// AnswerOther is a server that answered anything else, or held the connection silent.
	AnswerOther Answer = "other"
	// AnswerNone is a server that never accepted and kept a connection before the context ended.
	AnswerNone Answer = "none"
)

const (
	// handshakeSilence is the handshake's silence bound: a server that keeps the connection open
	// this long without a byte is not Postgres, which answers an SSLRequest at once.
	handshakeSilence = time.Second
	// handshakePoll is the pause between attempts while nothing is ready to answer. It is a poll
	// interval, not a bound: the caller's context is the only bound.
	handshakePoll = 50 * time.Millisecond
	// sslRequestLength is the SSLRequest's whole length: the length word and the request code.
	sslRequestLength = 8
	// replyBuffer holds the first read whole, so a one-byte answer is told apart from a longer one.
	replyBuffer = 64
)

// errAddress means the address cannot name a TCP endpoint at all.
var errAddress = errors.New("handshake address is not host:port")

// Handshake asks addr whether it speaks Postgres, by sending the 8-byte SSLRequest every Postgres
// server answers with one byte before anything else.
//
// A connection that closes or resets before a byte, or a dial that fails, means nothing is ready
// yet: the attempt is repeated until ctx ends, which answers AnswerNone. The connection is direct
// and nothing on it is recorded. The only error is an address that cannot be dialled at all.
func Handshake(ctx context.Context, addr string) (Answer, error) {
	if err := checkAddress(addr); err != nil {
		return AnswerNone, err
	}

	for {
		if answer, ready := handshakeOnce(ctx, addr); ready {
			return answer, nil
		}

		timer := time.NewTimer(handshakePoll)

		select {
		case <-ctx.Done():
			timer.Stop()

			return AnswerNone, nil
		case <-timer.C:
		}
	}
}

func checkAddress(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%w: %q", errAddress, addr)
	}

	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("%w: %q", errAddress, addr)
	}

	return nil
}

// handshakeOnce makes one attempt. The boolean is false when the server was not there to answer.
func handshakeOnce(ctx context.Context, addr string) (Answer, bool) {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return AnswerNone, false
	}

	defer func() { _ = conn.Close() }()

	// Closing interrupts a read the context outlived; that read then counts as not ready.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	request := make([]byte, sslRequestLength)
	binary.BigEndian.PutUint32(request[0:4], sslRequestLength)
	binary.BigEndian.PutUint32(request[4:8], sslRequest)

	if _, err = conn.Write(request); err != nil {
		return AnswerNone, false
	}

	if err = conn.SetReadDeadline(time.Now().Add(handshakeSilence)); err != nil {
		return AnswerNone, false
	}

	reply := make([]byte, replyBuffer)
	n, err := conn.Read(reply)

	var timeout net.Error

	switch {
	case n == 1 && (reply[0] == 'S' || reply[0] == 'N'):
		return AnswerPostgres, true
	case n > 0:
		return AnswerOther, true
	case errors.As(err, &timeout) && timeout.Timeout() && ctx.Err() == nil:
		return AnswerOther, true
	default:
		return AnswerNone, false
	}
}
