package relay

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	// PreambleSize is the length of the framing a relay writes before any payload: the magic, the
	// token, and the port the service dialled.
	PreambleSize = magicSize + tokenSize + portSize
	// magic opens every preamble and is the verification listener's whole answer: "STU" and format
	// version 1.
	magic     = "STU\x01"
	magicSize = len(magic)
	tokenSize = 16
	portSize  = 2
	// preambleWait bounds how long a host listener waits for a preamble. A relay writes it in its first
	// write, one round trip after the dial, so anything still silent after this is not a relay.
	preambleWait = 2 * time.Second
)

// ErrForeign means a connection did not open with this check's preamble, so it came from something
// other than one of the check's relays.
var ErrForeign = errors.New("not a relay of this check")

// errNoHalfClose means a connection offers no way to close only its write side.
var errNoHalfClose = errors.New("the connection cannot close its write side alone")

// Token authenticates the relay hop: minted once per check, handed to each relay in its argv, and
// required at the start of every connection a host listener accepts. It has no String method on
// purpose — the token is never printed.
type Token [tokenSize]byte

// NewToken mints a token from the system's cryptographic source.
func NewToken() (Token, error) {
	var token Token

	if _, err := rand.Read(token[:]); err != nil {
		return Token{}, fmt.Errorf("mint the relay token: %w", err)
	}

	return token, nil
}

// WritePreamble frames a relay-to-host connection: the magic, the token and the destination port, in
// ONE write, so the host reads it within one round trip of the dial.
func WritePreamble(w io.Writer, token Token, port uint16) error {
	preamble := make([]byte, 0, PreambleSize)
	preamble = append(preamble, magic...)
	preamble = append(preamble, token[:]...)
	preamble = binary.BigEndian.AppendUint16(preamble, port)

	return writeAll(w, preamble, "the preamble")
}

// WriteAck answers a verification probe: the magic, and nothing else.
func WriteAck(w io.Writer) error {
	return writeAll(w, []byte(magic), "the verification answer")
}

// Accept reads and checks the preamble a relay opens a connection with, within its bound, and returns
// the connection with the preamble consumed and the deadline cleared.
//
// The token is compared in constant time. Any mismatch, short read or timeout wraps ErrForeign; the
// port is returned as sent, because which ports are acceptable is the listener's to decide.
func Accept(c net.Conn, token Token) (*Conn, error) {
	if err := c.SetReadDeadline(time.Now().Add(preambleWait)); err != nil {
		return nil, fmt.Errorf("%w: bound the preamble read: %w", ErrForeign, err)
	}

	preamble := make([]byte, PreambleSize)
	if _, err := io.ReadFull(c, preamble); err != nil {
		return nil, fmt.Errorf("%w: read the preamble: %w", ErrForeign, err)
	}

	if string(preamble[:magicSize]) != magic {
		return nil, fmt.Errorf("%w: the connection did not open with the preamble magic", ErrForeign)
	}

	if subtle.ConstantTimeCompare(preamble[magicSize:magicSize+tokenSize], token[:]) != 1 {
		return nil, fmt.Errorf("%w: the preamble carries another check's token", ErrForeign)
	}

	if err := c.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("%w: clear the preamble deadline: %w", ErrForeign, err)
	}

	return &Conn{Conn: c, port: binary.BigEndian.Uint16(preamble[magicSize+tokenSize:])}, nil
}

// Conn is a relayed connection whose preamble has been read and checked.
type Conn struct {
	net.Conn

	port uint16
}

// DestinationPort is the port the service dialled on the relay: the one place the original
// destination survives on the host side.
func (c *Conn) DestinationPort() uint16 {
	return c.port
}

// CloseWrite half-closes the connection. Embedding net.Conn hides the method the wrapped connection
// has, so it is forwarded here; a connection without one is an error rather than a full close.
func (c *Conn) CloseWrite() error {
	return closeWrite(c.Conn)
}

// Abort closes the connection so that its far end reads a reset, never EOF. It is how a reset on the
// host side crosses the relay as a reset.
func (c *Conn) Abort() error {
	return reset(c.Conn)
}

// closeWrite half-closes c when it can.
func closeWrite(c net.Conn) error {
	half, ok := c.(interface{ CloseWrite() error })
	if !ok {
		return fmt.Errorf("%w: %T", errNoHalfClose, c)
	}

	if err := half.CloseWrite(); err != nil {
		return fmt.Errorf("close the write side: %w", err)
	}

	return nil
}

// reset closes c abortively: Abort when the connection offers it, else a zero linger on a TCP
// connection so the close sends a reset, then the close itself.
func reset(c net.Conn) error {
	if aborter, ok := c.(interface{ Abort() error }); ok {
		if err := aborter.Abort(); err != nil {
			return fmt.Errorf("abort the connection: %w", err)
		}

		return nil
	}

	var lingerErr error

	if tcp, ok := c.(*net.TCPConn); ok {
		lingerErr = tcp.SetLinger(0)
	}

	return errors.Join(lingerErr, c.Close())
}

// writeAll writes b in one call, treating a short write as the failure it is.
func writeAll(w io.Writer, b []byte, what string) error {
	written, err := w.Write(b)
	if err != nil {
		return fmt.Errorf("write %s: %w", what, err)
	}

	if written != len(b) {
		return fmt.Errorf("write %s: %w", what, io.ErrShortWrite)
	}

	return nil
}
