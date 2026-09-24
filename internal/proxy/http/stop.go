package http

import (
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// StopClass names why the stub stopped a run on the service's egress.
type StopClass uint8

// The egress stops. Each is a connection whose traffic the stub cannot observe, so going on would
// report a handler whose calls went unseen as one that made none.
const (
	// StopTLSOnCleartext is a client that opened TLS on the cleartext entry.
	StopTLSOnCleartext StopClass = iota + 1
	// StopCleartextOnTLS is a client that sent cleartext to the TLS entry.
	StopCleartextOnTLS
	// StopHandshake is a TLS handshake that failed: the client rejected the certificate, gave up, or
	// sent something malformed.
	StopHandshake
	// StopALPN is a client that offered only protocols other than HTTP/1.1.
	StopALPN
	// StopNoRequest is a client that completed a handshake and closed without a request.
	StopNoRequest
	// StopNotHTTP is a first request that is not HTTP/1.x.
	StopNotHTTP
	// StopConnect is a CONNECT request: the tunnel it opens hides the real request.
	StopConnect
	// StopSilent is a connection on a port the stub does not serve that sent no request.
	StopSilent
	// StopSRV is an SRV lookup: the connection it leads to is one Stutter cannot stand in for.
	StopSRV
)

// The fixed parts of a stop's text. None is built from a peer's address, a port, a time or a byte of
// payload, because a stop is compared between runs.
const (
	detailTLSOnCleartext = "the client used TLS where only cleartext HTTP/1.1 is served"
	detailCleartextOnTLS = "the client sent cleartext where TLS is served"
	detailNoRequest      = "the client completed a TLS handshake and sent no request, and no " +
		"earlier connection to this host did either: it likely pins certificates or speaks a protocol the " +
		"stub does not serve"
	detailNotHTTP = "not parseable HTTP/1.1"
	detailPreface = "the HTTP/2 connection preface, not parseable HTTP/1.1"
	detailConnect = "a CONNECT tunnel hides the request inside it, which cannot be observed"
	detailSRV     = "an SRV lookup precedes a connection Stutter cannot observe"
	// smtpNote follows a silent stop on a mail port: the client is waiting for a greeting nothing sends.
	smtpNote = "; SMTP is server-first and Stutter has no SMTP stub"
	// detailUnusedAtTeardown is a catch-all connection that never sent a request, still open or closed
	// when the service went away.
	detailUnusedAtTeardown = "sent no request before the end of the run"
	// alertOp is the operation crypto/tls names a TLS alert the client sent.
	alertOp = "remote error"
)

// The ports a mail client dials.
const (
	portSMTP       uint16 = 25
	portSMTPS      uint16 = 465
	portSubmission uint16 = 587
	portSMTPAlt    uint16 = 2525
)

// smtpPort reports whether a port is one a mail client dials: relay, legacy SMTPS, submission, and the
// common alternative.
func smtpPort(port uint16) bool {
	switch port {
	case portSMTP, portSMTPS, portSubmission, portSMTPAlt:
		return true
	default:
		return false
	}
}

// String is the class's name as a stop renders it.
func (c StopClass) String() string {
	switch c {
	case StopTLSOnCleartext:
		return "tls-on-cleartext"
	case StopCleartextOnTLS:
		return "cleartext-on-tls"
	case StopHandshake:
		return "handshake"
	case StopALPN:
		return "alpn"
	case StopNoRequest:
		return "no-request"
	case StopNotHTTP:
		return "not-http"
	case StopConnect:
		return "connect"
	case StopSilent:
		return "silent"
	case StopSRV:
		return "srv"
	default:
		return "unknown"
	}
}

// detail is the class's text when a stop carries none of its own.
func (c StopClass) detail() string {
	switch c {
	case StopTLSOnCleartext:
		return detailTLSOnCleartext
	case StopCleartextOnTLS:
		return detailCleartextOnTLS
	case StopNoRequest:
		return detailNoRequest
	case StopNotHTTP:
		return detailNotHTTP
	case StopConnect:
		return detailConnect
	case StopSRV:
		return detailSRV
	case StopHandshake, StopALPN, StopSilent:
		// Every producer gives these a detail of their own, because each has several causes.
		return "the connection could not be observed"
	default:
		return "an unknown stop"
	}
}

// EgressStop is a connection the stub could not observe, which stops the run. Its text is the same
// whichever connection raised it, so a shrink reproduces it and two runs compare equal on it.
//
//nolint:errname // the published name that callers match with errors.As.
type EgressStop struct {
	// Name is the server name the client asked for, the CONNECT target or the looked-up name; empty
	// when there is none.
	Name string
	// Detail says what the client did, from a fixed vocabulary; empty uses the class's own text.
	Detail string
	// Class is why the run stopped.
	Class StopClass
	// Port is the port the service dialled: 80 or 443 for the stub's own entries, the original
	// destination on any other. Zero for a stop no connection raised.
	Port uint16
}

// Error renders the stop: its class, the port and name when it has them, and what the client did.
func (s *EgressStop) Error() string {
	var text strings.Builder

	text.WriteString("egress stop ")
	text.WriteString(s.Class.String())

	if s.Port != 0 {
		text.WriteString(" on port ")
		text.WriteString(strconv.Itoa(int(s.Port)))
	}

	if s.Name != "" {
		text.WriteString(" for ")
		text.WriteString(strconv.Quote(s.Name))
	}

	text.WriteString(": ")

	if s.Detail != "" {
		text.WriteString(s.Detail)
	} else {
		text.WriteString(s.Class.detail())
	}

	if s.Class == StopSilent && smtpPort(s.Port) {
		text.WriteString(smtpNote)
	}

	return text.String()
}

// handshakeDetail says why a handshake failed, in words that never carry the connection's addresses.
func handshakeDetail(serverName string, err error) string {
	reason := "malformed handshake"

	if alert, isNetwork := errors.AsType[*net.OpError](err); isNetwork && alert.Op == alertOp {
		// crypto/tls's fixed text for the alert the client sent.
		reason = "the client rejected the certificate (" + alert.Err.Error() + ")"
	} else if errors.Is(err, os.ErrDeadlineExceeded) {
		reason = "it did not complete within " + headerBound.String()
	} else if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) {
		reason = "the client closed the connection during it"
	}

	detail := "the TLS handshake failed: " + reason
	if serverName == "" {
		detail = "no server name; " + detail
	}

	return detail
}

// alpnDetail names the protocols a client offered when none of them was HTTP/1.1.
func alpnDetail(offered []string) string {
	return "the client offered [" + strings.Join(offered, " ") + "]; only http/1.1 is served"
}

// refusesHTTP1 reports whether a client offered application protocols without HTTP/1.1 among them.
func refusesHTTP1(offered []string) bool {
	return len(offered) > 0 && !slices.Contains(offered, "http/1.1")
}
