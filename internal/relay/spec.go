package relay

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"path"
	"strconv"
	"strings"
	"time"
)

// DNSPort is the port the stub relay answers DNS on, and the preamble port of its signal connection.
const DNSPort uint16 = 53

// The reserved range: the stub relay's own outbound source ports, which it never listens on. A service
// connection to one of them meets the kernel's reset instead of the catch-all, so the range is kept
// small and stated once.
const (
	ReservedFirst uint16 = 64512
	ReservedLast  uint16 = 65535
)

const (
	modeServe  = "serve"
	modeVerify = "verify"
	modeCopy   = "copy"
	// hostAlias is the one name a verify target may be: the engine's name for the host.
	hostAlias = "host.docker.internal"
	// maxQueryName bounds a signal record's name to what its one-byte length can say.
	maxQueryName = 255
	// queryHeader is a signal record's type and length.
	queryHeader = 3
)

var (
	errArgv      = errors.New("invalid relay arguments")
	errUpstream  = errors.New("an upstream must be an IPv4 literal and a port")
	errPort      = errors.New("a port must be 1 to 65535")
	errToken     = errors.New("a token must be 32 lower-case hex digits")
	errPrefix    = errors.New("a bind prefix must be a masked IPv4 prefix")
	errTarget    = errors.New("a verify target must be an IPv4 literal or " + hostAlias)
	errDial      = errors.New("a dial bound must be positive")
	errPath      = errors.New("a copy path must be absolute")
	errQueryName = errors.New("a signal record's name is longer than 255 bytes")
)

// Kind is how a relay listener serves its connections.
type Kind uint8

const (
	// Pipe serves one port and pipes it to one upstream.
	Pipe Kind = iota
	// CatchAll serves every port no other listener or reservation holds, and pipes each connection
	// to one upstream with the port it arrived on in its preamble.
	CatchAll
)

// Listener is one relay listener. Port is zero for a catch-all.
type Listener struct {
	Upstream netip.AddrPort
	Port     uint16
	Kind     Kind
}

// Spec is everything one serving relay is told, fixed for the container's life.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Spec struct {
	// Bind is the prefix the relay's own address lies in. The engine assigns the address at start, so
	// the relay finds it rather than being told it.
	Bind netip.Prefix
	// Signal is where the DNS responder reports queries. Valid exactly when the relay answers DNS.
	Signal netip.AddrPort
	// Listeners are the relay's listeners, in the order they were given.
	Listeners []Listener
	Token     Token
}

// Verify is everything a verifier is told.
type Verify struct {
	// Target is an IPv4 literal, or host.docker.internal.
	Target string
	// Dial bounds the dial, the probe and the answer together.
	Dial  time.Duration
	Token Token
	// Port is the host's verification listener.
	Port uint16
}

// Copy is everything a copy helper is told.
type Copy struct {
	Src string
	Dst string
}

// Query is one DNS question the stub relay answered with something other than an address.
type Query struct {
	Name string
	Type uint16
}

// Args is the relay's argv after the entrypoint: the one place a serve argv is written.
func (s Spec) Args() []string {
	args := []string{modeServe, "--token", encodeToken(s.Token), "--bind", s.Bind.String()}

	for _, listener := range s.Listeners {
		if listener.Kind == CatchAll {
			args = append(args, "--catch-all", listener.Upstream.String())

			continue
		}

		args = append(args, "--pipe", strconv.FormatUint(uint64(listener.Port), 10)+"="+listener.Upstream.String())
	}

	if s.Signal.IsValid() {
		args = append(args, "--dns", s.Signal.String())
	}

	return args
}

// Args is the verifier's argv after the entrypoint.
func (v Verify) Args() []string {
	return []string{
		modeVerify, "--token", encodeToken(v.Token), "--target", v.Target,
		"--port", strconv.FormatUint(uint64(v.Port), 10), "--dial", v.Dial.String(),
	}
}

// Args is the copy helper's argv after the entrypoint.
func (c Copy) Args() []string {
	return []string{modeCopy, "--src", c.Src, "--dst", c.Dst}
}

// Parse reads a serve argv, mode word first. Every upstream must be an IPv4 literal: the relay
// resolves nothing.
func Parse(args []string) (Spec, error) {
	var spec Spec

	set, err := flagsFor(args, modeServe)
	if err != nil {
		return Spec{}, err
	}

	set.Func("token", "", func(v string) error { return decodeToken(v, &spec.Token) })
	set.Func("bind", "", func(v string) error { return parseBind(v, &spec.Bind) })
	set.Func("dns", "", func(v string) error { return parseUpstream(v, &spec.Signal) })
	set.Func("pipe", "", func(v string) error {
		listener, pipeErr := parsePipe(v)
		if pipeErr != nil {
			return pipeErr
		}

		spec.Listeners = append(spec.Listeners, listener)

		return nil
	})
	set.Func("catch-all", "", func(v string) error {
		listener := Listener{Kind: CatchAll}
		if upstreamErr := parseUpstream(v, &listener.Upstream); upstreamErr != nil {
			return upstreamErr
		}

		spec.Listeners = append(spec.Listeners, listener)

		return nil
	})

	if err := finish(set, args, "token", "bind"); err != nil {
		return Spec{}, err
	}

	if err := checkListeners(spec); err != nil {
		return Spec{}, err
	}

	return spec, nil
}

// ParseVerify reads a verify argv, mode word first.
func ParseVerify(args []string) (Verify, error) {
	var verify Verify

	set, err := flagsFor(args, modeVerify)
	if err != nil {
		return Verify{}, err
	}

	set.Func("token", "", func(v string) error { return decodeToken(v, &verify.Token) })
	set.Func("target", "", func(v string) error { return parseTarget(v, &verify.Target) })
	set.Func("port", "", func(v string) error { return parsePort(v, &verify.Port) })
	set.Func("dial", "", func(v string) error { return parseDial(v, &verify.Dial) })

	if err := finish(set, args, "token", "target", "port", "dial"); err != nil {
		return Verify{}, err
	}

	return verify, nil
}

// ParseCopy reads a copy argv, mode word first.
func ParseCopy(args []string) (Copy, error) {
	var copied Copy

	set, err := flagsFor(args, modeCopy)
	if err != nil {
		return Copy{}, err
	}

	set.Func("src", "", func(v string) error { return parsePath(v, &copied.Src) })
	set.Func("dst", "", func(v string) error { return parsePath(v, &copied.Dst) })

	if err := finish(set, args, "src", "dst"); err != nil {
		return Copy{}, err
	}

	return copied, nil
}

// WriteQuery writes one signal record: the query type, the name's length, the name.
func WriteQuery(w io.Writer, q Query) error {
	if len(q.Name) > maxQueryName {
		return fmt.Errorf("%w: %d bytes", errQueryName, len(q.Name))
	}

	record := make([]byte, 0, queryHeader+len(q.Name))
	record = binary.BigEndian.AppendUint16(record, q.Type)
	record = append(record, byte(len(q.Name))) //nolint:gosec // bounded by maxQueryName above.
	record = append(record, q.Name...)

	return writeAll(w, record, "a signal record")
}

// ReadQuery reads one signal record. It returns io.EOF only when the stream ends on a record boundary;
// a record cut short is io.ErrUnexpectedEOF.
func ReadQuery(r io.Reader) (Query, error) {
	header := make([]byte, queryHeader)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.EOF) {
			return Query{}, io.EOF
		}

		return Query{}, fmt.Errorf("read a signal record: %w", err)
	}

	name := make([]byte, header[2])
	if _, err := io.ReadFull(r, name); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}

		return Query{}, fmt.Errorf("read a signal record's name: %w", err)
	}

	return Query{Name: string(name), Type: binary.BigEndian.Uint16(header)}, nil
}

// flagsFor checks the mode word and returns a flag set for the flags after it.
func flagsFor(args []string, mode string) (*flag.FlagSet, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("%w: no mode, want %q", errArgv, mode)
	}

	if args[0] != mode {
		return nil, fmt.Errorf("%w: unknown relay mode %q, want %q", errArgv, args[0], mode)
	}

	set := flag.NewFlagSet(mode, flag.ContinueOnError)
	set.SetOutput(io.Discard)

	return set, nil
}

// finish parses the flags after the mode word and requires the named ones.
func finish(set *flag.FlagSet, args []string, required ...string) error {
	if err := set.Parse(args[1:]); err != nil {
		return fmt.Errorf("%w: %w", errArgv, err)
	}

	if set.NArg() > 0 {
		return fmt.Errorf("%w: unexpected argument %q", errArgv, set.Arg(0))
	}

	seen := make(map[string]bool, len(required))

	set.Visit(func(given *flag.Flag) { seen[given.Name] = true })

	for _, name := range required {
		if !seen[name] {
			return fmt.Errorf("%w: --%s is required", errArgv, name)
		}
	}

	return nil
}

// checkListeners refuses listeners that cannot all bind: a port twice, a pipe on the DNS port beside
// the responder, or more than one catch-all.
func checkListeners(spec Spec) error {
	ports := make(map[uint16]bool, len(spec.Listeners))
	catchAlls := 0

	for _, listener := range spec.Listeners {
		if listener.Kind == CatchAll {
			catchAlls++

			continue
		}

		if ports[listener.Port] {
			return fmt.Errorf("%w: --pipe %d is given twice", errArgv, listener.Port)
		}

		ports[listener.Port] = true
	}

	if catchAlls > 1 {
		return fmt.Errorf("%w: --catch-all is given %d times, at most once", errArgv, catchAlls)
	}

	if spec.Signal.IsValid() && ports[DNSPort] {
		return fmt.Errorf("%w: --pipe %d collides with the DNS responder --dns", errArgv, DNSPort)
	}

	return nil
}

func encodeToken(token Token) string {
	return hex.EncodeToString(token[:])
}

func decodeToken(v string, into *Token) error {
	decoded, err := hex.DecodeString(v)
	if err != nil || len(decoded) != tokenSize || encodeToken(Token(decoded)) != v {
		return errToken
	}

	*into = Token(decoded)

	return nil
}

func parseBind(v string, into *netip.Prefix) error {
	prefix, err := netip.ParsePrefix(v)
	if err != nil || !prefix.Addr().Is4() || prefix.Masked() != prefix {
		return fmt.Errorf("%w: %q", errPrefix, v)
	}

	*into = prefix

	return nil
}

func parseUpstream(v string, into *netip.AddrPort) error {
	upstream, err := netip.ParseAddrPort(v)
	if err != nil || !upstream.Addr().Is4() {
		return fmt.Errorf("%w: %q", errUpstream, v)
	}

	if upstream.Port() == 0 {
		return fmt.Errorf("%w: %q", errPort, v)
	}

	*into = upstream

	return nil
}

func parsePipe(v string) (Listener, error) {
	port, upstream, found := strings.Cut(v, "=")
	if !found {
		return Listener{}, fmt.Errorf("%w: %q is not <port>=<upstream>", errArgv, v)
	}

	var listener Listener

	if err := parsePort(port, &listener.Port); err != nil {
		return Listener{}, err
	}

	if err := parseUpstream(upstream, &listener.Upstream); err != nil {
		return Listener{}, err
	}

	return listener, nil
}

func parsePort(v string, into *uint16) error {
	port, err := strconv.ParseUint(v, 10, 16)
	if err != nil || port == 0 {
		return fmt.Errorf("%w: %q", errPort, v)
	}

	*into = uint16(port)

	return nil
}

func parseTarget(v string, into *string) error {
	if v != hostAlias {
		addr, err := netip.ParseAddr(v)
		if err != nil || !addr.Is4() {
			return fmt.Errorf("%w: %q", errTarget, v)
		}
	}

	*into = v

	return nil
}

func parseDial(v string, into *time.Duration) error {
	bound, err := time.ParseDuration(v)
	if err != nil || bound <= 0 {
		return fmt.Errorf("%w: %q", errDial, v)
	}

	*into = bound

	return nil
}

func parsePath(v string, into *string) error {
	if !path.IsAbs(v) {
		return fmt.Errorf("%w: %q", errPath, v)
	}

	*into = v

	return nil
}
