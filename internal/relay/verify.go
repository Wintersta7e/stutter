package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"syscall"
)

var (
	// errResolve means the host's name did not resolve to exactly one IPv4 address.
	errResolve = errors.New("cannot choose the host's address")
	// errWrongAck means something answered the probe, but not the verification listener.
	errWrongAck = errors.New("wrong ack")
)

// lookupIPv4 resolves a name to its IPv4 addresses through the container's own resolver.
func lookupIPv4(ctx context.Context, host string) ([]netip.Addr, error) {
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}

	return addrs, nil
}

// verify runs the verifier: one probe to the host's verification listener, bounded as a whole by
// v.Dial — the lookup, the dial, the probe and the answer — so a host that never answers is reported
// as the verification failing, inside the provisioner's own wait.
func (s system) verify(ctx context.Context, v Verify, stdout, stderr io.Writer) int {
	ctx, cancel := context.WithTimeout(ctx, v.Dial)
	defer cancel()

	target, err := s.resolveTarget(ctx, v.Target)
	if err != nil {
		return report(stderr, exitFailure, err)
	}

	address := netip.AddrPortFrom(target, v.Port)

	if err := probe(ctx, address, v.Token); err != nil {
		return report(stderr, exitFailure, fmt.Errorf("verify %s: %w", address, err))
	}

	if _, err := fmt.Fprintf(stdout, "%s %s\n", Verified, target); err != nil {
		return report(stderr, exitFailure, fmt.Errorf("print the verified line: %w", err))
	}

	return 0
}

// resolveTarget is a literal target as it stands, or the host alias's one IPv4 address. Several would
// be a guess, and a guessed address is the one failure verification exists to prevent.
func (s system) resolveTarget(ctx context.Context, target string) (netip.Addr, error) {
	if target != hostAlias {
		addr, err := netip.ParseAddr(target)
		if err != nil {
			return netip.Addr{}, fmt.Errorf("%w: %w", errTarget, err)
		}

		return addr, nil
	}

	answers, err := s.lookup(ctx, target)
	if err != nil {
		return netip.Addr{}, err
	}

	if len(answers) != 1 {
		named := make([]string, 0, len(answers))
		for _, answer := range answers {
			named = append(named, answer.String())
		}

		return netip.Addr{}, fmt.Errorf("%w: %s resolved to %d addresses [%s], want exactly 1",
			errResolve, target, len(answers), strings.Join(named, " "))
	}

	return answers[0].Unmap(), nil
}

// probe dials the verification listener, sends a probe and requires the magic back, all before ctx's
// deadline.
func probe(ctx context.Context, address netip.AddrPort, token Token) error {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp4", address.String())
	if err != nil {
		return describe(err)
	}

	defer func() { _ = conn.Close() }()

	if deadline, bounded := ctx.Deadline(); bounded {
		if err := conn.SetDeadline(deadline); err != nil {
			return describe(err)
		}
	}

	if err := WritePreamble(conn, token, 0); err != nil {
		return describe(err)
	}

	ack := make([]byte, magicSize)
	if _, err := io.ReadFull(conn, ack); err != nil {
		return describe(err)
	}

	if string(ack) != magic {
		return fmt.Errorf("%w: % x", errWrongAck, ack)
	}

	return nil
}

// describe names a probe failure by its likely cause.
func describe(err error) error {
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("timed out: %w", err)
	case errors.Is(err, syscall.ECONNREFUSED):
		return fmt.Errorf("refused: %w", err)
	default:
		return fmt.Errorf("probe: %w", err)
	}
}
