package provision

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
)

// Every Stutter network's IPAM: a /24 inside 172.16.0.0/12, the first one ascending that no local
// interface and no engine network holds. The engine cannot see the host's interfaces, so it would
// happily hand out a subnet that shadows one; Stutter picks the subnet itself and names it on create.
const (
	// subnetBits is every Stutter network's prefix length.
	subnetBits = 24
	// subnetAttempts bounds the picks one network may take: each attempt loses a race to another check
	// or a user's own network created in between, which is rare enough that eight in a row is a fault.
	subnetAttempts = 8
	// subnetRange is where every pick comes from.
	subnetRange = "172.16.0.0/12"
)

// errNoFreeSubnet means every candidate subnet is held by a local interface or an engine network.
var errNoFreeSubnet = errors.New("no free subnet")

// CreateFreeNetwork creates a network for role on a subnet nothing on this host or engine holds: the
// first free /24 of 172.16.0.0/12, re-picked when another network takes it between the read and the
// create, a bounded number of times.
func (e *Engine) CreateFreeNetwork(ctx context.Context, role string, internal bool) (*Network, error) {
	return createWithSubnet(ctx, e.Occupied, func(ctx context.Context, subnet netip.Prefix) (*Network, error) {
		return e.CreateNetwork(ctx, role, internal, subnet)
	})
}

// createWithSubnet picks a subnet from what occupied reports and creates on it. A create the engine
// refused because the subnet was taken meanwhile moves on to the next pick; any other failure is
// final. After subnetAttempts picks, the failure names every subnet tried.
func createWithSubnet(
	ctx context.Context,
	occupied func(context.Context) ([]Occupied, error),
	create func(context.Context, netip.Prefix) (*Network, error),
) (*Network, error) {
	tried := make([]netip.Prefix, 0, subnetAttempts)

	for range subnetAttempts {
		held, err := occupied(ctx)
		if err != nil {
			return nil, err
		}

		pick, err := pickSubnet(held, tried)
		if err != nil {
			return nil, err
		}

		tried = append(tried, pick)

		created, err := create(ctx, pick)
		if err == nil {
			return created, nil
		}

		if !errors.Is(err, ErrSubnetTaken) && !errors.Is(err, ErrSubnetOverlap) {
			return nil, err
		}
	}

	names := make([]string, 0, len(tried))
	for _, subnet := range tried {
		names = append(names, subnet.String())
	}

	return nil, fmt.Errorf("%w: each of %d subnets was taken as it was created: %s",
		ErrSubnetTaken, len(tried), strings.Join(names, ", "))
}

// pickSubnet is the first /24 of the candidate range that overlaps nothing held and was not tried.
func pickSubnet(held []Occupied, tried []netip.Prefix) (netip.Prefix, error) {
	const ipv4Bits = 32

	candidates := netip.MustParsePrefix(subnetRange)
	step := uint32(1) << (ipv4Bits - subnetBits)

	for at := candidates.Addr(); candidates.Contains(at); at = advance(at, step) {
		pick := netip.PrefixFrom(at, subnetBits)
		if !overlapsAny(pick, held, tried) {
			return pick, nil
		}
	}

	return netip.Prefix{}, fmt.Errorf("%w: every /%d of %s overlaps an interface or a network",
		errNoFreeSubnet, subnetBits, subnetRange)
}

func overlapsAny(pick netip.Prefix, held []Occupied, tried []netip.Prefix) bool {
	for _, occupied := range held {
		if occupied.Prefix.Overlaps(pick) {
			return true
		}
	}

	return slices.Contains(tried, pick)
}

// advance moves an IPv4 address on by step.
func advance(addr netip.Addr, step uint32) netip.Addr {
	octets := addr.As4()
	binary.BigEndian.PutUint32(octets[:], binary.BigEndian.Uint32(octets[:])+step)

	return netip.AddrFrom4(octets)
}
