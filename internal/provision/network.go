package provision

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
)

// networkTemplate reads a network as one snake_case JSON object.
const networkTemplate = `{"id":{{json .Id}},"name":{{json .Name}},"labels":{{json .Labels}},` +
	`"internal":{{json .Internal}},"ipv6":{{json .EnableIPv6}},` +
	`"subnets":[{{range $i, $c := .IPAM.Config}}{{if $i}},{{end}}{{json $c.Subnet}}{{end}}],` +
	`"gateways":[{{range $i, $c := .IPAM.Config}}{{if $i}},{{end}}{{json $c.Gateway}}{{end}}],` +
	`"members":[{{$first := true}}{{range $id, $c := .Containers}}{{if not $first}},{{end}}` +
	`{{json $id}}{{$first = false}}{{end}}]}`

// idTemplate lists IDs, one per line.
const idTemplate = `{{.ID}}`

// errReadBack means a created network differs from what was asked for.
var errReadBack = errors.New("the network read back differs from its request")

// networkReport is what networkTemplate prints.
type networkReport struct {
	Labels   map[string]string `json:"labels"`
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Subnets  []string          `json:"subnets"`
	Gateways []string          `json:"gateways"`
	Members  []string          `json:"members"`
	Internal bool              `json:"internal"`
	IPv6     bool              `json:"ipv6"`
}

// ipv4Subnets returns the report's IPv4 subnets; a subnet that does not parse is returned as the
// zero prefix, so it can never compare equal to a request.
func (r networkReport) ipv4Subnets() []netip.Prefix {
	var out []netip.Prefix

	for _, text := range r.Subnets {
		prefix, err := netip.ParsePrefix(text)
		if err != nil {
			out = append(out, netip.Prefix{})

			continue
		}

		if prefix.Addr().Is4() {
			out = append(out, prefix)
		}
	}

	return out
}

// NetworkState is a network as the engine reports it.
type NetworkState struct {
	// Subnet is its one IPv4 subnet.
	Subnet netip.Prefix
	// Gateway is its IPv4 gateway address.
	Gateway netip.Addr
	// Members are the IDs of the running containers attached to it.
	Members []string
	// Internal reports a network with no route out.
	Internal bool
	// IPv6 reports IPv6 enabled.
	IPv6 bool
}

// Occupied is one IPv4 prefix already in use: by a local interface or an engine network.
type Occupied struct {
	// By names the holder: `interface <name>` or `network <name>`.
	By string
	// Prefix is the prefix held, masked.
	Prefix netip.Prefix
}

// localAddr is one IPv4 address of a local interface, with its prefix length.
type localAddr struct {
	name   string
	prefix netip.Prefix
}

// localAddrs lists every IPv4 address of every local interface, loopback included.
func localAddrs() ([]localAddr, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list local interfaces: %w", err)
	}

	var out []localAddr

	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("list the addresses of %s: %w", iface.Name, err)
		}

		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}

			ip, ok := netip.AddrFromSlice(ipNet.IP.To4())
			if !ok {
				continue
			}

			ones, _ := ipNet.Mask.Size()
			out = append(out, localAddr{name: iface.Name, prefix: netip.PrefixFrom(ip, ones)})
		}
	}

	return out, nil
}

// Occupied returns every IPv4 prefix a new network must not intersect: each local interface's and
// each engine network's. It only reads.
func (e *Engine) Occupied(ctx context.Context) ([]Occupied, error) {
	locals, err := e.interfaces()
	if err != nil {
		return nil, err
	}

	networks, err := e.engineNetworks(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]Occupied, 0, len(locals)+len(networks))
	for _, local := range locals {
		out = append(out, Occupied{By: "interface " + local.name, Prefix: local.prefix.Masked()})
	}

	for _, network := range networks {
		for _, prefix := range network.ipv4Subnets() {
			out = append(out, Occupied{By: "network " + network.Name, Prefix: prefix})
		}
	}

	return out, nil
}

// engineNetworks reads every network the engine holds. One removed between the listing and its
// inspect is skipped.
func (e *Engine) engineNetworks(ctx context.Context) ([]networkReport, error) {
	res, err := e.run.call(ctx, request{verb: verbNetworkList, args: []arg{{val: idTemplate}}})
	if err != nil {
		return nil, err
	}

	var out []networkReport

	for id := range strings.FieldsSeq(string(res.out)) {
		var report networkReport

		found, err := e.read(ctx, request{verb: verbNetworkInspect, args: []arg{{val: networkTemplate}, {val: id}}},
			&report)
		if err != nil {
			return nil, err
		}

		if found {
			out = append(out, report)
		}
	}

	return out, nil
}

// InspectNetwork reads back a network the check created.
func (e *Engine) InspectNetwork(ctx context.Context, n *Network) (NetworkState, error) {
	if err := e.owns(n); err != nil {
		return NetworkState{}, err
	}

	var report networkReport

	found, err := e.read(ctx, request{verb: verbNetworkInspect, args: []arg{{val: networkTemplate}, {val: n.id}}},
		&report)
	if err != nil {
		return NetworkState{}, err
	}

	if !found {
		return NetworkState{}, fmt.Errorf("%w: network %s is gone", ErrEngine, n.name)
	}

	state := NetworkState{Members: report.Members, Internal: report.Internal, IPv6: report.IPv6}

	if subnets := report.ipv4Subnets(); len(subnets) > 0 {
		state.Subnet = subnets[0]
	}

	for _, text := range report.Gateways {
		if gateway, err := netip.ParseAddr(text); err == nil && gateway.Is4() {
			state.Gateway = gateway
		}
	}

	return state, nil
}

// admitSubnet refuses a subnet that is not a masked IPv4 prefix or that intersects a local
// interface's prefix, before anything is created.
func admitSubnet(subnet netip.Prefix, locals []localAddr) error {
	if !subnet.IsValid() || !subnet.Addr().Is4() || subnet != subnet.Masked() {
		return fmt.Errorf("%w: %s is not a masked IPv4 prefix", ErrSubnetOverlap, subnet)
	}

	for _, local := range locals {
		if local.prefix.Masked().Overlaps(subnet) {
			return fmt.Errorf("%w: %s intersects %s on interface %s", ErrSubnetOverlap, subnet, local.prefix,
				local.name)
		}
	}

	return nil
}

// checkReadBack compares a created network with its request: exactly one IPv4 subnet, the one
// asked for; internal as asked; no IPv6; and no intersection with a local interface — other than
// the bridge carrying its own gateway on a native engine — or with any other network.
func checkReadBack(
	report networkReport, subnet netip.Prefix, internal bool, locals []localAddr, others []networkReport,
) error {
	subnets := report.ipv4Subnets()

	switch {
	case len(subnets) != 1 || subnets[0] != subnet:
		return fmt.Errorf("%w: asked for %s, the engine holds %q", errReadBack, subnet, report.Subnets)
	case report.Internal != internal:
		return fmt.Errorf("%w: internal is %v, asked for %v", errReadBack, report.Internal, internal)
	case report.IPv6:
		return fmt.Errorf("%w: IPv6 is enabled", errReadBack)
	}

	for _, local := range locals {
		if local.prefix.Masked().Overlaps(subnet) && !carriesGateway(local, report.Gateways) {
			return fmt.Errorf("%w: %s intersects %s on interface %s", errReadBack, subnet, local.prefix, local.name)
		}
	}

	for _, other := range others {
		for _, prefix := range other.ipv4Subnets() {
			if other.ID != report.ID && prefix.Overlaps(subnet) {
				return fmt.Errorf("%w: %s intersects %s of network %s", errReadBack, subnet, prefix, other.Name)
			}
		}
	}

	return nil
}

// carriesGateway reports an interface whose address is one of the network's gateways: the bridge a
// native engine creates for the network itself.
func carriesGateway(local localAddr, gateways []string) bool {
	for _, text := range gateways {
		if gateway, err := netip.ParseAddr(text); err == nil && gateway == local.prefix.Addr() {
			return true
		}
	}

	return false
}
