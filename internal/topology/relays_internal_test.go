package topology

import (
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/relay"
)

// The bus's service and the unparsed dependency's endpoint key in these tests.
const (
	busService = "nats"
	cacheKey   = "cache:6379"
	dbKey      = "db:5432"
)

var (
	serviceSubnet    = netip.MustParsePrefix("172.16.0.0/24")
	dependencySubnet = netip.MustParsePrefix("172.16.1.0/24")
	advertised       = netip.MustParseAddr("192.168.65.254")
)

func twoDependencies() Layout {
	return Layout{
		Deps: []Dependency{
			{Service: "cache", Names: []string{"cache"}, Ports: []uint16{6379}},
			{Service: "db", Names: []string{"db", "postgres"}, Ports: []uint16{5432, 5433}},
		},
		BusService:   busService,
		BusNames:     []string{busService, "queue"},
		SeedBusNames: []string{busService},
		SelfAliases:  []string{"orders"},
		BusPorts:     []uint16{4222},
	}
}

// listenerPorts gives every key a distinct port, so a relay piped to the wrong key is caught.
func listenerPorts(key string) uint16 {
	ports := map[string]uint16{
		cacheKey: 40001, dbKey: 40002, "db:5433": 40003, harness.KeyBus: 40004,
		harness.KeyBusMonitor: 40005, harness.KeyHTTP: 40006, harness.KeyHTTPS: 40007,
		harness.KeyCatchAll: 40008, harness.KeyDNSSignal: 40009,
	}

	return ports[key]
}

func upstreamOn(port uint16) netip.AddrPort {
	return netip.AddrPortFrom(advertised, port)
}

// TestRelaySpecsFollowTheLayout holds every relay to its one job: each pipes the ports it listens on
// to the listener of the key those ports belong to, binds the service subnet, and only the stub carries
// the catch-all and the DNS responder.
func TestRelaySpecsFollowTheLayout(t *testing.T) {
	t.Parallel()

	networks := Networks{ServiceSubnet: serviceSubnet, DependencySubnet: dependencySubnet}
	planned := plan(twoDependencies(), networks, advertised, listenerPorts, relay.Token{1})

	want := map[string][]relay.Listener{
		"dependency cache": {{Upstream: upstreamOn(40001), Port: 6379}},
		"dependency db":    {{Upstream: upstreamOn(40002), Port: 5432}, {Upstream: upstreamOn(40003), Port: 5433}},
		"bus":              {{Upstream: upstreamOn(40004), Port: 4222}, {Upstream: upstreamOn(40005), Port: 8222}},
		"stub": {
			{Upstream: upstreamOn(40006), Port: 80},
			{Upstream: upstreamOn(40007), Port: 443},
			{Upstream: upstreamOn(40008), Kind: relay.CatchAll},
		},
	}

	if len(planned) != len(want) {
		t.Fatalf("%d relays planned, want %d", len(planned), len(want))
	}

	for _, each := range planned {
		if !slices.Equal(each.spec.Listeners, want[each.name]) {
			t.Errorf("%s: listeners %v, want %v", each.name, each.spec.Listeners, want[each.name])
		}

		if each.spec.Bind != serviceSubnet || each.onDependency {
			t.Errorf("%s: binds %s (dependency only %v), want the service subnet", each.name, each.spec.Bind,
				each.onDependency)
		}

		isStub := each.name == "stub"
		if isStub != each.spec.Signal.IsValid() || (isStub != (len(each.sysctls) > 0)) {
			t.Errorf("%s: signal %v, sysctls %v; only the stub carries either", each.name, each.spec.Signal,
				each.sysctls)
		}
	}

	stub := planned[len(planned)-1]
	if stub.spec.Signal != upstreamOn(40009) || len(stub.aliases) != 0 {
		t.Errorf("stub: signal %s, aliases %v; want %s and no alias", stub.spec.Signal, stub.aliases, upstreamOn(40009))
	}

	if got := stub.sysctls["net.ipv4.ip_local_port_range"]; got != "64512 65535" {
		t.Errorf("stub source-port range = %q, want %q", got, "64512 65535")
	}

	seed := seedPlan(twoDependencies(), networks, upstreamOn(40010), relay.Token{1})
	if seed.spec.Bind != dependencySubnet || !seed.onDependency || !slices.Equal(seed.aliases, []string{busService}) {
		t.Errorf("seed bus relay: binds %s, dependency only %v, aliases %v", seed.spec.Bind, seed.onDependency,
			seed.aliases)
	}

	if want := []relay.Listener{
		{Upstream: upstreamOn(40010), Port: 4222},
		{Upstream: upstreamOn(40010), Port: 8222},
	}; !slices.Equal(
		seed.spec.Listeners,
		want,
	) {
		t.Errorf("seed bus relay listeners = %v, want %v", seed.spec.Listeners, want)
	}
}

func testTopology() *Topology {
	return &Topology{cfg: Config{
		Layout:   twoDependencies(),
		Hostname: "orders-under-test",
		Networks: Networks{Service: &provision.Network{}},
	}, stub: netip.MustParseAddr("172.16.0.4")}
}

// TestTheUpstreamMapNamesEveryServedKey refuses a start whose addresses do not cover exactly what the
// relays carry: a missing one leaves a relayed port with nowhere to go.
func TestTheUpstreamMapNamesEveryServedKey(t *testing.T) {
	t.Parallel()

	topo := testTopology()
	complete := func() map[string]netip.AddrPort {
		return map[string]netip.AddrPort{
			cacheKey:  netip.MustParseAddrPort("127.0.0.1:50001"),
			dbKey:     netip.MustParseAddrPort("127.0.0.1:50002"),
			"db:5433": netip.MustParseAddrPort("127.0.0.1:50003"),
		}
	}

	bus, monitor := netip.MustParseAddrPort("127.0.0.1:4222"), netip.MustParseAddrPort("127.0.0.1:8222")

	upstreams, err := topo.UpstreamMap(complete(), bus, monitor)
	if err != nil {
		t.Fatalf("UpstreamMap() error = %v", err)
	}

	if upstreams[harness.KeyBus] != bus || upstreams[harness.KeyBusMonitor] != monitor || len(upstreams) != 5 {
		t.Errorf("UpstreamMap() = %v, want the three dependencies, the bus and its monitor", upstreams)
	}

	missing, extra, ipv6 := complete(), complete(), complete()
	delete(missing, dbKey)

	extra["x:1"] = netip.MustParseAddrPort("127.0.0.1:1")
	ipv6[cacheKey] = netip.MustParseAddrPort("[::1]:6379")

	wrong := map[string]map[string]netip.AddrPort{dbKey: missing, "x:1": extra, cacheKey: ipv6}

	for key, restored := range wrong {
		if _, err := topo.UpstreamMap(restored, bus, monitor); err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("a map wrong at %s: err = %v, want an error naming it", key, err)
		}
	}
}

// TestPlacementIsTheSameForEveryStart gives every start one target shape: the same network, names,
// hostname and resolver.
func TestPlacementIsTheSameForEveryStart(t *testing.T) {
	t.Parallel()

	topo := testTopology()
	first, second := topo.Placement(), topo.Placement()

	if first.Hostname != second.Hostname || first.DNS != second.DNS || first.Attach.Network != second.Attach.Network ||
		!slices.Equal(first.Attach.Aliases, second.Attach.Aliases) {
		t.Errorf("two placements differ: %+v then %+v", first, second)
	}

	if !slices.Equal(first.Attach.Aliases, []string{"orders"}) || first.Hostname != "orders-under-test" ||
		first.DNS != netip.MustParseAddr("172.16.0.4") {
		t.Errorf("Placement() = %+v, want the self-aliases, the hostname and the stub's address", first)
	}
}
