package topology

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"sync"

	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/relay"
	"github.com/Wintersta7e/stutter/internal/waits"
)

var (
	// errRelay means a relay did not come up as it should.
	errRelay = errors.New("relay not ready")
	// errMembers means a network's members are not the ones the topology placed there.
	errMembers = errors.New("network membership wrong")
	// errUpstreams means a per-start upstream map does not match what the listener set serves.
	errUpstreams = errors.New("upstream map wrong")
	// errNotVerified means relays were asked for before the host's address was verified.
	errNotVerified = errors.New("the host's address is not verified")
)

// stubSysctls are the stub relay's own: its outbound source ports held to the reserved range the
// catch-all leaves free, and those few ports reused quickly, since the stub closes first after a
// client's half-close.
func stubSysctls() map[string]string {
	return map[string]string{
		"net.ipv4.ip_local_port_range": fmt.Sprintf("%d %d", relay.ReservedFirst, relay.ReservedLast),
		"net.ipv4.tcp_tw_reuse":        "1",
	}
}

// plannedRelay is one relay before it exists: what it is called in errors, the compose service it
// stands for, its argv, and where it goes.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type plannedRelay struct {
	sysctls map[string]string
	name    string
	service string
	// aliases are its names on the network it binds; none on the other.
	aliases []string
	spec    relay.Spec
	// onDependency places it on the dependency network alone, as the seed bus relay is.
	onDependency bool
}

// plan lays out every per-check relay: one per relayed dependency and the bus relay, each named by the
// service's own names, and the stub relay, named by nothing. Each pipes to its key's listener at the
// verified address; the stub also carries the catch-all and the DNS responder.
func plan(
	layout Layout,
	networks Networks,
	advertise netip.Addr,
	port func(string) uint16,
	token relay.Token,
) []plannedRelay {
	upstream := func(key string) netip.AddrPort { return netip.AddrPortFrom(advertise, port(key)) }
	pipe := func(key string, listen uint16) relay.Listener {
		return relay.Listener{Upstream: upstream(key), Port: listen}
	}

	var planned []plannedRelay

	for _, dep := range layout.Deps {
		listeners := make([]relay.Listener, 0, len(dep.Ports))
		for _, listen := range dep.Ports {
			listeners = append(listeners, pipe(harness.EndpointKey(dep.Service, listen), listen))
		}

		planned = append(planned, plannedRelay{
			name: "dependency " + dep.Service, service: dep.Service, aliases: dep.Names,
			spec: relay.Spec{Bind: networks.ServiceSubnet, Listeners: listeners, Token: token},
		})
	}

	busListeners := make([]relay.Listener, 0, len(layout.BusPorts)+1)
	for _, listen := range layout.BusPorts {
		busListeners = append(busListeners, pipe(harness.KeyBus, listen))
	}

	busListeners = append(busListeners, pipe(harness.KeyBusMonitor, harness.MonitorPort))

	return append(planned,
		plannedRelay{
			name: "bus", service: layout.BusService, aliases: layout.BusNames,
			spec: relay.Spec{Bind: networks.ServiceSubnet, Listeners: busListeners, Token: token},
		},
		plannedRelay{
			name: "stub", sysctls: stubSysctls(),
			spec: relay.Spec{
				Bind: networks.ServiceSubnet,
				Listeners: []relay.Listener{
					pipe(harness.KeyHTTP, harness.HTTPPort),
					pipe(harness.KeyHTTPS, harness.HTTPSPort),
					{Upstream: upstream(harness.KeyCatchAll), Kind: relay.CatchAll},
				},
				Signal: upstream(harness.KeyDNSSignal),
				Token:  token,
			},
		},
	)
}

// seedPlan is the seed bus relay: on the dependency network alone, under the names jobs dial the bus
// by, piping every bus port and the monitoring port to the seed bus's listener.
func seedPlan(layout Layout, networks Networks, upstream netip.AddrPort, token relay.Token) plannedRelay {
	listeners := make([]relay.Listener, 0, len(layout.BusPorts)+1)
	for _, listen := range append(slices.Clone(layout.BusPorts), harness.MonitorPort) {
		listeners = append(listeners, relay.Listener{Upstream: upstream, Port: listen})
	}

	return plannedRelay{
		name: "seed-bus", service: layout.BusService, aliases: layout.SeedBusNames, onDependency: true,
		spec: relay.Spec{Bind: networks.DependencySubnet, Listeners: listeners, Token: token},
	}
}

// relayContainer is one relay that exists.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type relayContainer struct {
	container *provision.Container
	address   netip.Addr
	name      string
}

// Relays creates every per-check relay at once — each on both networks from its create, named only on
// the service network — and waits for each to be ready at the address the engine gave it. Then the
// service network must hold the relays and nothing else.
func (t *Topology) Relays(ctx context.Context) error {
	advertise := t.advertise
	if !advertise.IsValid() {
		return errNotVerified
	}

	port := func(key string) uint16 {
		listened, _ := t.set.Port(key)

		return listened
	}

	planned := plan(t.cfg.Layout, t.cfg.Networks, advertise, port, t.token)
	started := make([]*relayContainer, len(planned))
	failures := make([]error, len(planned))

	var wg sync.WaitGroup

	for index, each := range planned {
		wg.Go(func() { started[index], failures[index] = t.startRelay(ctx, each) })
	}

	wg.Wait()

	for _, running := range started {
		if running != nil {
			t.relays = append(t.relays, running)
		}
	}

	if err := errors.Join(failures...); err != nil {
		return err
	}

	t.stub = started[len(started)-1].address

	return t.checkMembers(ctx, t.relayIDs())
}

// startRelay creates, starts and waits for one relay, and checks the address it bound is the one the
// engine gave it on the network it serves.
func (t *Topology) startRelay(ctx context.Context, planned plannedRelay) (*relayContainer, error) {
	attach := []provision.NetworkAttach{
		{Network: t.cfg.Networks.Service, Aliases: planned.aliases},
		{Network: t.cfg.Networks.Dependency},
	}
	bound := t.cfg.Networks.Service

	if planned.onDependency {
		attach = []provision.NetworkAttach{{Network: t.cfg.Networks.Dependency, Aliases: planned.aliases}}
		bound = t.cfg.Networks.Dependency
	}

	container, err := t.cfg.Engine.CreateContainer(ctx, relayContainerSpec(t.cfg.Image, rules.KindRelay,
		planned.service, planned.spec.Args(), planned.sysctls, attach...))
	if err != nil {
		return nil, fmt.Errorf("relay %s: %w", planned.name, err)
	}

	running := &relayContainer{container: container, name: planned.name}

	if startErr := t.cfg.Engine.Start(ctx, container); startErr != nil {
		return running, fmt.Errorf("relay %s: %w", planned.name, startErr)
	}

	readyCtx, cancel := context.WithTimeout(ctx, waits.RelayReady)
	defer cancel()

	line, err := t.cfg.Engine.Output(readyCtx, container, relay.Ready)
	if err != nil {
		return running, fmt.Errorf("%w: relay %s within %s: %w", errRelay, planned.name, waits.RelayReady, err)
	}

	ready, err := netip.ParseAddr(strings.TrimSpace(strings.TrimPrefix(line, relay.Ready)))
	if err != nil {
		return running, fmt.Errorf("%w: relay %s printed %q", errRelay, planned.name, line)
	}

	inspected, err := t.cfg.Engine.Address(ctx, container, bound)
	if err != nil {
		return running, fmt.Errorf("relay %s: %w", planned.name, err)
	}

	if ready != inspected {
		return running, fmt.Errorf("%w: relay %s bound %s, but the engine gave it %s", errRelay, planned.name,
			ready, inspected)
	}

	running.address = ready

	return running, nil
}

// relayIDs are the per-check relays' container IDs.
func (t *Topology) relayIDs() []string {
	ids := make([]string, 0, len(t.relays))
	for _, running := range t.relays {
		ids = append(ids, running.container.ID())
	}

	return ids
}

// checkMembers requires the service network's running members to be exactly want.
func (t *Topology) checkMembers(ctx context.Context, want []string) error {
	state, err := t.cfg.Engine.InspectNetwork(ctx, t.cfg.Networks.Service)
	if err != nil {
		return fmt.Errorf("read the service network: %w", err)
	}

	found := slices.Sorted(slices.Values(state.Members))
	wanted := slices.Sorted(slices.Values(want))

	if !slices.Equal(found, wanted) {
		return fmt.Errorf("%w: the service network holds [%s], want [%s]", errMembers,
			strings.Join(found, " "), strings.Join(wanted, " "))
	}

	return nil
}

// RelayError is a relay found not running at a liveness check: the run that needed it cannot be
// judged. It names the relay, how it exited and the last thing it said.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type RelayError struct {
	Relay    string
	Stderr   string
	ExitCode int
	Restarts int
}

func (e *RelayError) Error() string {
	return fmt.Sprintf("relay %s is not running (exit code %d, restarts %d): %s", e.Relay, e.ExitCode, e.Restarts,
		e.Stderr)
}

// Live checks every relay is still running, never restarted, and that the service network holds the
// relays and nothing else — before a start places its target, and after it is gone.
func (t *Topology) Live(ctx context.Context) error {
	var err error

	for _, running := range t.relays {
		err = errors.Join(err, t.alive(ctx, running))
	}

	if err != nil {
		return err
	}

	return t.checkMembers(ctx, t.relayIDs())
}

// alive reads one relay's state, and names it when it has stopped or restarted.
func (t *Topology) alive(ctx context.Context, running *relayContainer) error {
	state, err := t.cfg.Engine.Status(ctx, running.container)
	if err != nil {
		return fmt.Errorf("read relay %s: %w", running.name, err)
	}

	if state.Running && state.RestartCount == 0 {
		return nil
	}

	stderr, stderrErr := t.cfg.Engine.LastStderrLine(ctx, running.container)
	if stderrErr != nil {
		stderr = "its stderr is unreadable: " + stderrErr.Error()
	}

	return &RelayError{Relay: running.name, Stderr: stderr, ExitCode: state.ExitCode, Restarts: state.RestartCount}
}

// Placement is everything a start needs from the topology to place its target, identical for every
// start of the check.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Placement struct {
	// DNS is the stub relay's address, the target's only resolver.
	DNS netip.Addr
	// Hostname is the target's hostname.
	Hostname string
	// Attach is the service network, with the target's names for itself.
	Attach provision.NetworkAttach
}

// Placement is the target's network, names, hostname and resolver. Before Relays, DNS is zero.
func (t *Topology) Placement() Placement {
	return Placement{
		Attach: provision.NetworkAttach{
			Network: t.cfg.Networks.Service,
			Aliases: slices.Clone(t.cfg.Layout.SelfAliases),
		},
		Hostname: t.cfg.Hostname,
		DNS:      t.stub,
	}
}

// UpstreamMap is one start's upstream source: every restored dependency's address, keyed as the set
// serves it, and the bus's two addresses. It must name every key the set serves, and nothing else.
func (t *Topology) UpstreamMap(
	restored map[string]netip.AddrPort,
	bus, monitor netip.AddrPort,
) (map[string]netip.AddrPort, error) {
	served := append(t.cfg.Layout.Keys(), harness.KeyBus, harness.KeyBusMonitor)
	upstreams := make(map[string]netip.AddrPort, len(served))

	for key, address := range restored {
		if !slices.Contains(t.cfg.Layout.Keys(), key) {
			return nil, fmt.Errorf("%w: %s is not an endpoint the relays carry", errUpstreams, key)
		}

		upstreams[key] = address
	}

	upstreams[harness.KeyBus], upstreams[harness.KeyBusMonitor] = bus, monitor

	for _, key := range served {
		if address, found := upstreams[key]; !found || !address.Addr().Is4() || address.Port() == 0 {
			return nil, fmt.Errorf("%w: %s has no IPv4 address and port (%q)", errUpstreams, key, address)
		}
	}

	return upstreams, nil
}

// CheckTarget requires a started target to be on the service network alone, beside the relays and
// nothing else. It runs after the start: a created container is not yet a member of any network.
func (t *Topology) CheckTarget(ctx context.Context, target *provision.Container) error {
	inspected, err := t.cfg.Engine.Inspect(ctx, target)
	if err != nil {
		return fmt.Errorf("read the target: %w", err)
	}

	if want := []string{t.cfg.Networks.Service.Name()}; !slices.Equal(inspected.Networks, want) {
		return fmt.Errorf("%w: the target is on [%s], want [%s]", errMembers, strings.Join(inspected.Networks, " "),
			strings.Join(want, " "))
	}

	return t.checkMembers(ctx, append(t.relayIDs(), target.ID()))
}

// SeedBus opens the seed bus for the seed phase: its listener, then its relay on the dependency
// network, where jobs reach the bus by its names without a byte being recorded.
func (t *Topology) SeedBus(ctx context.Context, client, monitor netip.AddrPort) error {
	port, err := t.set.OpenSeedBus(ctx, client, monitor)
	if err != nil {
		return fmt.Errorf("open the seed bus: %w", err)
	}

	planned := seedPlan(t.cfg.Layout, t.cfg.Networks, netip.AddrPortFrom(t.advertise, port), t.token)

	running, err := t.startRelay(ctx, planned)

	t.mu.Lock()
	t.seedRelay = running
	t.mu.Unlock()

	return err
}

// EndSeedBus ends the seed phase: the seed bus relay goes first, then its listener, whose first failed
// dial is returned naming the bus.
func (t *Topology) EndSeedBus(ctx context.Context) error {
	t.mu.Lock()
	running := t.seedRelay
	t.seedRelay = nil
	t.mu.Unlock()

	var err error

	if running != nil {
		err = t.remove(ctx, running.container)
	}

	return errors.Join(err, t.set.CloseSeedBus())
}

// Close removes every relay and the verifier, then closes the listener set — in that order, because a
// listener's close waits for its connections, which only the relays' going ends.
func (t *Topology) Close(ctx context.Context) error {
	var containers []*provision.Container

	t.mu.Lock()

	for _, running := range t.relays {
		containers = append(containers, running.container)
	}

	if t.seedRelay != nil {
		containers = append(containers, t.seedRelay.container)
	}

	if t.verifier != nil {
		containers = append(containers, t.verifier)
	}

	t.relays, t.seedRelay, t.verifier = nil, nil, nil
	t.mu.Unlock()

	var err error

	for _, container := range containers {
		err = errors.Join(err, t.remove(ctx, container))
	}

	return errors.Join(err, t.set.Close(ctx))
}

// remove removes one of the topology's containers.
func (t *Topology) remove(ctx context.Context, container *provision.Container) error {
	if err := t.cfg.Engine.Remove(ctx, container); err != nil {
		return fmt.Errorf("remove %s: %w", container.Name(), err)
	}

	return nil
}
