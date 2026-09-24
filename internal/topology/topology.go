package topology

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/relay"
	"github.com/Wintersta7e/stutter/internal/waits"
)

// The two networks' role words.
const (
	roleService    = "service"
	roleDependency = "dependency"
	// hostAlias is the engine's name for the host, which the verifier resolves in host-alias mode.
	hostAlias = "host.docker.internal"
)

var (
	// errConfig means the topology was asked for with something missing or inconsistent.
	errConfig = errors.New("invalid topology configuration")
	// errNetwork means a network came back from the engine other than it was asked for.
	errNetwork = errors.New("network read back wrong")
)

// Image makes the check's relay image from the running executable: one layer holding the binary, and
// the relay command as its entrypoint. It is imported, never built, and every relay container is
// created from the ID it returns.
func Image(ctx context.Context, eng *provision.Engine, executable string) (compose.Image, error) {
	layer, err := relay.Layer(executable)
	if err != nil {
		return compose.Image{}, fmt.Errorf("the relay image: %w", err)
	}

	defer func() { _ = layer.Close() }()

	image, err := eng.Import(ctx, rules.KindRelay, layer, relay.ImageChanges())
	if err != nil {
		return compose.Image{}, fmt.Errorf("the relay image: %w", err)
	}

	return image, nil
}

// Networks are the check's two networks as the engine reports them.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Networks struct {
	// Service is the internal network the service under test and the relays share.
	Service *provision.Network
	// Dependency is where the real dependencies run, and every relay's second interface.
	Dependency *provision.Network
	// Gateway is the dependency network's gateway.
	Gateway netip.Addr
	// ServiceSubnet and DependencySubnet are each network's subnet.
	ServiceSubnet    netip.Prefix
	DependencySubnet netip.Prefix
}

// CreateNetworks creates the dependency network, then the internal service network, each on a subnet
// nothing on the host or the engine holds, and reads both back.
func CreateNetworks(ctx context.Context, eng *provision.Engine) (Networks, error) {
	dependency, dependencyState, err := createNetwork(ctx, eng, roleDependency, false)
	if err != nil {
		return Networks{}, err
	}

	service, serviceState, err := createNetwork(ctx, eng, roleService, true)
	if err != nil {
		return Networks{}, err
	}

	return Networks{
		ServiceSubnet:    serviceState.Subnet,
		DependencySubnet: dependencyState.Subnet,
		Gateway:          dependencyState.Gateway,
		Service:          service,
		Dependency:       dependency,
	}, nil
}

func createNetwork(
	ctx context.Context,
	eng *provision.Engine,
	role string,
	internal bool,
) (*provision.Network, provision.NetworkState, error) {
	network, err := eng.CreateFreeNetwork(ctx, role, internal)
	if err != nil {
		return nil, provision.NetworkState{}, fmt.Errorf("the %s network: %w", role, err)
	}

	state, err := eng.InspectNetwork(ctx, network)
	if err != nil {
		return nil, provision.NetworkState{}, fmt.Errorf("the %s network: %w", role, err)
	}

	if !state.Gateway.Is4() {
		return nil, provision.NetworkState{}, fmt.Errorf("%w: the %s network has gateway %q", errNetwork, role,
			state.Gateway)
	}

	return network, state, nil
}

// Config is what the topology is built from, once per check.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Config struct {
	// Networks are the check's networks, from CreateNetworks.
	Networks Networks
	// Image is the relay image, from Image. Every relay is created from its ID.
	Image compose.Image
	// Engine is the check's engine.
	Engine *provision.Engine
	// Upstreams is the per-start upstream source the listener set reads at every attach.
	Upstreams harness.UpstreamSource
	// HTTPHost is the HTTP stub's logical host; empty uses the default.
	HTTPHost string
	// Hostname is the target's hostname, identical for every start.
	Hostname string
	// VerifyTarget replaces the address the verifier dials. It exists for tests alone, which force a
	// wrong address with it; a check never sets it.
	VerifyTarget string
	// Layout is the relay set.
	Layout Layout
	// Postgres and Opaque split Layout's keys between the two proxies that serve them.
	Postgres []string
	Opaque   []string
}

// Topology is one check's relays and the listeners they pipe to.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Topology struct {
	set *harness.ListenerSet
	// verifier is the verification container, kept until teardown.
	verifier *provision.Container
	cfg      Config
	mu       sync.Mutex
	token    relay.Token
	mode     harness.Mode
}

// Open mints the check's relay token, detects how containers reach this host, and opens the listener
// set on the address that mode binds — before any relay exists.
func Open(ctx context.Context, cfg Config) (*Topology, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}

	token, err := relay.NewToken()
	if err != nil {
		return nil, fmt.Errorf("the relay token: %w", err)
	}

	local, err := localAddresses()
	if err != nil {
		return nil, err
	}

	mode := detectMode(cfg.Networks.Gateway, local)

	bind := netip.MustParseAddr("127.0.0.1")
	if mode == harness.ModeGateway {
		bind = cfg.Networks.Gateway
	}

	set, err := harness.OpenListeners(ctx, harness.ListenerConfig{
		Bind:      bind,
		Upstreams: cfg.Upstreams,
		HTTPHost:  cfg.HTTPHost,
		Postgres:  cfg.Postgres,
		Opaque:    cfg.Opaque,
		Bus:       cfg.Layout.BusPorts,
		Token:     token,
		Mode:      mode,
	})
	if err != nil {
		return nil, fmt.Errorf("the invocation listeners: %w", err)
	}

	return &Topology{cfg: cfg, set: set, token: token, mode: mode}, nil
}

// validate refuses a configuration the topology cannot build: the relay set and the proxies' split
// of its keys must agree, or a relayed port would reach no proxy.
func validate(cfg Config) error {
	switch {
	case cfg.Engine == nil:
		return fmt.Errorf("%w: no engine", errConfig)
	case cfg.Upstreams == nil:
		return fmt.Errorf("%w: no upstream source", errConfig)
	case cfg.Hostname == "":
		return fmt.Errorf("%w: no hostname for the target", errConfig)
	case cfg.Networks.Service == nil || cfg.Networks.Dependency == nil:
		return fmt.Errorf("%w: no networks", errConfig)
	default:
	}

	served := slices.Sorted(slices.Values(slices.Concat(cfg.Postgres, cfg.Opaque)))
	if keys := cfg.Layout.Keys(); !slices.Equal(served, keys) {
		return fmt.Errorf("%w: the relays carry [%s] but the proxies serve [%s]", errConfig,
			strings.Join(keys, " "), strings.Join(served, " "))
	}

	return nil
}

// localAddresses are this host's IPv4 addresses.
func localAddresses() ([]netip.Addr, error) {
	found, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("list this host's addresses: %w", err)
	}

	var local []netip.Addr

	for _, entry := range found {
		if network, isNetwork := entry.(*net.IPNet); isNetwork {
			if addr, parsed := netip.AddrFromSlice(network.IP); parsed {
				local = append(local, addr.Unmap())
			}
		}
	}

	return local, nil
}

// Listeners is the check's listener set.
func (t *Topology) Listeners() *harness.ListenerSet {
	return t.set
}

// Mode is how containers reach this host.
func (t *Topology) Mode() harness.Mode {
	return t.mode
}

// VerifyError is the host-address verification failing: containers cannot reach this host where the
// mode says they should. It names the mode, the address tried and what most likely stands between.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type VerifyError struct {
	// Address is what the verifier dialled.
	Address string
	// Cause is what the verifier reported, or why it could not.
	Cause string
	Mode  harness.Mode
}

func (e *VerifyError) Error() string {
	likely := "a host firewall on the Docker bridge"
	if e.Mode == harness.ModeHostAlias {
		likely = "WSL's localhost forwarding being off"
	}

	return fmt.Sprintf("containers cannot reach this host in %s mode at %s: %s; most likely %s, "+
		"or a rootless or remote engine", e.Mode, e.Address, e.Cause, likely)
}

// Verify proves, before any relay or dependency exists, that a container on the dependency network
// reaches the listener set where the relays will dial it, and fixes that address. There is no
// fallback and no guessed address: a failure is a VerifyError.
func (t *Topology) Verify(ctx context.Context) error {
	target := t.cfg.VerifyTarget

	switch {
	case target != "":
	case t.mode == harness.ModeGateway:
		target = t.cfg.Networks.Gateway.String()
	default:
		target = hostAlias
	}

	verified, err := t.runVerifier(ctx, target)
	if err != nil {
		return err
	}

	if t.mode == harness.ModeGateway && verified != t.cfg.Networks.Gateway {
		return &VerifyError{Mode: t.mode, Address: target, Cause: "the verifier reached " + verified.String()}
	}

	t.set.SetAdvertise(verified)

	if err := t.set.CloseVerify(); err != nil {
		return fmt.Errorf("close the verification listener: %w", err)
	}

	return nil
}

// runVerifier runs the verifier to its exit, within its wait, and returns the address it verified.
func (t *Topology) runVerifier(ctx context.Context, target string) (netip.Addr, error) {
	port, _ := t.set.Port(harness.KeyVerify)
	argv := relay.Verify{Target: target, Dial: waits.VerifierDial, Token: t.token, Port: port}.Args()

	verifier, err := t.cfg.Engine.CreateContainer(ctx, relayContainerSpec(t.cfg.Image, rules.KindVerifier, "", argv,
		nil, provision.NetworkAttach{Network: t.cfg.Networks.Dependency}))
	if err != nil {
		return netip.Addr{}, fmt.Errorf("create the verifier: %w", err)
	}

	t.mu.Lock()
	t.verifier = verifier
	t.mu.Unlock()

	if startErr := t.cfg.Engine.Start(ctx, verifier); startErr != nil {
		return netip.Addr{}, fmt.Errorf("start the verifier: %w", startErr)
	}

	failed := func(cause string) (netip.Addr, error) {
		return netip.Addr{}, &VerifyError{Mode: t.mode, Address: target, Cause: cause}
	}

	select {
	case <-t.cfg.Engine.Exited(verifier):
	case <-time.After(waits.Verifier):
		return failed(fmt.Sprintf("the verifier did not finish within %s", waits.Verifier))
	case <-ctx.Done():
		return netip.Addr{}, fmt.Errorf("wait for the verifier: %w", ctx.Err())
	}

	state, err := t.cfg.Engine.Status(ctx, verifier)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("read the verifier's exit: %w", err)
	}

	if state.ExitCode != 0 {
		stderr, stderrErr := t.cfg.Engine.LastStderrLine(ctx, verifier)
		if stderrErr != nil {
			stderr = "its stderr is unreadable: " + stderrErr.Error()
		}

		return failed(fmt.Sprintf("exit %d: %s", state.ExitCode, stderr))
	}

	return t.verifiedAddress(ctx, verifier, failed)
}

// verifiedAddress reads the address from the verifier's verified line.
func (t *Topology) verifiedAddress(
	ctx context.Context,
	verifier *provision.Container,
	failed func(string) (netip.Addr, error),
) (netip.Addr, error) {
	readCtx, cancel := context.WithTimeout(ctx, waits.Verifier)
	defer cancel()

	line, err := t.cfg.Engine.Output(readCtx, verifier, relay.Verified)
	if err != nil {
		return failed(fmt.Sprintf("no verified line: %v", err))
	}

	verified, err := netip.ParseAddr(strings.TrimSpace(strings.TrimPrefix(line, relay.Verified)))
	if err != nil || !verified.Is4() {
		return failed(fmt.Sprintf("the verified line %q names no IPv4 address", line))
	}

	return verified, nil
}

// relayContainerSpec is every relay container's settings: no capabilities, a read-only root, no new
// privileges, no forwarding between its two networks — a relay is dual-homed, and the engine's default
// would route between them — and the image's own unprivileged user. Nothing is published, mounted or
// passed in the environment.
func relayContainerSpec(
	image compose.Image,
	kind rules.Kind,
	service string,
	argv []string,
	sysctls map[string]string,
	networks ...provision.NetworkAttach,
) provision.ContainerSpec {
	settings := map[string]string{"net.ipv4.ip_forward": "0"}
	maps.Copy(settings, sysctls)

	return provision.ContainerSpec{
		Kind:     kind,
		Service:  service,
		Networks: networks,
		Spec: compose.Spec{
			Service:     service,
			Image:       image.ID,
			Cmd:         argv,
			CmdSet:      true,
			CapDrop:     []string{"ALL"},
			SecurityOpt: []string{"no-new-privileges"},
			ReadOnly:    true,
			Sysctls:     settings,
		},
	}
}
