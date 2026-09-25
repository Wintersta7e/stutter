package cli

import (
	"bytes"
	"cmp"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
	"github.com/Wintersta7e/stutter/internal/relay"
	"github.com/Wintersta7e/stutter/internal/report"
	"github.com/Wintersta7e/stutter/internal/topology"
	"github.com/Wintersta7e/stutter/internal/waits"
)

// caMode lets every target's user read the CA file and no one write it.
const caMode = 0o444

// selfExecutable is the running binary, read by its own bytes: a rebuild at its path cannot replace
// the one that is running.
const selfExecutable = "/proc/self/exe"

var (
	// errCAShape means the listener set's CA was not exactly one certificate.
	errCAShape = errors.New("the check's CA is not exactly one PEM certificate")
	// errUnresolved means the second classification started a service whose image was never
	// resolved: the first pass would never have started it.
	errUnresolved = errors.New("a service to start has no resolved image")
)

// composeRun is what the command line asked of a compose check.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type composeRun struct {
	service string
	stream  string
	corpus  string
	routes  string
	// compose are the --compose files as given; files are the same, absolute.
	compose []string
	files   []string
	// profiles and consumers are --profile and --consumer, in the order given.
	profiles  []string
	consumers []string
	// timingFlags name each timing flag given.
	timingFlags []string
	// startup is --startup; zero leaves it to the harness's default.
	startup time.Duration
	keep    bool
}

// composeCheck is one compose check's state, built step by step in the pre-run order.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type composeCheck struct {
	// images are the pinned images, by service.
	images map[string]compose.Image
	// restored is where the latest restore put each dependency endpoint.
	restored map[string]netip.AddrPort
	// upstreamMap turns one start's restores and the bus's addresses into the listener set's upstreams:
	// the topology's, once it is open.
	upstreamMap func(restored map[string]netip.AddrPort, bus, monitor netip.AddrPort) (
		map[string]netip.AddrPort, error)
	stderr   *lockedWriter
	header   *report.Header
	engine   *provision.Engine
	model    *compose.Model
	deps     *provision.Dependencies
	topology *topology.Topology
	bus      *corpus.Corpus
	identity provision.Identity
	networks topology.Networks
	// caPath is the check's CA file on the host.
	caPath         string
	classification compose.Classification
	relayImage     compose.Image
	layout         topology.Layout
	run            composeRun
	prints         compose.Prints
	loaded         corpus.Loaded
	targetSpec     compose.Spec
	mu             sync.Mutex
}

func newComposeCheck(run composeRun, stderr *lockedWriter) *composeCheck {
	return &composeCheck{run: run, stderr: stderr, header: &report.Header{Given: report.Given{
		Stream: run.stream, Corpus: run.corpus, Routes: run.routes, Compose: run.compose, Profiles: run.profiles,
		Consumers: run.consumers, Timings: run.timingFlags,
	}}}
}

// prerunSteps are everything before the first consumer check, in order. Every refusal comes before
// the engine is touched; the start fingerprint and the key refusals before the first mutation; the
// classification containers and the endpoint split before any listener; the CA file once the
// listeners' address is verified; the bus once the relays are up.
func (c *composeCheck) prerunSteps() []step {
	return []step{
		{name: "preconditions", run: c.preconditions},
		{name: "static-self", run: c.staticSelf},
		{name: "host-networking", run: c.hostNetworking},
		{name: "open", run: c.open},
		{name: "corpus", run: c.loadCorpus},
		{name: "model", run: c.parseModel},
		{name: "classify", run: c.classifyLocal},
		{name: "fingerprint", run: c.fingerprint},
		{name: "refusals", run: c.refuse},
		{name: "images", run: c.resolveImages},
		{name: "reclassify", run: c.reclassify},
		{name: "bus-urls", run: c.refuseBusURLs},
		{name: "relay-image", run: c.buildRelayImage},
		{name: "networks", run: c.createNetworks},
		{name: "classification-containers", run: c.classifyContainers},
		{name: "dependencies", run: c.dependencies},
		{name: "layout", run: c.layOut},
		{name: "listeners", run: c.openListeners},
		{name: "verify", run: c.verify},
		{name: "ca-file", run: c.writeCAFile},
		{name: "relays", run: c.startRelays},
		{name: "bus", run: c.openBus},
	}
}

// prepare runs the pre-run steps in order and stops at the first that fails, naming it.
//
//nolint:unused // the compose sequence calls it once `stutter check --compose` is wired.
func (c *composeCheck) prepare(ctx context.Context) error {
	for _, each := range c.prerunSteps() {
		if err := each.run(ctx); err != nil {
			return fmt.Errorf("%s: %w", each.name, err)
		}
	}

	return nil
}

func (c *composeCheck) preconditions(ctx context.Context) error {
	identity, err := provision.Preconditions(ctx)
	c.identity = identity

	return err //nolint:wrapcheck // named with its step by prepare.
}

func (c *composeCheck) staticSelf(context.Context) error {
	return relay.Qualify(selfExecutable, c.identity.OS, c.identity.Arch) //nolint:wrapcheck // named by prepare.
}

func (*composeCheck) hostNetworking(ctx context.Context) error {
	return topology.Prerequisites(ctx, provision.WSLNetworkingMode) //nolint:wrapcheck // named by prepare.
}

// open gives the check its ledger and private directory, then names both on stderr with what the
// sweep of dead checks found.
func (c *composeCheck) open(ctx context.Context) error {
	engine, err := provision.Open(ctx, provision.Options{Keep: c.run.keep})
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.engine = engine
	identity := engine.Identity()
	c.header.Engine = &identity

	c.stderr.line(checkStartLine(engine.CheckID(), engine.PrivateDir()))
	c.stderr.line(sweepLine(engine.Sweep()))

	return nil
}

func (c *composeCheck) loadCorpus(context.Context) error {
	loaded, err := corpus.LoadDir(c.run.corpus)
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.loaded = loaded
	c.header.Corpus = &c.loaded

	return nil
}

// parseModel reads the compose project once: the files in the order given, the profiles, nothing
// else — no project directory, env file or project name of Stutter's.
func (c *composeCheck) parseModel(ctx context.Context) error {
	model, err := compose.Parse(ctx, c.engine.ComposeConfig, compose.Inputs{
		Service: c.run.service, Files: c.run.files, Profiles: c.run.profiles,
	})
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.model = model
	c.header.Model = model

	return nil
}

// classifyLocal is the first classification pass. It is given no image: an image present locally is
// pinned without a pull when its service is resolved, so the second pass sees the same evidence.
func (c *composeCheck) classifyLocal(context.Context) error {
	c.images = map[string]compose.Image{}

	return c.classify(nil)
}

func (c *composeCheck) classify(answers map[string]map[uint16]pg.Answer) error {
	classified, err := compose.Classify(c.model, c.images, answers)
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.classification = classified
	c.header.Classification = &c.classification

	return nil
}

// fingerprint is the start walk over every bind source a started service mounts: completed before
// anything is created, so the end walk can prove later runs read the same input.
func (c *composeCheck) fingerprint(ctx context.Context) error {
	prints, err := compose.Fingerprint(ctx, c.model.BindSources(c.classification.Started()))
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.prints = prints
	c.header.Prints = &c.prints

	return nil
}

func (c *composeCheck) refuse(context.Context) error {
	return c.model.Refusals(c.classification.Started(), rules.Namespace, c.prints) //nolint:wrapcheck // named.
}

// resolveImages pins every started service's image: built where the model builds it, resolved
// otherwise. It is the first mutation.
func (c *composeCheck) resolveImages(ctx context.Context) error {
	refs := c.model.Images(c.classification.Started())
	built := map[string]rules.Kind{}

	for _, ref := range refs {
		if ref.Service == c.run.service {
			c.header.Target = ref
		}

		if ref.Build && (ref.Ref == "" || ref.Policy == compose.PullBuild) {
			built[ref.Service] = c.kindOf(ref.Service)

			continue
		}

		image, err := c.engine.ResolveImage(ctx, ref.Ref, ref.Platform)
		if err != nil {
			return fmt.Errorf("the image of %s: %w", ref.Service, err)
		}

		c.images[ref.Service] = image
	}

	if len(built) > 0 {
		images, err := c.engine.Build(ctx, provision.BuildSpec{
			Render: c.model.BuildModel, Services: built, Dir: filepath.Dir(c.run.files[0]),
		})
		if err != nil {
			return err //nolint:wrapcheck // named with its step by prepare.
		}

		maps.Copy(c.images, images)
	}

	if image, pinned := c.images[c.run.service]; pinned {
		c.header.Image = &image
	}

	return nil
}

// kindOf is what a started service is to the check.
func (c *composeCheck) kindOf(service string) rules.Kind {
	if service == c.run.service {
		return rules.KindTarget
	}

	for _, dep := range c.classification.Deps {
		if dep.Service == service && dep.Role == compose.RoleJob {
			return rules.KindJob
		}
	}

	return rules.KindDependency
}

// reclassify is the second pass, over the resolved images. Every service it starts must have one.
func (c *composeCheck) reclassify(context.Context) error {
	if err := c.classify(nil); err != nil {
		return err
	}

	for _, service := range c.classification.Started() {
		if _, pinned := c.images[service]; !pinned {
			return fmt.Errorf("%w: %s", errUnresolved, service)
		}
	}

	return nil
}

// refuseBusURLs refuses a target configured to reach the bus over TLS or websockets, before anything
// starts. The target's spec is the header's account of what Stutter changed.
func (c *composeCheck) refuseBusURLs(context.Context) error {
	spec, err := c.model.Spec(c.run.service, c.images[c.run.service], compose.CAEnvironment())
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.targetSpec = spec
	c.header.Spec = &c.targetSpec

	return natsproxy.RefuseURLs(spec.Env, c.classification.BusNames) //nolint:wrapcheck // named by prepare.
}

func (c *composeCheck) buildRelayImage(ctx context.Context) error {
	image, err := topology.Image(ctx, c.engine, selfExecutable)
	c.relayImage = image

	return err //nolint:wrapcheck // named with its step by prepare.
}

func (c *composeCheck) createNetworks(ctx context.Context) error {
	networks, err := topology.CreateNetworks(ctx, c.engine)
	c.networks = networks

	return err //nolint:wrapcheck // named with its step by prepare.
}

// classifyContainers asks every still-opaque endpoint whether it speaks Postgres, on a throwaway
// container of its service, and runs the third pass with the answers: the classification is frozen
// from here.
func (c *composeCheck) classifyContainers(ctx context.Context) error {
	answers, err := provision.Classify(ctx, provision.ClassifyConfig{
		Engine: c.engine, Model: c.model, Images: c.images, Network: c.networks.Dependency,
		Classification: c.classification, Startup: cmp.Or(c.run.startup, harness.DefaultStartup),
	})
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	return c.classify(answers)
}

// dependencies derives the started dependencies from the final classification, which fixes the split
// of endpoints between the Postgres and the opaque proxies before any listener exists.
func (c *composeCheck) dependencies(context.Context) error {
	deps, err := provision.NewDependencies(provision.DependencyConfig{
		Engine: c.engine, Model: c.model, Images: c.images, Network: c.networks.Dependency, Helper: c.relayImage,
		Classification: c.classification,
		Waits: provision.Waits{
			DependencyReady: waits.DependencyReady, Job: waits.Job, PostgresRestore: waits.PostgresRestore,
		},
	})
	c.deps = deps

	return err //nolint:wrapcheck // named with its step by prepare.
}

func (c *composeCheck) layOut(context.Context) error {
	layout, err := topology.LayoutFrom(c.classification)
	c.layout = layout

	return err //nolint:wrapcheck // named with its step by prepare.
}

// openListeners opens the check's listener set, before any relay exists. Its upstream source is read
// at every attach, through the topology this returns.
func (c *composeCheck) openListeners(ctx context.Context) error {
	postgres, opaque := c.deps.Keys()

	opened, err := topology.Open(ctx, topology.Config{
		Engine: c.engine, Upstreams: c.upstreams, Networks: c.networks, Layout: c.layout, Image: c.relayImage,
		Hostname: c.targetSpec.Hostname, Postgres: postgres, Opaque: opaque,
	})
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.topology = opened
	c.upstreamMap = opened.UpstreamMap
	c.header.Mode = opened.Mode()

	return nil
}

func (c *composeCheck) verify(ctx context.Context) error {
	return c.topology.Verify(ctx) //nolint:wrapcheck // named with its step by prepare.
}

// writeCAFile writes the check's CA where every target's CA variables point, once the address the
// containers reach the listeners at is verified and before any target exists.
func (c *composeCheck) writeCAFile(context.Context) error {
	path, err := c.engine.HostPath(provision.HostCA)
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	if err := writeCA(path, c.topology.Listeners().CAPEM()); err != nil {
		return err
	}

	c.caPath = path

	return c.engine.LogHostPath(path) //nolint:wrapcheck // named with its step by prepare.
}

func (c *composeCheck) startRelays(ctx context.Context) error {
	return c.topology.Relays(ctx) //nolint:wrapcheck // named with its step by prepare.
}

// openBus starts the embedded bus in the check's store directory, bound to --stream.
func (c *composeCheck) openBus(ctx context.Context) error {
	dir, err := c.engine.HostPath(provision.HostStore)
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	bus, err := corpus.Open(ctx, dir, c.run.stream)
	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.bus = bus

	return c.engine.LogHostPath(dir) //nolint:wrapcheck // named with its step by prepare.
}

// upstreams is the listener set's upstream source: the latest restore, and where the bus is now —
// read at every attach, because a restored dependency and a restarted bus each come back elsewhere.
func (c *composeCheck) upstreams(context.Context) (map[string]netip.AddrPort, error) {
	bus, err := addrPortOf(c.bus.URL())
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	restored := c.restored
	c.mu.Unlock()

	return c.upstreamMap(restored, bus, c.bus.MonitorAddr())
}

// setRestored records where the latest restore put each dependency endpoint.
func (c *composeCheck) setRestored(restored map[string]netip.AddrPort) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.restored = restored
}

// addrPortOf is a client URL's address.
func addrPortOf(rawURL string) (netip.AddrPort, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("parse the bus address: %w", err)
	}

	addr, err := netip.ParseAddrPort(parsed.Host)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("parse the bus address: %w", err)
	}

	return addr, nil
}

// writeCA writes the CA file: exactly one certificate, readable by every target's user, written by
// no one. It is public — a certificate, never its key.
func writeCA(path string, certificate []byte) error {
	block, rest := pem.Decode(certificate)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) > 0 {
		return errCAShape
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, caMode)
	if err != nil {
		return fmt.Errorf("create the CA file: %w", err)
	}

	_, writeErr := file.Write(pem.EncodeToMemory(block))

	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("write the CA file: %w", err)
	}

	return nil
}
