package cli

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
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
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
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
	// errImageAbsent means a service's pull_policy is never and the engine does not hold its image.
	errImageAbsent = errors.New("pull_policy is never and the image is not on the engine")
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
	// timingFlags name each timing flag given.
	timingFlags []string
	// files are the --compose files, absolute, in the order given.
	files []string
	// profiles and consumers are --profile and --consumer, in the order given.
	profiles  []string
	consumers []string
	// compose are the --compose files as given.
	compose []string
	// httpDefault and httpRoutes are the --routes file, decoded.
	httpRoutes  []httpproxy.Route
	httpDefault httpproxy.Response
	// startup, quiesce and drain are the timing flags; zero leaves each to its owner's default.
	startup time.Duration
	quiesce time.Duration
	drain   time.Duration
	// maxRuns is each consumer check's own budget of faulted runs.
	maxRuns int
	keep    bool
	// gatesOnly is the gate command.
	gatesOnly bool
}

// composeCheck is one compose check's state, built step by step in the pre-run order.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type composeCheck struct {
	// before and after are the bus surveyed at B0 and after the probe start.
	before corpus.Survey
	after  corpus.Survey
	engine *provision.Engine
	// newSandbox builds one consumer check's sandbox; logHold writes one hold to the invocation log.
	newSandbox func(plan consumerPlan) (sandbox, error)
	logHold    func(hold provision.Hold) error
	// discover is the discovery start from B1.
	discover func(ctx context.Context) (harness.Discovery, error)
	stderr   *lockedWriter
	header   *report.Header
	// upstreamMap turns one start's restores and the bus's addresses into the listener set's upstreams:
	// the topology's, once it is open.
	upstreamMap func(restored map[string]netip.AddrPort, bus, monitor netip.AddrPort) (
		map[string]netip.AddrPort, error)
	model    *compose.Model
	deps     *provision.Dependencies
	topology *topology.Topology
	bus      *corpus.Corpus
	// probed is what the probe start learned; nil when the stream existed at B0.
	probed *harness.Probe
	// restored is where the latest restore put each dependency endpoint.
	restored map[string]netip.AddrPort
	// images are the pinned images, by service.
	images map[string]compose.Image
	// local are the images the engine held before anything was resolved, by service.
	local    map[string]compose.Image
	identity provision.Identity
	networks topology.Networks
	// b0 and b1 are the bus checkpoints: before any start, and once the stream's owner is settled.
	b0 corpus.Checkpoint
	b1 corpus.Checkpoint
	// caPath is the check's CA file on the host.
	caPath         string
	classification compose.Classification
	relayImage     compose.Image
	layout         topology.Layout
	// hashKey keys every run's raw-effect hash: one per check.
	hashKey []byte
	prints  compose.Prints
	loaded  corpus.Loaded
	run     composeRun
	// found is what discovery learned: the consumer checks are made from it.
	found      harness.Discovery
	targetSpec compose.Spec
	mu         sync.Mutex
}

func newComposeCheck(run composeRun, stderr *lockedWriter) *composeCheck {
	c := &composeCheck{
		run:    run,
		stderr: stderr,
		header: &report.Header{Given: report.Given{
			Stream: run.stream, Corpus: run.corpus, Routes: run.routes, Compose: run.compose, Profiles: run.profiles,
			Consumers: run.consumers, Timings: run.timingFlags,
		}},
	}
	c.discover = c.discoverStart
	c.newSandbox = c.buildSandbox
	c.logHold = c.writeHold

	return c
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
		{name: "seed", run: c.seed},
		{name: "jobs", run: c.jobs},
		{name: "snapshot", run: c.snapshot},
		{name: "checkpoint-b0", run: c.checkpointB0},
		{name: "probe", run: c.probe},
		{name: "checkpoint-b1", run: c.checkpointB1},
		{name: "discovery", run: c.discovery},
	}
}

// prepare runs the pre-run steps in order and stops at the first that fails, naming it.
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

	c.hashKey = make([]byte, hashKeyLen)
	if _, err := rand.Read(c.hashKey); err != nil {
		return fmt.Errorf("generate the effect hash key: %w", err)
	}

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

// classifyLocal is the first classification pass, on the images the engine holds now: read, never
// pulled, for every service naming one. What it finds also decides how each started service's image is
// pinned.
func (c *composeCheck) classifyLocal(ctx context.Context) error {
	local, err := localImages(ctx, c.model.Images(c.model.Services()), c.engine.LocalImage)
	if err != nil {
		return err
	}

	c.local = local
	c.images = maps.Clone(local)

	return c.classify(nil)
}

// localImages reads, by service, every image the engine holds of those refs name. A service naming no
// image is not asked about; one whose image is absent is left out.
func localImages(
	ctx context.Context,
	refs []compose.ImageRef,
	lookup func(ctx context.Context, ref string) (compose.Image, bool, error),
) (map[string]compose.Image, error) {
	local := map[string]compose.Image{}

	for _, ref := range refs {
		if ref.Ref == "" {
			continue
		}

		image, present, err := lookup(ctx, ref.Ref)
		if err != nil {
			return nil, fmt.Errorf("read the image of %s: %w", ref.Service, err)
		}

		if present {
			local[ref.Service] = image
		}
	}

	return local, nil
}

// imageStep is how one started service's image is pinned.
type imageStep uint8

const (
	// imageResolve pins the image the model names: the engine's own when present, pulled when absent.
	imageResolve imageStep = iota + 1
	// imageBuild builds it, under a reference the check reserved.
	imageBuild
)

// imageStepFor decides how a started service's image is pinned, from whether the engine holds it: a
// service with a build and no image, or pull_policy build, is built; one with both an image and a build
// builds only when its image is absent; pull_policy never refuses an absent image rather than pull it.
func imageStepFor(ref compose.ImageRef, present bool) (imageStep, error) {
	switch {
	case ref.Build && (ref.Ref == "" || ref.Policy == compose.PullBuild):
		return imageBuild, nil
	case present:
		return imageResolve, nil
	case ref.Policy == compose.PullNever:
		return 0, fmt.Errorf("%w: service %s, image %s", errImageAbsent, ref.Service, ref.Ref)
	case ref.Build:
		return imageBuild, nil
	default:
		return imageResolve, nil
	}
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

// resolveImages pins every started service's image as its pull policy says, reading what the first
// pass found on the engine: built, or resolved — pulled only where a pull is allowed. It is the first
// mutation.
func (c *composeCheck) resolveImages(ctx context.Context) error {
	refs := c.model.Images(c.classification.Started())
	built := map[string]rules.Kind{}

	for _, ref := range refs {
		if ref.Service == c.run.service {
			c.header.Target = ref
		}

		_, present := c.local[ref.Service]

		step, err := imageStepFor(ref, present)
		if err != nil {
			return err
		}

		if step == imageBuild {
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

func (c *composeCheck) seed(ctx context.Context) error {
	return c.deps.Seed(ctx) //nolint:wrapcheck // named with its step by prepare.
}

// jobs runs the discovered jobs inside the seed-phase bracket, when there are any.
func (c *composeCheck) jobs(ctx context.Context) error {
	return runJobs(ctx, c.deps.Discovered(), c.openSeedBus, c.deps.Jobs, c.topology.EndSeedBus)
}

// openSeedBus opens the path jobs reach the bus by during the seed phase, unrecorded.
func (c *composeCheck) openSeedBus(ctx context.Context) error {
	client, err := addrPortOf(c.bus.URL())
	if err != nil {
		return err
	}

	return c.topology.SeedBus(ctx, client, c.bus.MonitorAddr()) //nolint:wrapcheck // named by prepare.
}

// runJobs runs the jobs between opening and closing the seed bus, which is closed whether they succeed
// or not. With no job to run, nothing is opened.
func runJobs(ctx context.Context, discovered []string, open, run, closeBus func(context.Context) error) error {
	if len(discovered) == 0 {
		return nil
	}

	if err := open(ctx); err != nil {
		return err
	}

	return errors.Join(run(ctx), closeBus(ctx))
}

// snapshot proves every seed clean and commits it; the seed records are the header's account of what
// every start begins from.
func (c *composeCheck) snapshot(ctx context.Context) error {
	if err := c.deps.Snapshot(ctx); err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	records := c.deps.Records()
	c.header.Seeds = &records

	return nil
}

// checkpointB0 takes the bus before any start of the service, and surveys it: a stream present now
// was made by a job.
func (c *composeCheck) checkpointB0(ctx context.Context) error {
	var err error

	c.b0, err = c.checkpoint(ctx, provision.HostB0)
	if err != nil {
		return err
	}

	c.before, err = c.bus.Survey(ctx)

	return err //nolint:wrapcheck // named with its step by prepare.
}

// needsProbe reports a stream absent at B0: whether the service makes it is learned by starting it.
func needsProbe(survey corpus.Survey) bool {
	return !survey.Exists()
}

// probe runs the probe start from B0, when the stream is absent, and surveys the bus it left.
func (c *composeCheck) probe(ctx context.Context) error {
	if !needsProbe(c.before) {
		return nil
	}

	began := time.Now()
	probed, err := harness.ProbeStart(ctx, c.startConfig(rules.KindProbe, &c.b0))
	c.stderr.line(runLine("", runProbe, policy.FaultNone, time.Since(began), 0))

	if err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	c.probed = &probed
	c.after, err = c.bus.Survey(ctx)

	return err //nolint:wrapcheck // named with its step by prepare.
}

// checkpointB1 settles who owns the stream — a job, the service, or Stutter — from B0 again, and takes
// the checkpoint discovery and every run start from.
func (c *composeCheck) checkpointB1(ctx context.Context) error {
	after := c.before

	if c.probed != nil {
		if err := c.bus.Restore(ctx, c.b0); err != nil {
			return err //nolint:wrapcheck // named with its step by prepare.
		}

		after = c.after
	}

	if err := c.bus.Establish(ctx, c.before, after, c.loaded.Messages); err != nil {
		return err //nolint:wrapcheck // named with its step by prepare.
	}

	var err error

	c.b1, err = c.checkpoint(ctx, provision.HostB1)

	return err
}

// checkpoint copies the bus store into the check-private entry name.
func (c *composeCheck) checkpoint(ctx context.Context, name provision.HostName) (corpus.Checkpoint, error) {
	dir, err := c.engine.HostPath(name)
	if err != nil {
		return corpus.Checkpoint{}, err //nolint:wrapcheck // named with its step by prepare.
	}

	checkpoint, err := c.bus.Checkpoint(ctx, dir)
	if err != nil {
		return corpus.Checkpoint{}, err //nolint:wrapcheck // named with its step by prepare.
	}

	return checkpoint, c.engine.LogHostPath(dir) //nolint:wrapcheck // named with its step by prepare.
}

// discovery reads the consumers the service creates, from B1 — unless the probe start saw the stream
// appear and read them already, on the same start.
func (c *composeCheck) discovery(ctx context.Context) error {
	if c.probed != nil && c.probed.Discovery != nil {
		c.found = *c.probed.Discovery

		return nil
	}

	began := time.Now()
	found, err := c.discover(ctx)
	c.stderr.line(runLine("", runDiscovery, policy.FaultNone, time.Since(began), 0))
	c.found = found

	return err
}

// discoverStart is discovery's own start of the service, from B1.
func (c *composeCheck) discoverStart(ctx context.Context) (harness.Discovery, error) {
	return harness.Discover(ctx, c.startConfig(rules.KindDiscovery, &c.b1)) //nolint:wrapcheck // named by prepare.
}

// startConfig is the harness configuration every start of the check shares: its bus, its listeners, a
// target container of kind, the dependencies restored before every start, the check's hash key and
// the timings given. baseline is the bus checkpoint the start restores.
func (c *composeCheck) startConfig(kind rules.Kind, baseline *corpus.Checkpoint) harness.Config {
	return harness.Config{
		Corpus: c.bus, Listeners: c.topology.Listeners(), Start: c.target().startAs(kind), Baseline: baseline,
		Reset: c.restore, HashKey: c.hashKey, Startup: c.run.startup, Quiesce: c.run.quiesce,
	}
}

// target is the service under test's container, as every start places it.
func (c *composeCheck) target() *target {
	return &target{
		engine: c.engine, topology: c.topology, deps: c.deps, model: c.model, service: c.run.service,
		ca: c.caPath, image: c.images[c.run.service],
	}
}

// restore replaces every started dependency before a start, and keeps where each came back.
func (c *composeCheck) restore(ctx context.Context) error {
	restored, err := c.deps.Restore(ctx)
	if err != nil {
		return err //nolint:wrapcheck // the harness names the reset.
	}

	c.setRestored(restored)

	return nil
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
