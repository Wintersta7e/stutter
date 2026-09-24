// Package harness provisions a service under test with all of its egress observed, and adapts it to
// the interface a check drives.
//
// Everything the service can talk to is proxied: the database, the bus, outbound HTTP, and any
// dependency on a protocol Stutter does not parse. Watching only the database makes the commonest
// JetStream-native idempotency guard invisible — a claim in a key/value bucket is a publish to
// $KV.<bucket>.<key> — while omitting HTTP hides API-backed guards and omitting the rest hides a
// whole datastore. A handler judged on partial evidence gets the right verdict only by luck.
//
// A Go caller supplies the service as a function that connects it or starts it. A provisioned
// container is started the same way, but its connections do not reach proxies of the sandbox's own:
// they arrive through relays onto a listener set that outlives every sandbox of the check
// (Config.Listeners), and each run attaches its proxies to that set.
package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/policy"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	opaqueproxy "github.com/Wintersta7e/stutter/internal/proxy/opaque"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// errNoService means the sandbox was built with no service to observe, or with both run models at
// once — a service that consumes for itself AND is dispatched to would see every message twice.
var errNoService = errors.New("exactly one of Connect and Start must be set: " +
	"Connect dispatches to the service, Start lets it consume for itself")

// errListenersBeside means a sandbox on the invocation listeners was also given an endpoint of its own.
var errListenersBeside = errors.New("the listener set owns every endpoint of a relayed service")

// defaultHTTPHost is stable across runs while the listener's kernel-assigned port is not. The
// reserved .invalid suffix makes it impossible to mistake this local stub identity for a real host.
const defaultHTTPHost = "dependency.invalid"

// loopback is where the proxies listen unless the service under test cannot reach it.
const loopback = "127.0.0.1"

// alwaysProxied counts the proxies every run gets whatever the configuration says: the bus and the
// HTTP stub. The database and any unparsed dependency are added to it.
const alwaysProxied = 2

// Service is the service under test, already connected to the proxied dependencies.
type Service interface {
	// Handle processes one delivered message.
	Handle(ctx context.Context, msg replay.Message) error
	// Close releases the service's connections. It runs BEFORE the proxies are closed.
	Close(ctx context.Context)
}

// Addresses are the endpoints the service under test must dial.
//
// Every one points at a proxy, never at a real dependency. A service that connects directly
// produces no observable effects, which reads as a handler that did nothing at all.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Addresses struct {
	// Opaque is one proxied address per unparsed dependency, under the logical name it was
	// configured with.
	Opaque map[string]string
	// Postgres is the rewritten DSN. Empty when the sandbox was built without a database.
	Postgres string
	// NATS is the bus URL.
	NATS string
	// HTTP is the base URL of the HTTP stub.
	HTTP string
	// HTTPCACert is the PEM-encoded certificate authority the HTTP stub's certificate was signed
	// with, and is empty unless the stub is serving TLS. A service that does not trust it cannot
	// reach the stub at all, which reads as a handler that produced no effects.
	HTTPCACert []byte
}

// Connect builds the service against the proxied addresses.
type Connect func(ctx context.Context, at Addresses) (Service, error)

// Config describes the sandbox.
type Config struct {
	// Corpus is the recorded traffic and the bus the service talks to.
	Corpus *corpus.Corpus
	// Listeners is the check's invocation listener set, for a service reached through relays. Set, it
	// is where every endpoint, the HTTP host and the certificate authority come from, so none of
	// PostgresDSN, Opaque, BindHost, AdvertiseHost, HTTPHost or Connect may be set beside it.
	Listeners *ListenerSet
	// Connect builds a service Stutter dispatches to. Exactly one of Connect and Start is set.
	Connect Connect
	// Start builds a service that pulls from the bus for itself, as a provisioned container does.
	// Stutter then has nothing to dispatch and no acknowledgement of its own to withhold, so the run
	// is watched and faulted on the wire instead.
	Start Start
	// Baseline is the bus checkpoint every observed run restores before the service starts. Nil takes
	// one of the sandbox's own at the first observed run, after clearing the corpus stream once — the
	// path for a caller that published its corpus into the stream; a corpus opened without a stream
	// needs one set. Unused when Stutter dispatches.
	Baseline *corpus.Checkpoint
	// Reset returns the caller's dependencies to the starting position. A claim left in a key/value
	// bucket corrupts the next run exactly as leftover rows would: on the observed model the bus is
	// returned by restoring Baseline, and Reset covers everything else; when Stutter dispatches, Reset
	// covers the bus-side state too.
	Reset func(ctx context.Context) error
	// Recorded is the corpus every observed run replays. Nil reads it once from the corpus stream,
	// before the first run; set, the stream is never read for it.
	Recorded []corpus.Message
	// Opaque are dependencies on protocols Stutter does not parse, as logical name → real address.
	// The logical name is what appears in effects, because the proxy's port is assigned per run.
	Opaque map[string]string
	// PostgresDSN points at the REAL database. The sandbox rewrites its host to the proxy. Empty
	// means the service under test has no Postgres, which is the case an opaque dependency covers.
	PostgresDSN string
	// HTTPDefault is the response for an unconfigured endpoint and for a mutated-run call absent
	// from the clean run. Its zero value is a 200 JSON object.
	HTTPDefault httpproxy.Response
	// HTTPRoutes are optional exact endpoint responses used while capturing the first clean run.
	HTTPRoutes []httpproxy.Route
	// HTTPHost is the stable logical host rendered into effects when Connect uses the local proxy
	// URL. Empty uses a reserved non-resolving name and costs local callers no declaration.
	HTTPHost string
	// BindHost is the interface the proxies listen on. Empty is loopback, which is what a service
	// sharing this network namespace needs. A CONTAINERISED service does not share it, so its proxies
	// have to listen somewhere it can reach.
	BindHost string
	// AdvertiseHost is the host the service is told to dial, where that differs from the interface the
	// proxies bound. A container reaches its host by a name of its own and never by the host's
	// loopback, so the address that works for the listener is not the address to hand out.
	AdvertiseHost string
	// Consumer names the consumer under test, for a service that consumes for itself and creates more
	// than one on the corpus stream. Every other one is paused for the whole run, because effects from
	// consumers sharing a process interleave with no way to tell them apart. Empty picks the only
	// consumer there is, and is refused when there are several.
	Consumer string
	// HashKey keys the raw-effect hash. Every run in a comparison must share one.
	HashKey []byte
	// Policy is the recorded consumer configuration.
	Policy policy.Config
	// Quiesce is how long an attribution window stays open after a handler returns.
	Quiesce time.Duration
	// Drain lengthens how long an observed run waits in silence for messages still owed. Zero derives
	// the floor from Policy's redelivery deadlines (DrainFloor); a value below the floor is refused, so
	// the wait can only be lengthened. It is unused when Stutter dispatches, because there the driver
	// knows when it has stopped delivering.
	Drain time.Duration
	// Startup is how long a service that consumes for itself may take to create a consumer on the
	// corpus stream before the corpus is published regardless. Zero uses DefaultStartup.
	Startup time.Duration
	// HTTPTLS serves the stub over TLS instead of cleartext, for a service that will not talk to a
	// dependency any other way. Addresses.HTTPCACert is then what the service must trust.
	HTTPTLS bool
}

// Sandbox provisions one service under test. It satisfies the interface a check drives.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Sandbox struct {
	httpScript *httpproxy.Script
	// certificates is nil unless the stub serves TLS. It is minted once per sandbox, so every run in
	// a comparison presents the same certificate.
	certificates *authority
	// checkpoint is the sandbox's own starting point, taken at the first observed run when the
	// configuration supplies no Baseline.
	checkpoint *corpus.Checkpoint
	// holds is every Fill hold an observed run made, in run order.
	holds []FillHold
	// recorded is the corpus held outside the stream, taken before the first observed run. A run
	// stages only what it replays, so the messages a later whole-corpus run needs have to be in hand
	// before the first subset run.
	recorded []corpus.Message
	upstream string
	cfg      Config
}

// New builds a sandbox.
func New(cfg Config) (*Sandbox, error) {
	if (cfg.Connect == nil) == (cfg.Start == nil) {
		return nil, errNoService
	}

	if floor := DrainFloor(cfg.Policy, cfg.Quiesce); cfg.Start != nil && cfg.Drain > 0 && cfg.Drain < floor {
		return nil, fmt.Errorf("%w: Drain %s is below the floor of %s", ErrDrainBelowFloor, cfg.Drain, floor)
	}

	if cfg.Listeners != nil {
		return onListeners(cfg)
	}

	var upstream string

	if cfg.PostgresDSN != "" {
		parsed, err := url.Parse(cfg.PostgresDSN)
		if err != nil {
			return nil, fmt.Errorf("parse the database address: %w", err)
		}

		upstream = parsed.Host
	}

	if cfg.HTTPHost == "" {
		cfg.HTTPHost = defaultHTTPHost
	}

	var certificates *authority

	if cfg.HTTPTLS {
		minted, mintErr := newAuthority(cfg.HTTPHost)
		if mintErr != nil {
			return nil, mintErr
		}

		certificates = minted
	}

	return &Sandbox{
		cfg:          cfg,
		httpScript:   httpproxy.NewScript(cfg.HTTPDefault, cfg.HTTPRoutes),
		certificates: certificates,
		upstream:     upstream,
	}, nil
}

// onListeners builds a sandbox whose endpoints are the invocation listener set's. Its HTTP host and
// its certificate authority are the set's, so every consumer check presents the service one CA.
func onListeners(cfg Config) (*Sandbox, error) {
	beside := map[string]bool{
		"PostgresDSN":   cfg.PostgresDSN != "",
		"Opaque":        len(cfg.Opaque) > 0,
		"BindHost":      cfg.BindHost != "",
		"AdvertiseHost": cfg.AdvertiseHost != "",
		"HTTPHost":      cfg.HTTPHost != "",
		"Connect":       cfg.Connect != nil,
	}

	for _, field := range slices.Sorted(maps.Keys(beside)) {
		if beside[field] {
			return nil, fmt.Errorf("%w: Listeners is set beside %s", errListenersBeside, field)
		}
	}

	cfg.HTTPHost = cfg.Listeners.cfg.HTTPHost
	if cfg.HTTPHost == "" {
		cfg.HTTPHost = defaultHTTPHost
	}

	return &Sandbox{
		cfg:          cfg,
		httpScript:   httpproxy.NewScript(cfg.HTTPDefault, cfg.HTTPRoutes),
		certificates: cfg.Listeners.authority,
	}, nil
}

// Timings are the waits a sandbox's observed runs use, as the values in force: a default where nothing
// was set, never a zero.
type Timings struct {
	// Startup is how long a service may take to create its consumer.
	Startup time.Duration
	// Quiesce is how long an attribution window stays open after a handler's last word.
	Quiesce time.Duration
	// Drain is the owed-silence limit's static part: Config.Drain, or the floor Policy derives.
	Drain time.Duration
}

// Timings reports the waits this sandbox's observed runs use.
func (s *Sandbox) Timings() Timings {
	drain := s.cfg.Drain
	if drain <= 0 {
		drain = DrainFloor(s.cfg.Policy, s.cfg.Quiesce)
	}

	return Timings{Startup: s.startupLimit(), Quiesce: s.quiesce(), Drain: drain}
}

// Reset returns the service's dependencies to the starting position.
func (s *Sandbox) Reset(ctx context.Context) error {
	if s.cfg.Reset == nil {
		return nil
	}

	if err := s.cfg.Reset(ctx); err != nil {
		return fmt.Errorf("reset the sandbox: %w", err)
	}

	return nil
}

// Run replays the corpus once, with every effect the service produces observed.
func (s *Sandbox) Run(
	ctx context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	if s.cfg.Start != nil {
		return s.runObserved(ctx, name, mutation, retain)
	}

	return s.runDriven(ctx, name, mutation, retain)
}

// runDriven replays the corpus into a service Stutter dispatches to, injecting the fault itself.
func (s *Sandbox) runDriven(
	ctx context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	runner := replay.NewRunner(s.cfg.Corpus, s.cfg.HashKey, s.cfg.Policy, replay.Options{
		Retain:  retain,
		Quiesce: s.cfg.Quiesce,
	})
	recorder := runner.NewRecorder()

	observed, err := s.observe(ctx, recorder, natsproxy.Options{})
	if err != nil {
		return replay.Result{}, err
	}

	service, err := s.cfg.Connect(ctx, observed.at)
	if err != nil {
		return replay.Result{}, errors.Join(
			fmt.Errorf("connect the service under test: %w", err),
			observed.close(ctx),
			observed.httpRun.Abort(),
		)
	}

	result, runErr := runner.Run(ctx, name, mutation, service.Handle, recorder)
	if runErr != nil {
		runErr = fmt.Errorf("replay %q: %w", name, runErr)
	}

	// Ordered deliberately: the forwarding proxies wait for in-flight connections, so a service
	// holding an idle connection open would make teardown hang rather than fail.
	service.Close(ctx)

	if err := observed.settle(ctx, runErr); err != nil {
		return replay.Result{}, err
	}

	return result, nil
}

// bind is the address the proxies listen on, with a kernel-assigned port.
func (s *Sandbox) bind() string {
	host := s.cfg.BindHost
	if host == "" {
		host = loopback
	}

	return net.JoinHostPort(host, "0")
}

// advertise rewrites a listener's address into the one the service under test should dial.
//
// A proxy bound to every interface reports 0.0.0.0, which is not an address anything can connect to,
// and a container cannot reach its host's loopback under any name of its own. Both are the same
// problem: where the proxy listens and where the service dials are not always the same host.
func (s *Sandbox) advertise(addr string) string {
	if s.cfg.AdvertiseHost == "" {
		return addr
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	return net.JoinHostPort(s.cfg.AdvertiseHost, port)
}

// egress is the proxies standing in front of the service's dependencies.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type egress struct {
	httpRun *httpproxy.Run
	served  chan error
	// attached is the start's hold on the invocation listeners; nil on the Go-caller path.
	attached *attachment
	at       Addresses
	// halted is the egress-policy stop the run ended on, in the stub's own words; empty otherwise.
	halted  string
	closers []entryCloser
	// taken counts the Serve results a wait took before teardown: the proxies that stopped early.
	taken int
}

// entryCloser tears down one proxy, under the endpoint key it serves.
type entryCloser struct {
	close func(context.Context) error
	key   string
}

// start registers a proxy and begins serving it. A Serve that fails is reported under key, the
// endpoint it stands in front of, so an unreachable dependency is named rather than merely noticed.
//
// Registering as each listener opens, rather than after all of them are up, is what lets a failure
// half way through tear down the ones already running.
func (e *egress) start(
	ctx context.Context,
	key string,
	closer func(context.Context) error,
	serve func(context.Context) error,
) {
	e.closers = append(e.closers, entryCloser{close: closer, key: key})

	go func() {
		err := serve(ctx)
		if err != nil {
			err = fmt.Errorf("%s: %w", key, err)
		}

		e.served <- err
	}()
}

// settle tears the proxies down and decides the run's fate.
//
// The captured HTTP replies are frozen only when the run actually finished: a failed run, or one an
// egress stop ended, saw part of a clean run at best, and freezing that would pin later runs to answers
// the service never really settled on.
func (e *egress) settle(ctx context.Context, runErr error) error {
	closeErr := e.close(ctx)

	if runErr != nil || e.halted != "" {
		return errors.Join(runErr, closeErr, e.httpRun.Abort())
	}

	if closeErr != nil {
		return errors.Join(fmt.Errorf("close observed egress: %w", closeErr), e.httpRun.Abort())
	}

	if err := e.httpRun.Commit(); err != nil {
		return fmt.Errorf("freeze HTTP responses: %w", err)
	}

	return nil
}

// close tears the proxies down. A proxy that died mid-run would otherwise present as a handler that
// simply stopped producing effects, so its error is surfaced rather than discarded. A start on the
// invocation listeners detaches from them, within the drain bound.
//
// Only the results still to come are drained: one a wait took early already ended that wait, and is
// the run's error.
func (e *egress) close(ctx context.Context) error {
	var err error

	if e.attached != nil {
		err = e.detach(ctx)
	} else {
		for _, entry := range e.closers {
			err = errors.Join(err, entry.close(ctx))
		}
	}

	for range len(e.closers) - e.taken {
		err = errors.Join(err, <-e.served)
	}

	return err
}

// observe puts a proxy in front of each dependency, all recording into the same sink so one
// ordered effect sequence covers the whole run.
func (s *Sandbox) observe(ctx context.Context, sink *effect.Recorder, bus natsproxy.Options) (*egress, error) {
	if s.cfg.Listeners != nil {
		return s.observeRelayed(ctx, sink, bus)
	}

	observed := &egress{
		served: make(chan error, s.proxyCount()),
		at:     Addresses{Opaque: make(map[string]string, len(s.cfg.Opaque))},
	}

	fail := func(err error) (*egress, error) {
		if observed.httpRun != nil {
			err = errors.Join(err, observed.httpRun.Abort())
		}

		return nil, errors.Join(err, observed.close(ctx))
	}

	if err := s.observeBus(ctx, sink, observed, bus); err != nil {
		return fail(err)
	}

	if err := s.observeDatabase(ctx, sink, observed); err != nil {
		return fail(err)
	}

	if err := s.observeHTTP(ctx, sink, observed); err != nil {
		return fail(err)
	}

	if err := s.observeOpaque(ctx, sink, observed); err != nil {
		return fail(err)
	}

	return observed, nil
}

// proxyCount is how many Serve goroutines the run will have, and so how many results close drains.
func (s *Sandbox) proxyCount() int {
	count := alwaysProxied + len(s.cfg.Opaque)
	if s.upstream != "" {
		count++
	}

	return count
}

// observeBus proxies the bus. opts is empty for a driven run, which leaves the wire untouched in
// both directions; an observed run supplies the hooks it is watched and faulted through.
func (s *Sandbox) observeBus(
	ctx context.Context,
	sink *effect.Recorder,
	observed *egress,
	opts natsproxy.Options,
) error {
	upstream, err := hostPort(s.cfg.Corpus.URL())
	if err != nil {
		return err
	}

	bus, err := natsproxy.ListenWith(ctx, s.bind(), upstream, sink, opts)
	if err != nil {
		return fmt.Errorf("listen in front of the bus: %w", err)
	}

	observed.start(ctx, KeyBus, ignoringContext(bus.Close), bus.Serve)
	observed.at.NATS = "nats://" + s.advertise(bus.Addr())

	return nil
}

func (s *Sandbox) observeDatabase(ctx context.Context, sink *effect.Recorder, observed *egress) error {
	if s.upstream == "" {
		return nil
	}

	postgres, err := pg.Listen(ctx, s.bind(), s.upstream, sink)
	if err != nil {
		return fmt.Errorf("listen in front of the database: %w", err)
	}

	observed.start(ctx, keyDatabase, ignoringContext(postgres.Close), postgres.Serve)

	proxied, err := rewriteHost(s.cfg.PostgresDSN, s.advertise(postgres.Addr()))
	if err != nil {
		return err
	}

	observed.at.Postgres = proxied

	return nil
}

func (s *Sandbox) observeHTTP(ctx context.Context, sink *effect.Recorder, observed *egress) error {
	httpRun, err := s.httpScript.Begin()
	if err != nil {
		return fmt.Errorf("start the HTTP response script: %w", err)
	}

	observed.httpRun = httpRun

	proxy, err := s.listenStub(ctx, sink)
	if err != nil {
		return fmt.Errorf("listen for HTTP egress: %w", err)
	}

	observed.start(ctx, KeyHTTP, proxy.CloseContext, proxy.Serve)

	if s.certificates == nil {
		observed.at.HTTP = "http://" + s.advertise(proxy.Addr())

		return nil
	}

	observed.at.HTTP = "https://" + s.advertise(proxy.Addr())
	observed.at.HTTPCACert = s.certificates.pem

	return nil
}

func (s *Sandbox) listenStub(ctx context.Context, sink *effect.Recorder) (*httpproxy.Proxy, error) {
	if s.certificates == nil {
		proxy, err := httpproxy.Listen(ctx, s.bind(), s.cfg.HTTPHost, sink, s.httpScript)
		if err != nil {
			return nil, fmt.Errorf("bind the cleartext stub: %w", err)
		}

		return proxy, nil
	}

	proxy, err := httpproxy.ListenTLS(
		ctx,
		s.bind(),
		s.cfg.HTTPHost,
		sink,
		s.httpScript,
		s.certificates.leaf,
	)
	if err != nil {
		return nil, fmt.Errorf("bind the TLS stub: %w", err)
	}

	return proxy, nil
}

// observeOpaque proxies every dependency Stutter cannot parse, in name order so that teardown and
// error joining do not depend on map iteration.
func (s *Sandbox) observeOpaque(ctx context.Context, sink *effect.Recorder, observed *egress) error {
	for _, name := range slices.Sorted(maps.Keys(s.cfg.Opaque)) {
		proxy, err := opaqueproxy.Listen(ctx, s.bind(), name, s.cfg.Opaque[name], sink)
		if err != nil {
			return fmt.Errorf("listen in front of %q: %w", name, err)
		}

		observed.start(ctx, name, ignoringContext(proxy.Close), proxy.Serve)
		observed.at.Opaque[name] = s.advertise(proxy.Addr())
	}

	return nil
}

// ignoringContext adapts a proxy that closes immediately to the teardown signature. Only the HTTP
// stub needs the context, because only it waits for in-flight handlers rather than connections.
func ignoringContext(closer func() error) func(context.Context) error {
	return func(context.Context) error { return closer() }
}

// rewriteHost points a connection string at a proxy, with TLS off: an encrypted connection is
// unreadable, and the proxy refuses it rather than reporting a handler as having no side effects.
func rewriteHost(dsn, addr string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse the database address: %w", err)
	}

	parsed.Host = addr

	query := parsed.Query()
	query.Set("sslmode", "disable")
	parsed.RawQuery = query.Encode()

	return parsed.String(), nil
}

func hostPort(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse the bus address %q: %w", rawURL, err)
	}

	return parsed.Host, nil
}
