// Package harness provisions a service under test with all of its egress observed, and adapts it to
// the interface a check drives.
//
// Everything the service can talk to is proxied: the database, the bus, outbound HTTP, and any
// dependency on a protocol Stutter does not parse. Watching only the database makes the commonest
// JetStream-native idempotency guard invisible — a claim in a key/value bucket is a publish to
// $KV.<bucket>.<key> — while omitting HTTP hides API-backed guards and omitting the rest hides a
// whole datastore. A handler judged on partial evidence gets the right verdict only by luck.
//
// Container provisioning does not exist yet, so the service is supplied as a connect function. When
// a provisioner lands it implements the same shape and nothing above this package changes.
package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

// errNoConnect means the sandbox was built without a way to reach the service under test.
var errNoConnect = errors.New("no connect function: the sandbox has no service to drive")

// defaultHTTPHost is stable across runs while the listener's kernel-assigned port is not. The
// reserved .invalid suffix makes it impossible to mistake this local stub identity for a real host.
const defaultHTTPHost = "dependency.invalid"

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
	// Connect builds the service under test.
	Connect Connect
	// Reset returns every dependency to the starting position — datastore AND bus-side state. A
	// claim left in a key/value bucket corrupts the next run exactly as leftover rows would.
	Reset func(ctx context.Context) error
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
	// HashKey keys the raw-effect hash. Every run in a comparison must share one.
	HashKey []byte
	// Policy is the recorded consumer configuration.
	Policy policy.Config
	// Quiesce is how long an attribution window stays open after a handler returns.
	Quiesce time.Duration
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
	upstream     string
	cfg          Config
}

// New builds a sandbox.
func New(cfg Config) (*Sandbox, error) {
	if cfg.Connect == nil {
		return nil, errNoConnect
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
	runner := replay.NewRunner(s.cfg.Corpus, s.cfg.HashKey, s.cfg.Policy, replay.Options{
		Retain:  retain,
		Quiesce: s.cfg.Quiesce,
	})
	recorder := runner.NewRecorder()

	observed, err := s.observe(ctx, recorder)
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

	result, err := runner.Run(ctx, name, mutation, service.Handle, recorder)
	// Ordered deliberately: the forwarding proxies wait for in-flight connections, so a service
	// holding an idle connection open would make teardown hang rather than fail.
	service.Close(ctx)
	closeErr := observed.close(ctx)

	if err != nil {
		return replay.Result{}, errors.Join(
			fmt.Errorf("replay %q: %w", name, err),
			closeErr,
			observed.httpRun.Abort(),
		)
	}

	if closeErr != nil {
		return replay.Result{}, errors.Join(
			fmt.Errorf("close observed egress: %w", closeErr),
			observed.httpRun.Abort(),
		)
	}

	if err := observed.httpRun.Commit(); err != nil {
		return replay.Result{}, fmt.Errorf("freeze HTTP responses: %w", err)
	}

	return result, nil
}

// egress is the proxies standing in front of the service's dependencies.
type egress struct {
	httpRun *httpproxy.Run
	served  chan error
	at      Addresses
	closers []func(context.Context) error
}

// start registers a proxy and begins serving it.
//
// Registering as each listener opens, rather than after all of them are up, is what lets a failure
// half way through tear down the ones already running.
func (e *egress) start(
	ctx context.Context,
	closer func(context.Context) error,
	serve func(context.Context) error,
) {
	e.closers = append(e.closers, closer)

	go func() { e.served <- serve(ctx) }()
}

// close tears the proxies down. A proxy that died mid-run would otherwise present as a handler that
// simply stopped producing effects, so its error is surfaced rather than discarded.
func (e *egress) close(ctx context.Context) error {
	var err error

	for _, closer := range e.closers {
		err = errors.Join(err, closer(ctx))
	}

	for range len(e.closers) {
		err = errors.Join(err, <-e.served)
	}

	return err
}

// observe puts a proxy in front of each dependency, all recording into the same sink so one
// ordered effect sequence covers the whole run.
func (s *Sandbox) observe(ctx context.Context, sink *effect.Recorder) (*egress, error) {
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

	if err := s.observeBus(ctx, sink, observed); err != nil {
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

func (s *Sandbox) observeBus(ctx context.Context, sink *effect.Recorder, observed *egress) error {
	upstream, err := hostPort(s.cfg.Corpus.URL())
	if err != nil {
		return err
	}

	bus, err := natsproxy.Listen(ctx, "127.0.0.1:0", upstream, sink)
	if err != nil {
		return fmt.Errorf("listen in front of the bus: %w", err)
	}

	observed.start(ctx, ignoringContext(bus.Close), bus.Serve)
	observed.at.NATS = "nats://" + bus.Addr()

	return nil
}

func (s *Sandbox) observeDatabase(ctx context.Context, sink *effect.Recorder, observed *egress) error {
	if s.upstream == "" {
		return nil
	}

	postgres, err := pg.Listen(ctx, "127.0.0.1:0", s.upstream, sink)
	if err != nil {
		return fmt.Errorf("listen in front of the database: %w", err)
	}

	observed.start(ctx, ignoringContext(postgres.Close), postgres.Serve)

	proxied, err := rewriteHost(s.cfg.PostgresDSN, postgres.Addr())
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

	observed.start(ctx, proxy.CloseContext, proxy.Serve)

	if s.certificates == nil {
		observed.at.HTTP = "http://" + proxy.Addr()

		return nil
	}

	observed.at.HTTP = "https://" + proxy.Addr()
	observed.at.HTTPCACert = s.certificates.pem

	return nil
}

func (s *Sandbox) listenStub(ctx context.Context, sink *effect.Recorder) (*httpproxy.Proxy, error) {
	if s.certificates == nil {
		proxy, err := httpproxy.Listen(ctx, "127.0.0.1:0", s.cfg.HTTPHost, sink, s.httpScript)
		if err != nil {
			return nil, fmt.Errorf("bind the cleartext stub: %w", err)
		}

		return proxy, nil
	}

	proxy, err := httpproxy.ListenTLS(
		ctx,
		"127.0.0.1:0",
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
		proxy, err := opaqueproxy.Listen(ctx, "127.0.0.1:0", name, s.cfg.Opaque[name], sink)
		if err != nil {
			return fmt.Errorf("listen in front of %q: %w", name, err)
		}

		observed.start(ctx, ignoringContext(proxy.Close), proxy.Serve)
		observed.at.Opaque[name] = proxy.Addr()
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
