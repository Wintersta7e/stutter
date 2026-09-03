// Package harness provisions a service under test with all of its egress observed, and adapts it to
// the interface a check drives.
//
// Both dependencies are proxied: the database, and the bus itself. Watching only the database makes
// the commonest JetStream-native idempotency guard invisible — a claim in a key/value bucket is a
// publish to $KV.<bucket>.<key> — and a handler judged on partial evidence gets the right verdict
// only by luck.
//
// Container provisioning does not exist yet, so the service is supplied as a connect function. When
// a provisioner lands it implements the same shape and nothing above this package changes.
package harness

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/policy"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// errNoConnect means the sandbox was built without a way to reach the service under test.
var errNoConnect = errors.New("no connect function: the sandbox has no service to drive")

// Service is the service under test, already connected to the proxied dependencies.
type Service interface {
	// Handle processes one delivered message.
	Handle(ctx context.Context, msg replay.Message) error
	// Close releases the service's connections. It runs BEFORE the proxies are closed.
	Close(ctx context.Context)
}

// Connect builds the service against proxied addresses.
//
// Both arguments point at proxies, never at the real dependency. A service that connects directly
// produces no observable effects, which reads as a handler that did nothing at all.
type Connect func(ctx context.Context, postgresDSN, natsURL string) (Service, error)

// Config describes the sandbox.
type Config struct {
	// Corpus is the recorded traffic and the bus the service talks to.
	Corpus *corpus.Corpus
	// Connect builds the service under test.
	Connect Connect
	// Reset returns every dependency to the starting position — datastore AND bus-side state. A
	// claim left in a key/value bucket corrupts the next run exactly as leftover rows would.
	Reset func(ctx context.Context) error
	// PostgresDSN points at the REAL database. The sandbox rewrites its host to the proxy.
	PostgresDSN string
	// HashKey keys the raw-effect hash. Every run in a comparison must share one.
	HashKey []byte
	// Policy is the recorded consumer configuration.
	Policy policy.Config
	// Quiesce is how long an attribution window stays open after a handler returns.
	Quiesce time.Duration
}

// Sandbox provisions one service under test. It satisfies the interface a check drives.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Sandbox struct {
	upstream string
	cfg      Config
}

// New builds a sandbox.
func New(cfg Config) (*Sandbox, error) {
	if cfg.Connect == nil {
		return nil, errNoConnect
	}

	parsed, err := url.Parse(cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("parse the database address: %w", err)
	}

	return &Sandbox{cfg: cfg, upstream: parsed.Host}, nil
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

	defer observed.close()

	service, err := s.cfg.Connect(ctx, observed.postgresDSN, observed.natsURL)
	if err != nil {
		return replay.Result{}, fmt.Errorf("connect the service under test: %w", err)
	}

	// Ordered deliberately: both proxies' Close wait for in-flight connections, so a service
	// holding an idle connection open would make teardown hang rather than fail.
	defer service.Close(ctx)

	result, err := runner.Run(ctx, name, mutation, service.Handle, recorder)
	if err != nil {
		return replay.Result{}, fmt.Errorf("replay %q: %w", name, err)
	}

	return result, nil
}

// egress is the pair of proxies standing in front of the service's dependencies.
type egress struct {
	postgres    *pg.Proxy
	bus         *natsproxy.Proxy
	served      chan error
	postgresDSN string
	natsURL     string
}

// observe puts a proxy in front of each dependency, both recording into the same sink so one
// ordered effect sequence covers the whole run.
func (s *Sandbox) observe(ctx context.Context, sink *effect.Recorder) (*egress, error) {
	postgres, err := pg.Listen(ctx, "127.0.0.1:0", s.upstream, sink)
	if err != nil {
		return nil, fmt.Errorf("listen in front of the database: %w", err)
	}

	busUpstream, err := hostPort(s.cfg.Corpus.URL())
	if err != nil {
		_ = postgres.Close()

		return nil, err
	}

	bus, err := natsproxy.Listen(ctx, "127.0.0.1:0", busUpstream, sink)
	if err != nil {
		_ = postgres.Close()

		return nil, fmt.Errorf("listen in front of the bus: %w", err)
	}

	proxied, err := rewriteHost(s.cfg.PostgresDSN, postgres.Addr())
	if err != nil {
		_ = postgres.Close()
		_ = bus.Close()

		return nil, err
	}

	observed := &egress{
		postgres:    postgres,
		bus:         bus,
		served:      make(chan error, 2), //nolint:mnd // one slot per proxy.
		postgresDSN: proxied,
		natsURL:     "nats://" + bus.Addr(),
	}

	go func() { observed.served <- postgres.Serve(ctx) }()
	go func() { observed.served <- bus.Serve(ctx) }()

	return observed, nil
}

// close tears the proxies down. A proxy that died mid-run would otherwise present as a handler that
// simply stopped producing effects, so its error is surfaced rather than discarded.
func (e *egress) close() {
	_ = e.postgres.Close()
	_ = e.bus.Close()
	<-e.served
	<-e.served
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
