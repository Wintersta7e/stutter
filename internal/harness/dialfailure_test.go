package harness_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// closedPorts hands out this package's closed ports, each once, from [65400, 65500): above the kernel's
// source-port range, so no socket another test opens can take one before a proxy dials it.
var closedPorts atomic.Uint32

// closedPort is an address nothing listens on.
func closedPort(t *testing.T) netip.AddrPort {
	t.Helper()

	var config net.ListenConfig

	for offset := closedPorts.Add(1); offset < 100; offset = closedPorts.Add(1) {
		addr := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(65400+offset))

		listener, err := config.Listen(t.Context(), "tcp4", addr.String())
		if err == nil {
			_ = listener.Close()

			return addr
		}
	}

	t.Fatal("no free port in [65400, 65500)")

	return netip.AddrPort{}
}

// tolerant is a service that consumes for itself and shrugs off a dependency it cannot reach: it dials
// the dependency once, whatever happens, then acknowledges every message it is handed. It is the
// service a proxy's silent dial failure would pass off as a handler that did nothing.
type tolerant struct {
	connection *nats.Conn
	consumer   jetstream.Consumer
	done       chan struct{}
	stopped    chan struct{}
}

func startTolerant(ctx context.Context, dependency, bus string, config policy.Config) (*tolerant, error) {
	var dialer net.Dialer

	// The dependency is unreachable behind its proxy; the service carries on regardless.
	if conn, err := dialer.DialContext(ctx, "tcp", dependency); err == nil {
		_, _ = conn.Write([]byte("hello\n")) //nolint:errcheck // a refused dependency is this service's premise.
		_ = conn.Close()
	}

	connection, err := nats.Connect(bus)
	if err != nil {
		return nil, fmt.Errorf("connect to the bus: %w", err)
	}

	stream, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf("open jetstream: %w", err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, corpus.StreamName, jetstream.ConsumerConfig{
		Name:          observedConsumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       config.AckWait,
		MaxDeliver:    config.MaxDeliver,
		MaxAckPending: config.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("create the consumer: %w", err)
	}

	service := &tolerant{
		connection: connection,
		consumer:   consumer,
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}

	go service.pump()

	return service, nil
}

func (s *tolerant) Close(context.Context) (replay.Exit, error) {
	close(s.done)
	<-s.stopped
	s.connection.Close()

	return replay.Exit{}, nil
}

func (*tolerant) Exited() <-chan struct{} { return nil }

func (s *tolerant) pump() {
	defer close(s.stopped)

	for {
		select {
		case <-s.done:
			return
		default:
		}

		batch, err := s.consumer.Fetch(1, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			continue
		}

		for msg := range batch.Messages() {
			_ = msg.Ack() //nolint:errcheck // a lost ack is a redelivery, which the run observes anyway.
		}
	}
}

// tolerantSandbox is an observed sandbox around a tolerant service, with one message in the corpus.
// dependency picks, from the proxied addresses, the one the service dials.
func tolerantSandbox(
	t *testing.T,
	tune func(*harness.Config),
	dependency func(harness.Addresses) string,
) *harness.Sandbox {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	if _, publishErr := store.Publish(
		t.Context(), toy.SubjectOrderCreated, orderPayload("ORD-DIAL-1", "WIDGET"),
	); publishErr != nil {
		t.Fatalf("Publish() error = %v", publishErr)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	config := observedConfig()
	settings := harness.Config{
		Corpus:  store,
		HashKey: key,
		Policy:  config,
		Quiesce: toy.DefaultQuiesce,
		Startup: 5 * time.Second,
		Start: func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			return startTolerant(ctx, dependency(at), at.NATS, config)
		},
	}

	tune(&settings)

	built, err := harness.New(settings)
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built
}

// TestADialFailureNamesTheEndpoint is the run error a user reads when a dependency is unreachable: it
// names which dependency, and where it was looked for. A run that carried on would report a handler
// that did nothing.
func TestADialFailureNamesTheEndpoint(t *testing.T) {
	t.Parallel()

	unreachable := closedPort(t)

	built := tolerantSandbox(t,
		func(settings *harness.Config) {
			settings.Opaque = map[string]string{opaqueCache: unreachable.String()}
		},
		func(at harness.Addresses) string { return at.Opaque[opaqueCache] },
	)

	result, err := built.Run(t.Context(), "clean", replay.Clean{}, nil)
	if err == nil {
		t.Fatalf("run err = <nil>, want an error naming %q (effects: %d)", opaqueCache, len(result.Effects))
	}

	if !strings.Contains(err.Error(), opaqueCache+":") {
		t.Errorf("run err does not name %q: %v", opaqueCache, err)
	}

	if !strings.Contains(err.Error(), unreachable.String()) {
		t.Errorf("run err does not name %s: %v", unreachable, err)
	}

	if len(result.Effects) != 0 {
		t.Errorf("the failed run reported %d effects, want none", len(result.Effects))
	}
}

// TestADialFailureOnThePostgresPathNamesTheDatabase is the same rule for the database proxy.
func TestADialFailureOnThePostgresPathNamesTheDatabase(t *testing.T) {
	t.Parallel()

	unreachable := closedPort(t)

	built := tolerantSandbox(t,
		func(settings *harness.Config) {
			settings.PostgresDSN = "postgres://stutter:stutter@" + unreachable.String() + "/stutter"
		},
		func(at harness.Addresses) string {
			proxied, err := url.Parse(at.Postgres)
			if err != nil {
				return ""
			}

			return proxied.Host
		},
	)

	_, err := built.Run(t.Context(), "clean", replay.Clean{}, nil)
	if err == nil {
		t.Fatal("run err = <nil>, want an error naming the database")
	}

	if !strings.Contains(err.Error(), "database:") || !strings.Contains(err.Error(), unreachable.String()) {
		t.Errorf("run err does not name the database at %s: %v", unreachable, err)
	}
}
