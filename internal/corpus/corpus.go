// Package corpus stores recorded messages as a JetStream stream inside an embedded NATS server.
//
// A corpus is handed over as a directory of files (LoadDir), but it is replayed from a stream rather
// than from those files, so that replay uses real JetStream consumers
// with real acknowledgement semantics: a duplicate is an actual server redelivery, a withheld ack
// is an actual Nak, and the ack window is an actual AckWait. The delivery semantics under test are
// the bus's own rather than Stutter's model of them, which is the whole reason the dependency is
// worth carrying.
//
// NATS types do not leave this package. Callers see Delivery and Replay, so the bus stays a
// replaceable seam rather than leaking into the driver.
package corpus

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// StreamName is the stream holding the corpus.
	StreamName = "STUTTER_CORPUS"
	// SubjectPrefix prefixes every recorded subject, keeping the corpus separable from any other
	// stream that might share the embedded server.
	SubjectPrefix = "corpus."

	subjectFilter  = SubjectPrefix + ">"
	startupTimeout = 10 * time.Second
	// loopback is the only interface the embedded server listens on.
	loopback = "127.0.0.1"
	// storeName is the store's directory inside the one a Go caller hands Start.
	storeName = "store"
	// storeMode keeps the store readable by this user alone: it holds the corpus.
	storeMode = 0o700
)

// ErrDrained is returned by Replay.Next when the corpus has no further messages to deliver.
var ErrDrained = errors.New("corpus drained")

// errServerNotReady means the embedded server did not accept connections in time.
var errServerNotReady = errors.New("embedded nats server not ready for connections")

// errStoreDir means the directory handed over for the bus store cannot be one.
var errStoreDir = errors.New("the bus store directory is unusable")

// errWrongStorage means the stream was not created with the storage type that was asked for.
//
// Embedded JetStream does not always honour the storage options it is given, and a corpus that
// silently became memory-backed would vanish on restart. Asserting is cheap; discovering it later
// is not.
var errWrongStorage = errors.New("stream storage type is not what was requested")

// Topic names the stream a corpus lives on and the subjects it holds.
//
// It is configurable because a corpus of a REAL service's traffic has to live where that service
// consumes from: a provisioned consumer subscribes to its own stream by name and filter, both
// compiled into it, and it cannot be told to read Stutter's instead.
type Topic struct {
	// Stream is the stream name.
	Stream string
	// Subjects are the subjects the stream captures and its replay consumers are bound to. Empty on a
	// corpus opened without a stream: who owns it, and so what it captures, is settled afterwards.
	Subjects []string
}

// DefaultTopic is the corpus of Stutter's own making, used wherever the traffic is synthetic.
func DefaultTopic() Topic {
	return Topic{Stream: StreamName, Subjects: []string{subjectFilter}}
}

// Corpus is an embedded NATS server and the one stream of recorded messages it is bound to.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Corpus struct {
	server *server.Server
	conn   *nats.Conn
	stream jetstream.JetStream
	// dir is the JetStream store directory the server runs from.
	dir   string
	topic Topic
}

// Topic reports which stream and subjects this corpus occupies.
func (c *Corpus) Topic() Topic {
	return c.topic
}

// Open starts an embedded JetStream server in dir, bound to stream, and creates no stream.
//
// Nothing is created because who owns the stream — a job, the service, or Stutter — is learned by
// looking before anything makes it. Every stream-scoped method still names the bound stream from the
// first call, so no caller carries a second copy of the name that could disagree with this one.
//
// dir IS the store: absent, it is created (its parent must exist); present, it must be an empty
// directory, because a store is never reused. The caller owns the returned Corpus and must Close it.
func Open(ctx context.Context, dir, stream string) (*Corpus, error) {
	if err := claimStoreDir(dir); err != nil {
		return nil, err
	}

	corpus := &Corpus{dir: dir, topic: Topic{Stream: stream}}
	if err := corpus.boot(ctx); err != nil {
		return nil, err
	}

	return corpus, nil
}

// Start brings up an embedded JetStream server and creates the corpus stream beneath storeDir.
//
// The caller owns the returned Corpus and must Close it.
func Start(ctx context.Context, storeDir string) (*Corpus, error) {
	return StartAs(ctx, storeDir, DefaultTopic())
}

// StartAs brings up a corpus on a chosen stream, for traffic belonging to a service that already
// decided where it consumes from.
//
// The store is put in a directory of its own inside parent, so a checkpoint can be taken beside it
// without being nested in the store a restore replaces.
func StartAs(ctx context.Context, parent string, topic Topic) (*Corpus, error) {
	corpus, err := Open(ctx, filepath.Join(parent, storeName), topic.Stream)
	if err != nil {
		return nil, err
	}

	corpus.topic = topic

	created, err := corpus.stream.CreateOrUpdateStream(ctx, streamConfig(topic.Stream, topic.Subjects))
	if err != nil {
		corpus.Close()

		return nil, fmt.Errorf("create corpus stream: %w", err)
	}

	info, err := created.Info(ctx)
	if err != nil {
		corpus.Close()

		return nil, fmt.Errorf("read corpus stream info: %w", err)
	}

	if info.Config.Storage != jetstream.FileStorage {
		corpus.Close()

		return nil, fmt.Errorf("%w: wanted file, got %s", errWrongStorage, info.Config.Storage)
	}

	return corpus, nil
}

// URL is the client URL of the embedded server, for a service under test that must reach the bus.
//
// Read from the running server on every call: a restart serves a fresh port, and an address kept from
// before one reaches nothing.
func (c *Corpus) URL() string {
	return c.server.ClientURL()
}

// MonitorAddr is where the embedded server serves monitoring, read from the running server on every
// call for the same reason as URL.
func (c *Corpus) MonitorAddr() netip.AddrPort {
	addr := c.server.MonitorAddr()
	if addr == nil {
		return netip.AddrPort{}
	}

	port := addr.AddrPort()

	return netip.AddrPortFrom(port.Addr().Unmap(), port.Port())
}

// StoreDir is the JetStream store directory the server runs from.
func (c *Corpus) StoreDir() string {
	return c.dir
}

// Publish appends one message to the corpus and returns its stream sequence.
func (c *Corpus) Publish(ctx context.Context, subject string, payload []byte) (uint64, error) {
	ack, err := c.stream.Publish(ctx, subject, payload)
	if err != nil {
		return 0, fmt.Errorf("publish to corpus: %w", err)
	}

	return ack.Sequence, nil
}

// ConsumerOptions mirrors a recorded consumer's configuration onto the replay consumer, so replay
// runs under the same delivery contract the corpus was recorded under.
//
// BackOff is carried through rather than flattened to AckWait: where a consumer sets one, it is the
// curve and not the declared wait that decides when the server redelivers, and replaying under the
// wrong deadline would model a fault the bus would never commit.
type ConsumerOptions struct {
	BackOff []time.Duration
	AckWait time.Duration
	// MaxDeliver caps delivery attempts. Zero means the server default.
	MaxDeliver int
	// MaxAckPending is how many messages may be outstanding. Zero or one keeps delivery serial, so
	// every effect observed while a message is in flight provably belongs to it and attribution
	// costs nothing. Anything higher is a deliberate concurrency mutation.
	MaxAckPending int
}

// Replay creates a consumer that delivers the corpus from the beginning.
func (c *Corpus) Replay(ctx context.Context, name string, opts ConsumerOptions) (*Replay, error) {
	pending := opts.MaxAckPending
	if pending <= 0 {
		pending = 1
	}

	consumer, err := c.stream.CreateOrUpdateConsumer(ctx, c.topic.Stream, jetstream.ConsumerConfig{
		Name:           name,
		Durable:        name,
		FilterSubjects: c.topic.Subjects,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		ReplayPolicy:   jetstream.ReplayInstantPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        opts.AckWait,
		BackOff:        opts.BackOff,
		MaxDeliver:     opts.MaxDeliver,
		MaxAckPending:  pending,
	})
	if err != nil {
		return nil, fmt.Errorf("create replay consumer %q: %w", name, err)
	}

	return &Replay{consumer: consumer}, nil
}

// Close shuts down the embedded server.
//
// Shutdown must be followed by WaitForShutdown or JetStream's final flush can truncate.
func (c *Corpus) Close() {
	if c.conn != nil {
		c.conn.Close()
	}

	shutdown(c.server)
}

// options is the embedded server's one configuration.
//
// Loopback only, on kernel-assigned ports read back from the running server. A nonce is always offered
// because an nkey client refuses a server that offers none, and no auth is configured, so the key it
// signs with is never checked; user/password, token and JWT clients connect either way.
func options(dir string) *server.Options {
	return &server.Options{
		ServerName:        "stutter-corpus",
		Host:              loopback,
		Port:              server.RANDOM_PORT,
		HTTPHost:          loopback,
		HTTPPort:          server.RANDOM_PORT,
		JetStream:         true,
		StoreDir:          dir,
		NoLog:             true,
		NoSigs:            true,
		AlwaysEnableNonce: true,
	}
}

// boot starts the server from the store directory and connects to it: every start of the bus, first
// or restarted, goes through here.
func (c *Corpus) boot(ctx context.Context) error {
	natsServer, err := server.NewServer(options(c.dir))
	if err != nil {
		return fmt.Errorf("create embedded nats server: %w", err)
	}

	natsServer.Start()

	if !natsServer.ReadyForConnections(startupTimeout) {
		shutdown(natsServer)

		return errServerNotReady
	}

	if err := c.connect(ctx, natsServer); err != nil {
		shutdown(natsServer)

		return err
	}

	c.server = natsServer

	return nil
}

// connect opens Stutter's own direct connection to a running server.
func (c *Corpus) connect(ctx context.Context, natsServer *server.Server) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("connect to embedded nats server: %w", err)
	}

	conn, err := nats.Connect(natsServer.ClientURL())
	if err != nil {
		return fmt.Errorf("connect to embedded nats server: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return fmt.Errorf("open jetstream context: %w", err)
	}

	c.conn, c.stream = conn, js

	return nil
}

// claimStoreDir makes dir the store's directory: created when absent, accepted when an empty
// directory, refused otherwise, each refusal naming the path.
func claimStoreDir(dir string) error {
	info, err := os.Lstat(dir)

	switch {
	case errors.Is(err, fs.ErrNotExist):
		if mkErr := os.Mkdir(dir, storeMode); mkErr != nil {
			return fmt.Errorf("create the bus store directory: %w", mkErr)
		}

		return nil
	case err != nil:
		return fmt.Errorf("read the bus store directory: %w", err)
	case !info.IsDir():
		return fmt.Errorf("%w: %s is not a directory", errStoreDir, dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list the bus store directory: %w", err)
	}

	if len(entries) != 0 {
		return fmt.Errorf("%w: %s already holds %d entries; a store is never reused", errStoreDir, dir, len(entries))
	}

	return nil
}

func shutdown(natsServer *server.Server) {
	if natsServer == nil {
		return
	}

	natsServer.Shutdown()
	natsServer.WaitForShutdown()
}
