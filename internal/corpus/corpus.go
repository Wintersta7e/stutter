// Package corpus stores recorded messages as a JetStream stream inside an embedded NATS server.
//
// The corpus is a stream rather than a file format so that replay uses real JetStream consumers
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
)

// ErrDrained is returned by Replay.Next when the corpus has no further messages to deliver.
var ErrDrained = errors.New("corpus drained")

// errServerNotReady means the embedded server did not accept connections in time.
var errServerNotReady = errors.New("embedded nats server not ready for connections")

// errWrongStorage means the stream was not created with the storage type that was asked for.
//
// Embedded JetStream does not always honour the storage options it is given, and a corpus that
// silently became memory-backed would vanish on restart. Asserting is cheap; discovering it later
// is not.
var errWrongStorage = errors.New("stream storage type is not what was requested")

// Corpus is an embedded NATS server holding one stream of recorded messages.
type Corpus struct {
	server *server.Server
	conn   *nats.Conn
	stream jetstream.JetStream
}

// Start brings up an embedded JetStream server and creates the corpus stream beneath storeDir.
//
// The caller owns the returned Corpus and must Close it.
func Start(ctx context.Context, storeDir string) (*Corpus, error) {
	natsServer, err := server.NewServer(&server.Options{
		ServerName: "stutter-corpus",
		Host:       "127.0.0.1",
		Port:       server.RANDOM_PORT,
		JetStream:  true,
		StoreDir:   storeDir,
		NoLog:      true,
		NoSigs:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("create embedded nats server: %w", err)
	}

	natsServer.Start()

	if !natsServer.ReadyForConnections(startupTimeout) {
		shutdown(natsServer)

		return nil, errServerNotReady
	}

	corpus, err := connect(ctx, natsServer)
	if err != nil {
		shutdown(natsServer)

		return nil, err
	}

	return corpus, nil
}

// URL is the client URL of the embedded server, for a service under test that must reach the bus.
func (c *Corpus) URL() string {
	return c.server.ClientURL()
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

	consumer, err := c.stream.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Name:          name,
		Durable:       name,
		FilterSubject: subjectFilter,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       opts.AckWait,
		BackOff:       opts.BackOff,
		MaxDeliver:    opts.MaxDeliver,
		MaxAckPending: pending,
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

func connect(ctx context.Context, natsServer *server.Server) (*Corpus, error) {
	conn, err := nats.Connect(natsServer.ClientURL())
	if err != nil {
		return nil, fmt.Errorf("connect to embedded nats server: %w", err)
	}

	stream, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("open jetstream context: %w", err)
	}

	created, err := stream.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamName,
		Subjects:  []string{subjectFilter},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("create corpus stream: %w", err)
	}

	info, err := created.Info(ctx)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("read corpus stream info: %w", err)
	}

	if info.Config.Storage != jetstream.FileStorage {
		conn.Close()

		return nil, fmt.Errorf("%w: wanted file, got %s", errWrongStorage, info.Config.Storage)
	}

	return &Corpus{server: natsServer, conn: conn, stream: stream}, nil
}

func shutdown(natsServer *server.Server) {
	if natsServer == nil {
		return
	}

	natsServer.Shutdown()
	natsServer.WaitForShutdown()
}
