package harness_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// observedConsumer is the consumer the service under test creates for itself. It is a fixed name on
// purpose: a real service's consumer is part of its configuration, not something Stutter picks per
// run, and rebuilding the stream is what stops a second run resuming where the first stopped.
const observedConsumer = "reserve_stock_wire"

// observedAckWait is the deadline a withheld acknowledgement waits out before the bus redelivers.
// The drain wait is deliberately left to the sandbox to derive from it, because that derivation is
// what a real caller gets and a hand-picked number here would never exercise it.
const observedAckWait = 500 * time.Millisecond

// fetchWait bounds one pull. It is short so the service stops promptly when the run is over, and it
// is what makes the service send a pull request per message rather than one for the whole run.
//
// It is not shorter because a pull's expiry bounds the Fill hold at a tenth of it: at 100 ms every
// run's hold had 10 ms, against a measured 2.2 to 3.0 ms to publish three messages under the race
// detector.
const fetchWait = 500 * time.Millisecond

// pulling is a service Stutter does not dispatch to. It creates its own JetStream consumer, fetches
// its own messages and acknowledges them on its own connection — the shape a provisioned container
// has, reduced to the parts that decide whether Stutter can still fault it.
//
// Its one side effect is a line to a dependency on a protocol Stutter does not parse, so the run is
// observed entirely through the proxies and needs no database.
type pulling struct {
	connection *nats.Conn
	consumer   jetstream.Consumer
	dependency net.Conn
	replies    *bufio.Reader
	// seeded is the job-seeded bucket the handler writes to, under the seededKey quirk.
	seeded jetstream.KeyValue
	// stream is the service's JetStream context, and side its consumer on its own stream.
	stream  jetstream.JetStream
	side    jetstream.Consumer
	done    chan struct{}
	stopped chan struct{}
	quirks  quirks
}

// quirks are the ways a pulling service departs from the plain one, each for the test that needs it.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type quirks struct {
	// seen is where a starting service notes what it found. Shared by every run of a sandbox.
	seen *startups
	// filter is the one subject the service's consumer admits. Empty admits the whole stream.
	filter string
	// nakFor refuses each message's first delivery with a NAK asking for redelivery after this long.
	// Zero never does.
	nakFor time.Duration
	// lateBy holds the whole service back this long after it is started, so its consumer appears
	// after the startup limit. Zero starts it at once.
	lateBy time.Duration
	// idempotent reserves only on a message's first delivery, so no fault makes it diverge and a
	// check spends no runs shrinking.
	idempotent bool
	// echo publishes a note of every order it reserves into the stream it consumes, so the bus hands
	// the service its own output at a sequence Stutter never staged.
	echo bool
	// echoWrites makes the service write to its dependency while handling its own note, too.
	echoWrites bool
	// tlsFirst makes the service also open a raw connection to the bus and start a TLS handshake on
	// it, as a client configured for TLS does.
	tlsFirst bool
	// bucket makes the service look its own key/value bucket up at startup, noting whether it found
	// one, and then create it and write to it.
	bucket bool
	// seededKey makes the service read a job-seeded key at startup, noting its revision, and makes
	// the handler write that key.
	seededKey bool
	// audit makes the handler publish a note of every delivery into the stream it consumes, on a
	// subject its own filter does not admit.
	audit bool
	// pullExpires is how long each pull lives. Zero is fetchWait.
	pullExpires time.Duration
	// coreSubscribe makes the service also subscribe to the order subject with a core subscription,
	// beside its consumer.
	coreSubscribe bool
	// sideStream makes the service keep a stream of its own, and for every order publish a note into
	// it and fetch one message from it while handling the order.
	sideStream bool
	// ephemeral makes the service create its consumer with no name, so the client names it afresh on
	// every start.
	ephemeral bool
	// conflictingStream makes the service try, at startup, to create the corpus stream over other
	// subjects — which the bus refuses — and carry on.
	conflictingStream bool
}

// The service's own stream, under the sideStream quirk.
const (
	sideStream   = "SIDE"
	sideConsumer = "side"
	sideSubject  = "side.note"
)

// auditSubject is where an auditing service notes each delivery: inside the stream it consumes, and
// outside its own filter, so the note takes a stream sequence without ever being delivered to it.
const auditSubject = corpus.SubjectPrefix + "order.audited"

// Buckets a quirky service keeps state in.
const (
	// serviceBucket is the bucket the service creates for itself at startup.
	serviceBucket = "svc-state"
	// seededBucket and seededKey are what a job writes before the service ever starts.
	seededBucket = "seeded"
	seededKey    = "k"
)

// startups records what a service found each time it started, across the runs of one sandbox.
type startups struct {
	found     []bool
	revisions []uint64
	consumers []string
	mu        sync.Mutex
}

// consumer notes the name a starting service's consumer was given.
func (s *startups) consumer(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.consumers = append(s.consumers, name)
}

// consumerNames reports every start's consumer name, in order.
func (s *startups) consumerNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.consumers)
}

// bucketFound notes whether a starting service found its own bucket already there.
func (s *startups) bucketFound(found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.found = append(s.found, found)
}

// revision notes the revision a starting service read the seeded key at.
func (s *startups) revision(revision uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.revisions = append(s.revisions, revision)
}

// buckets and seededRevisions report what every start found, in order.
func (s *startups) buckets() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.found)
}

func (s *startups) seededRevisions() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.revisions)
}

// echoSubject is where an echoing service notes each order: inside the corpus subjects, so its own
// consumer is handed the note.
const echoSubject = corpus.SubjectPrefix + "order.echoed"

// echoNote is the note an echoing service publishes for an order.
type echoNote struct {
	EchoOf string `json:"echo_of"`
}

// latecomer is a service that comes up only after a delay: it connects and creates its consumer once
// the startup limit has passed and the corpus has already been published.
type latecomer struct {
	service *pulling
	done    chan struct{}
	settled chan struct{}
}

// startLate starts a pulling service after a delay, in the background, as a slow container does.
func startLate(ctx context.Context, at harness.Addresses, config policy.Config, after time.Duration) *latecomer {
	late := &latecomer{done: make(chan struct{}), settled: make(chan struct{})}

	go func() {
		defer close(late.settled)

		select {
		case <-late.done:
			return
		case <-time.After(after):
		}

		//nolint:errcheck // a service that fails to come up is one the run never sees, which is the
		// case the test's own assertion reports.
		late.service, _ = startPulling(ctx, at, config, quirks{})
	}()

	return late
}

// Close stops the service if it ever came up, after waiting for it to finish coming up.
func (l *latecomer) Close(ctx context.Context) {
	close(l.done)
	<-l.settled

	if l.service != nil {
		l.service.Close(ctx)
	}
}

// startPulling connects the service to the proxied bus and lets it start consuming.
//
// Every address it dials is a proxy's. A service that reached a real dependency directly would
// produce no observable effects at all, which reads as a handler that did nothing.
func startPulling(
	ctx context.Context,
	at harness.Addresses,
	config policy.Config,
	behaviour quirks,
) (*pulling, error) {
	dialer := net.Dialer{Timeout: time.Second}

	dependency, err := dialer.DialContext(ctx, "tcp", at.Opaque[opaqueCache])
	if err != nil {
		return nil, fmt.Errorf("dial the unparsed dependency: %w", err)
	}

	service := &pulling{
		dependency: dependency,
		replies:    bufio.NewReader(dependency),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
		quirks:     behaviour,
	}

	service.connection, err = nats.Connect(at.NATS)
	if err != nil {
		return nil, fmt.Errorf("connect to the bus: %w", err)
	}

	stream, err := jetstream.New(service.connection)
	if err != nil {
		return nil, fmt.Errorf("open jetstream: %w", err)
	}

	if startupErr := service.startup(ctx, stream); startupErr != nil {
		return nil, startupErr
	}

	name := observedConsumer
	if behaviour.ephemeral {
		name = ""
	}

	service.consumer, err = stream.CreateOrUpdateConsumer(ctx, corpus.StreamName, jetstream.ConsumerConfig{
		Name:          name,
		FilterSubject: behaviour.filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       config.AckWait,
		MaxDeliver:    config.MaxDeliver,
		MaxAckPending: config.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("create the consumer: %w", err)
	}

	if behaviour.seen != nil {
		behaviour.seen.consumer(service.consumer.CachedInfo().Name)
	}

	if behaviour.coreSubscribe {
		if _, err := service.connection.Subscribe(toy.SubjectOrderCreated, func(*nats.Msg) {}); err != nil {
			return nil, fmt.Errorf("subscribe to the order subject: %w", err)
		}
	}

	if behaviour.tlsFirst {
		if err := handshake(ctx, at.NATS); err != nil {
			return nil, err
		}
	}

	go service.pump(ctx)

	return service, nil
}

// handshake opens a raw connection to the bus, reads its greeting and answers with the opening bytes
// of a TLS handshake, where a CONNECT would otherwise be.
func handshake(ctx context.Context, address string) error {
	bus, err := url.Parse(address)
	if err != nil {
		return fmt.Errorf("parse the bus address: %w", err)
	}

	dialer := net.Dialer{Timeout: time.Second}

	conn, err := dialer.DialContext(ctx, "tcp", bus.Host)
	if err != nil {
		return fmt.Errorf("dial the bus: %w", err)
	}

	defer func() { _ = conn.Close() }()

	if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
		return fmt.Errorf("read the bus greeting: %w", err)
	}

	if _, err := conn.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x01}); err != nil {
		return fmt.Errorf("start a TLS handshake: %w", err)
	}

	return nil
}

// Close stops pulling and waits for the pump before the connections go, so the proxies are not torn
// down underneath a delivery still being worked on.
func (p *pulling) Close(context.Context) {
	close(p.done)
	<-p.stopped

	p.connection.Close()

	_ = p.dependency.Close()
}

// startup is the service's own work before it consumes: its bucket looked up and made, the seeded
// key read.
func (p *pulling) startup(ctx context.Context, stream jetstream.JetStream) error {
	if p.quirks.bucket {
		_, err := stream.KeyValue(ctx, serviceBucket)
		if err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
			return fmt.Errorf("look the service bucket up: %w", err)
		}

		p.quirks.seen.bucketFound(err == nil)

		own, err := stream.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: serviceBucket})
		if err != nil {
			return fmt.Errorf("create the service bucket: %w", err)
		}

		if _, err := own.Put(ctx, "state", []byte("started")); err != nil {
			return fmt.Errorf("write the service bucket: %w", err)
		}
	}

	p.stream = stream

	if p.quirks.conflictingStream {
		// Refused, as a service's own create is when the stream exists with other subjects; the
		// service carries on regardless, as one that only logs the error does.
		//nolint:errcheck // the refusal is the point, and the bus's answer is what the test reads.
		_, _ = stream.CreateStream(ctx, jetstream.StreamConfig{Name: corpus.StreamName, Subjects: []string{"other.>"}})
	}

	if p.quirks.sideStream {
		if _, err := stream.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     sideStream,
			Subjects: []string{"side.>"},
		}); err != nil {
			return fmt.Errorf("create the side stream: %w", err)
		}

		side, err := stream.CreateOrUpdateConsumer(ctx, sideStream, jetstream.ConsumerConfig{
			Durable:   sideConsumer,
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		if err != nil {
			return fmt.Errorf("create the side consumer: %w", err)
		}

		p.side = side
	}

	if p.quirks.seededKey {
		seeded, err := stream.KeyValue(ctx, seededBucket)
		if err != nil {
			return fmt.Errorf("open the seeded bucket: %w", err)
		}

		entry, err := seeded.Get(ctx, seededKey)
		if err != nil {
			return fmt.Errorf("read the seeded key: %w", err)
		}

		p.quirks.seen.revision(entry.Revision())
		p.seeded = seeded
	}

	return nil
}

// pump pulls one message at a time for as long as the run lasts, which is what a real pull consumer
// does and what makes a redelivery arrive on its own rather than being handed over.
func (p *pulling) pump(ctx context.Context) {
	defer close(p.stopped)

	for {
		select {
		case <-p.done:
			return
		default:
		}

		expires := fetchWait
		if p.quirks.pullExpires > 0 {
			expires = p.quirks.pullExpires
		}

		batch, err := p.consumer.Fetch(1, jetstream.FetchMaxWait(expires))
		if err != nil {
			return
		}

		for msg := range batch.Messages() {
			p.handle(ctx, msg)
		}
	}
}

// handle reserves stock once per delivery, which is the planted bug: the bus is permitted to deliver
// the same message twice, and this service reserves twice when it does.
func (p *pulling) handle(ctx context.Context, msg jetstream.Msg) {
	if p.quirks.nakFor > 0 && firstDelivery(msg) {
		//nolint:errcheck // as below: a settle that fails is the run ending underneath the service.
		_ = msg.NakWithDelay(p.quirks.nakFor)

		return
	}

	settle := msg.Ack

	var note echoNote

	switch {
	case msg.Subject() == echoSubject:
		if json.Unmarshal(msg.Data(), &note) != nil || (p.quirks.echoWrites && p.call("ECHO "+note.EchoOf) != nil) {
			settle = msg.Nak
		}
	case !p.quirks.idempotent || firstDelivery(msg):
		if err := p.reserve(msg.Data()); err != nil {
			settle = msg.Nak
		}

		if p.seeded != nil {
			if _, err := p.seeded.Put(ctx, seededKey, msg.Data()); err != nil {
				settle = msg.Nak
			}
		}

		if p.quirks.audit && p.connection.Publish(auditSubject, msg.Data()) != nil {
			settle = msg.Nak
		}

		if p.side != nil && p.noteAside(ctx, msg.Data()) != nil {
			settle = msg.Nak
		}

		if p.quirks.echo {
			published, err := json.Marshal(echoNote{EchoOf: string(msg.Data())})
			if err != nil || p.connection.Publish(echoSubject, published) != nil {
				settle = msg.Nak
			}
		}
	default:
	}

	//nolint:errcheck // a settle that fails is the run ending underneath the service, and the proxy
	// reports that; retrying it here would add a delivery the run never asked for.
	_ = settle()
}

// noteAside publishes a note into the service's own stream and takes it straight back off it: bus work
// on a stream other than the one under test, done while handling a message.
func (p *pulling) noteAside(ctx context.Context, note []byte) error {
	if _, err := p.stream.Publish(ctx, sideSubject, note); err != nil {
		return fmt.Errorf("note aside: %w", err)
	}

	batch, err := p.side.Fetch(1, jetstream.FetchMaxWait(fetchWait))
	if err != nil {
		return fmt.Errorf("fetch the note back: %w", err)
	}

	for msg := range batch.Messages() {
		if err := msg.Ack(); err != nil {
			return fmt.Errorf("settle the note: %w", err)
		}
	}

	return nil
}

// call sends one line to the dependency and waits for its answer.
func (p *pulling) call(line string) error {
	if _, err := fmt.Fprintln(p.dependency, line); err != nil {
		return fmt.Errorf("write %q: %w", line, err)
	}

	if _, err := p.replies.ReadString('\n'); err != nil {
		return fmt.Errorf("read the answer to %q: %w", line, err)
	}

	return nil
}

// firstDelivery reports whether the bus is handing this message over for the first time.
func firstDelivery(msg jetstream.Msg) bool {
	metadata, err := msg.Metadata()

	return err == nil && metadata.NumDelivered == 1
}

func (p *pulling) reserve(payload []byte) error {
	var order struct {
		OrderID string `json:"order_id"`
		Qty     int    `json:"qty"`
	}

	if err := json.Unmarshal(payload, &order); err != nil {
		return fmt.Errorf("decode the order: %w", err)
	}

	if _, err := fmt.Fprintf(p.dependency, "RESERVE %s %d\n", order.OrderID, order.Qty); err != nil {
		return fmt.Errorf("reserve stock: %w", err)
	}

	if _, err := p.replies.ReadString('\n'); err != nil {
		return fmt.Errorf("read the reservation: %w", err)
	}

	return nil
}

// observedSandbox builds a sandbox around a service that consumes for itself, and stocks the corpus
// with one message per order id.
func observedSandbox(
	t *testing.T,
	config policy.Config,
	orders ...string,
) (*harness.Sandbox, []uint64) {
	t.Helper()

	return quirkySandbox(t, config, quirks{}, nil, orders...)
}

// quirkySandbox is observedSandbox around a service with quirks, with the harness configuration open to
// the test through tune.
func quirkySandbox(
	t *testing.T,
	config policy.Config,
	behaviour quirks,
	tune func(*harness.Config),
	orders ...string,
) (*harness.Sandbox, []uint64) {
	t.Helper()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	recorded := make([]uint64, 0, len(orders))

	for _, order := range orders {
		seq, publishErr := store.Publish(
			t.Context(),
			toy.SubjectOrderCreated,
			orderPayload(order, "WIDGET-WIRE"),
		)
		if publishErr != nil {
			t.Fatalf("Publish() error = %v", publishErr)
		}

		recorded = append(recorded, seq)
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	upstream := startLineDependency(t)

	settings := harness.Config{
		Corpus:  store,
		Opaque:  map[string]string{opaqueCache: upstream},
		HashKey: key,
		Policy:  config,
		Quiesce: toy.DefaultQuiesce,
		Start: func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			if behaviour.lateBy > 0 {
				return startLate(ctx, at, config, behaviour.lateBy), nil
			}

			service, startErr := startPulling(ctx, at, config, behaviour)
			if startErr != nil {
				return nil, startErr
			}

			return service, nil
		},
	}

	if tune != nil {
		tune(&settings)
	}

	built, err := harness.New(settings)
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built, recorded
}

// observedConfig is the recorded consumer's own configuration. In this run model it is also the
// service's: Stutter no longer creates the consumer, so the two have to agree or the faults the
// legality table licenses are not the ones the bus will actually commit.
func observedConfig() policy.Config {
	return policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{filterAll},
		AckWait:        observedAckWait,
		MaxDeliver:     3,
		MaxAckPending:  1,
	}
}

// TestObservedServiceIsFaultedOnTheWire is the run model a provisioned container needs: Stutter
// dispatches nothing and has no acknowledgement of its own to withhold, so the delivery is watched
// and the fault injected by swallowing the service's own acknowledgement on the wire.
//
// It needs no database and no container — only a service that consumes for itself.
func TestObservedServiceIsFaultedOnTheWire(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := observedSandbox(t, config, "ORD-WIRE-1")

	session := &counting{inner: built}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: recorded,
		Consumer: observedConsumer,
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if session.faults == 0 {
		t.Fatal("no fault was injected, so the finding proves nothing")
	}

	if len(result.Gates) == 0 || len(result.Violations()) != 0 {
		t.Fatalf("the gates did not hold for an undriven service:\n%s", result)
	}

	if len(result.Findings) != 1 {
		t.Fatalf("Findings = %d, want 1\n%s", len(result.Findings), result)
	}

	found := result.Findings[0]
	if found.Status != report.StatusWarn {
		t.Errorf("Status = %q, want WARN (reservations: %v)", found.Status, found.Reservations)
	}

	if !strings.Contains(result.String(), "opaque dependency=cache") {
		t.Errorf("report did not name the unparsed dependency:\n%s", result)
	}
}

// TestAWholeCorpusRunFollowsAScopedOne is the property staging would destroy if the corpus were
// re-read from the stream each run.
//
// A check interleaves whole-corpus runs with the subsets a shrink asks for, and staging republishes
// only the messages it was handed — so a caller that read the stream back would find the corpus
// already reduced to the last subset it staged, and every run after the first shrink would compare
// against a corpus that was never delivered.
func TestAWholeCorpusRunFollowsAScopedOne(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := observedSandbox(t, config, "ORD-WIRE-1", "ORD-WIRE-2")

	scoped, err := built.Run(t.Context(), "scoped", replay.Clean{}, []uint64{recorded[1]})
	if err != nil {
		t.Fatalf("Run() scoped error = %v", err)
	}

	if scoped.Delivered != 1 {
		t.Fatalf("the scoped run delivered %d messages, want 1", scoped.Delivered)
	}

	whole, err := built.Run(t.Context(), "whole", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() over the whole corpus after a scoped one: %v", err)
	}

	if whole.Delivered != len(recorded) {
		t.Fatalf("the whole-corpus run delivered %d messages, want %d — the scoped run took the rest "+
			"of the corpus with it", whole.Delivered, len(recorded))
	}
}

// TestAScopedObservedRunFaultsTheRecordedSequence covers the translation a rebuilt stream forces.
//
// Scoping an observed run means restaging the corpus with only the retained messages, and staging
// renumbers from one. A fault built against a recorded sequence would otherwise be aimed at a
// message that is no longer there — which a shrink would read as "this subset does not reproduce",
// leaving every finding without a minimal repro.
func TestAScopedObservedRunFaultsTheRecordedSequence(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := observedSandbox(t, config, "ORD-WIRE-1", "ORD-WIRE-2")

	second := recorded[1]
	if second == 1 {
		t.Fatal("the second message was recorded at sequence 1, so staging cannot renumber it")
	}

	result, err := built.Run(t.Context(), "scoped", replay.Duplicate{Seq: second}, []uint64{second})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Delivered != 2 {
		t.Fatalf("Delivered = %d, want 2 — the duplicate never landed on the retained message",
			result.Delivered)
	}

	if len(result.Effects) != 2 {
		t.Fatalf("Effects = %d, want 2 reservations\n%v", len(result.Effects), result.Effects)
	}

	// A pull consumer asks for its next message for as long as the run lasts, so anything of its own
	// delivery machinery that reached the sequence would arrive after the last window closed. A
	// non-zero count here is the sequence filling up with traffic the handler never chose to send.
	if result.Late != 0 {
		t.Errorf("Late = %d, want 0 — the run recorded delivery bookkeeping as the handler's work",
			result.Late)
	}

	for at, observed := range result.Effects {
		if observed.MessageSeq != second {
			t.Errorf("effect %d attributed to message %d, want the recorded sequence %d",
				at, observed.MessageSeq, second)
		}

		if !strings.Contains(observed.Printable, "ORD-WIRE-2") {
			t.Errorf("effect %d = %q, want the retained message's order", at, observed.Printable)
		}
	}
}

// TestAnObservedRunCountsADelayedNak: a service asking for redelivery later sends `-NAK {"delay": …}`,
// which refuses the delivery as surely as a bare NAK. Read as an acknowledgement, the refusal went
// uncounted and the clean run's health claimed a service that refused nothing.
func TestAnObservedRunCountsADelayedNak(t *testing.T) {
	t.Parallel()

	// Well inside the drain the sandbox derives, so the redelivery lands before the run ends.
	const nakFor = 200 * time.Millisecond

	built, _ := quirkySandbox(t, observedConfig(), quirks{nakFor: nakFor}, nil, "ORD-NAK-1")

	result, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Failed != 1 {
		t.Errorf("Failed = %d, want 1 — the delayed NAK was read as a settled delivery", result.Failed)
	}

	// The run went on to the redelivery the NAK asked for, rather than ending on the refused message.
	if result.Delivered != 2 || len(result.Effects) != 1 {
		t.Errorf("Delivered = %d, Effects = %d, want 2 deliveries and the one reservation of the second",
			result.Delivered, len(result.Effects))
	}
}

// TestAFedBackDeliveryIsKeptOutOfTheComparison: a service that publishes into the stream it consumes is
// handed its own output at sequences Stutter never staged. Handled without doing anything, it is
// counted and left out. Handled with work, the run stops: that work belongs to no corpus message, and
// the fallback that attributed it to one compared the service's echo as if it were the recording.
func TestAFedBackDeliveryIsKeptOutOfTheComparison(t *testing.T) {
	t.Parallel()

	const echoed = "ORD-ECHO-1"

	cases := []struct {
		mutation func(recorded []uint64) replay.Mutation
		name     string
		stops    string
		orders   []string
		// retain picks the recorded messages the run keeps, by position; nil keeps them all.
		retain    []int
		behaviour quirks
		delivered int
		fedBack   int
	}{
		{
			name:      "handled without work",
			mutation:  func([]uint64) replay.Mutation { return replay.Clean{} },
			orders:    []string{echoed},
			behaviour: quirks{echo: true},
			delivered: 2,
			fedBack:   1,
		},
		{
			name:      "handled with work",
			mutation:  func([]uint64) replay.Mutation { return replay.Clean{} },
			orders:    []string{echoed},
			behaviour: quirks{echo: true, echoWrites: true},
			stops:     "stream sequence 2",
		},
		{
			// Recorded message 2 is staged alone, at sequence 1, so the echo lands at sequence 2 — the
			// recorded sequence the duplicate is aimed at. Its acknowledgement must still go through.
			name:      "numbered like the faulted message",
			mutation:  func(recorded []uint64) replay.Mutation { return replay.Duplicate{Seq: recorded[1]} },
			orders:    []string{echoed, "ORD-ECHO-2"},
			retain:    []int{1},
			behaviour: quirks{echo: true},
			delivered: 4,
			fedBack:   2,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			built, recorded := quirkySandbox(t, observedConfig(), testCase.behaviour, nil, testCase.orders...)

			var retain []uint64
			for _, at := range testCase.retain {
				retain = append(retain, recorded[at])
			}

			result, err := built.Run(t.Context(), "fed-back", testCase.mutation(recorded), retain)

			if testCase.stops != "" {
				if err == nil || !strings.Contains(err.Error(), testCase.stops) {
					t.Fatalf("Run() error = %v, want the run stopped naming %q", err, testCase.stops)
				}

				return
			}

			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			if result.Delivered != testCase.delivered || result.FedBack != testCase.fedBack {
				t.Errorf("Delivered = %d, FedBack = %d, want %d and %d",
					result.Delivered, result.FedBack, testCase.delivered, testCase.fedBack)
			}

			for at, observed := range result.Effects {
				if !slices.Contains(recorded, observed.MessageSeq) {
					t.Errorf("effect %d attributed to message %d, which is no corpus message", at, observed.MessageSeq)
				}
			}
		})
	}
}

// ledger wraps a session and keeps the deliveries of every faulted run, by fault.
type ledger struct {
	inner      check.Session
	deliveries map[policy.Fault][]int
}

func (l *ledger) Reset(ctx context.Context) error {
	return l.inner.Reset(ctx)
}

func (l *ledger) Run(
	ctx context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	result, err := l.inner.Run(ctx, name, mutation, retain)
	if err == nil && mutation.Fault() != policy.FaultNone {
		l.deliveries[mutation.Fault()] = append(l.deliveries[mutation.Fault()], result.Delivered)
	}

	return result, err
}

// TestCrashBeforeAckIsACrashLoop: crash_before_ack withheld one acknowledgement, exactly as duplicate
// does, so every finding under it was duplicate's reported twice. As a crash loop it withholds two and
// lets the third delivery through — repeated partial work, a different experiment — and each fault is
// still run once per message.
func TestCrashBeforeAckIsACrashLoop(t *testing.T) {
	t.Parallel()

	config := observedConfig()
	built, recorded := quirkySandbox(t, config, quirks{idempotent: true}, nil, "ORD-LOOP-1", "ORD-LOOP-2")

	session := &ledger{inner: built, deliveries: make(map[policy.Fault][]int)}

	if _, err := check.Run(t.Context(), session, check.Options{
		Messages: recorded,
		Consumer: observedConsumer,
		Config:   config,
	}); err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	// Every other message is delivered once, so the faulted message's own count is the excess.
	want := map[policy.Fault]int{policy.FaultDuplicate: 2, policy.FaultCrashBeforeAck: 3}

	for fault, times := range want {
		runs := session.deliveries[fault]
		t.Logf("%s: %d runs over %d messages, deliveries per run %v", fault, len(runs), len(recorded), runs)

		if len(runs) != len(recorded) {
			t.Errorf("%s ran %d times, want once per message (%d)", fault, len(runs), len(recorded))
		}

		for _, delivered := range runs {
			if got := delivered - (len(recorded) - 1); got != times {
				t.Errorf("%s delivered its message %d times, want %d", fault, got, times)
			}
		}
	}
}

// absent is a service that started and never connected to anything, as a container handed an
// address it cannot reach does.
type absent struct{}

func (absent) Close(context.Context) {}

// TestAServiceThatNeverConnectedIsNotPassed is the false negative measured against a real container:
// it never reached a proxy, three runs saw zero effects each, determinism held over two empty
// sequences, and the report read PASS for a service Stutter never saw.
func TestAServiceThatNeverConnectedIsNotPassed(t *testing.T) {
	t.Parallel()

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	seq, err := store.Publish(t.Context(), toy.SubjectOrderCreated, orderPayload("ORD-ABSENT-1", "WIDGET-WIRE"))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	config := observedConfig()

	built, err := harness.New(harness.Config{
		Corpus:  store,
		HashKey: make([]byte, hashKeyLen),
		Policy:  config,
		Quiesce: toy.DefaultQuiesce,
		// Nothing will ever arrive, so waiting out the derived horizons proves nothing more.
		Drain:   fetchWait,
		Startup: fetchWait,
		Start:   func(context.Context, harness.Addresses) (harness.Consumer, error) { return absent{}, nil },
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	result, err := check.Run(t.Context(), built, check.Options{
		Messages: []uint64{seq},
		Consumer: observedConsumer,
		Config:   config,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if got := result.ExitCode(); got != report.ExitGateViolated {
		t.Fatalf("ExitCode() = %d, want %d (gate violated):\n%s", got, report.ExitGateViolated, result)
	}

	if violations := result.Violations(); len(violations) != 1 || violations[0].Name != report.GateObservation {
		t.Errorf("Violations() = %v, want only %q", violations, report.GateObservation)
	}

	if want := "may never have connected"; !strings.Contains(result.String(), want) {
		t.Errorf("report does not say where to look (%q):\n%s", want, result)
	}
}
