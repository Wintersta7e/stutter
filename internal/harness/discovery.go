package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/policy"
	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
	"github.com/Wintersta7e/stutter/internal/replay"
)

var (
	// errStartOnly means a start that publishes nothing was given a service Stutter dispatches to. Only
	// a service that consumes for itself can be watched without being handed anything.
	errStartOnly = errors.New("a start that publishes nothing needs Config.Start, and Config.Connect unset")
	// errNoBaseline means such a start was given no checkpoint to restore. It would run against whatever
	// the bus holds now, which no run will ever start from.
	errNoBaseline = errors.New("a start that publishes nothing needs Config.Baseline")
	// errUnbound means such a start was given a corpus bound to no stream, so there is nothing to look
	// for the service's consumers on.
	errUnbound = errors.New("a start that publishes nothing needs a corpus bound to a stream")
)

var (
	// ErrExitedBeforeConsumer means the service stopped by itself before creating any consumer, so
	// discovery had nothing to read. The Discovery returned beside it says how it ended and why.
	ErrExitedBeforeConsumer = errors.New("the service under test exited before creating any consumer")
	// ErrConsumesElsewhere means the service's consumers are all on streams other than the corpus
	// stream. The Discovery returned beside it names them.
	ErrConsumesElsewhere = errors.New("the service under test consumes only from other streams")
)

// stageDiscovery names discovery in its errors.
const stageDiscovery = "discovery"

// Discovery is what one start of the service that publishes nothing learned about its consumers.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Discovery struct {
	// Consumers are the consumers on the corpus stream, sorted by name.
	Consumers []Found
	// Elsewhere names, as <stream>/<consumer>, every consumer on another stream that is not a key/value
	// bucket's or an object store's, sorted.
	Elsewhere []string
	// Refusals are the JetStream API requests the bus refused, with its code and description.
	Refusals []effect.Refusal
	// Exit is how the start ended: stopped by Stutter, or by itself.
	Exit replay.Exit
	// Setup counts everything the service did on its dependencies during the start.
	Setup int
	// ClosedAfterInfo counts bus connections that hung up after the greeting without a byte.
	ClosedAfterInfo int
}

// Found is one consumer on the corpus stream, as the server holds it.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Found struct {
	// Name is the consumer's name.
	Name string
	// Excluded, when set, says why the consumer cannot be a target.
	Excluded string
	// Policy is the consumer's configuration as the server read it back, defaults applied: the one
	// legality input for it.
	Policy policy.Config
	// Kind is what the consumer is as a target.
	Kind corpus.ConsumerKind
	// Unstable says the consumer's name does not survive a start, so no run can find it by name.
	Unstable bool
}

// Discover runs one start of the service that publishes nothing, from Config.Baseline, and reads every
// consumer the service creates as the server holds it, before any run rewrites one.
//
// Nothing is published, so everything the service does is setup and every effect is discarded. The
// HTTP script is aborted rather than committed: a call made while starting is not one any run will
// repeat, and freezing it would answer runs from a start that never handled a message.
func Discover(ctx context.Context, cfg Config) (Discovery, error) {
	sandbox, err := startOnly(cfg)
	if err != nil {
		return Discovery{}, fmt.Errorf("%s: %w", stageDiscovery, err)
	}

	return sandbox.discover(ctx)
}

// startOnly builds the sandbox for a start that publishes nothing: the same proxies, attach, stub and
// certificate authority as a run, without the fields only a run reads.
func startOnly(cfg Config) (*Sandbox, error) {
	switch {
	case cfg.Start == nil || cfg.Connect != nil:
		return nil, errStartOnly
	case cfg.Baseline == nil:
		return nil, errNoBaseline
	case cfg.Corpus == nil || cfg.Corpus.Topic().Stream == "":
		return nil, errUnbound
	}

	// A start that publishes nothing reads none of them, and a drain has a floor that means nothing here.
	cfg.Drain, cfg.Consumer, cfg.Policy, cfg.Recorded = 0, "", policy.Config{}, nil

	return New(cfg)
}

// discover runs discovery's start and reads what it found.
func (s *Sandbox) discover(ctx context.Context) (Discovery, error) {
	start, err := s.bareStart(ctx, stageDiscovery)
	if err != nil {
		return Discovery{}, err
	}

	return s.settleDiscovery(ctx, start)
}

// bare is one start that publishes nothing: the service running against the restored bus, the proxies
// in front of it, and what they have seen.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type bare struct {
	observed *egress
	service  Consumer
	recorder *effect.Recorder
	watch    *startWatch
	// started is when Start returned: the startup limit runs from here.
	started time.Time
	// quietSince is when the setup count last changed, and setup that count.
	quietSince time.Time
	// stage names the start in its errors.
	stage string
	setup int
}

// bareStart runs one start that publishes nothing, in a run's order: the dependencies reset, the bus
// restored to Baseline, the proxies up on the fresh bus, the service started.
func (s *Sandbox) bareStart(ctx context.Context, stage string) (*bare, error) {
	if err := s.Reset(ctx); err != nil {
		return nil, fmt.Errorf("%s: %w", stage, err)
	}

	if err := s.cfg.Corpus.Restore(ctx, *s.cfg.Baseline); err != nil {
		return nil, fmt.Errorf("%s: restore the bus: %w", stage, err)
	}

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), s.cfg.HashKey)
	watch := &startWatch{}

	observed, err := s.observe(ctx, recorder, natsproxy.Options{Deliveries: watch, Requests: watch})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", stage, err)
	}

	service, err := s.cfg.Start(ctx, observed.at)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("%s: start the service under test: %w", stage, err),
			observed.close(ctx),
			observed.httpRun.Abort(),
		)
	}

	started := time.Now()

	return &bare{
		observed:   observed,
		service:    service,
		recorder:   recorder,
		watch:      watch,
		started:    started,
		quietSince: started,
		stage:      stage,
		setup:      recorder.SetupCount(),
	}, nil
}

// quiet is how long the service has done nothing on any dependency, on the monotonic clock. It is only
// ever read from the one wait in progress.
func (b *bare) quiet() time.Duration {
	if count := b.recorder.SetupCount(); count != b.setup {
		b.quietSince, b.setup = time.Now(), count
	}

	return time.Since(b.quietSince)
}

// discard ends the start. The service goes first, because the proxies wait for its connections; the
// script is aborted, never committed.
func (b *bare) discard(ctx context.Context) (replay.Exit, error) {
	exit, err := b.service.Close(ctx)
	if err != nil {
		err = fmt.Errorf("%s: close the service under test: %w", b.stage, err)
	}

	return exit, errors.Join(err, b.observed.close(ctx), b.observed.httpRun.Abort())
}

// settleDiscovery waits the start out under discovery's rule, reads what it found, and discards it.
//
// Every read happens before the service is removed: a consumer with no durable name is deleted once
// its client has gone idle, and a read after the removal could find it gone.
func (s *Sandbox) settleDiscovery(ctx context.Context, start *bare) (Discovery, error) {
	listing, ended, err := s.awaitDiscovery(ctx, start)

	var (
		found       Discovery
		durableLess []string
	)

	if err == nil {
		found, durableLess, err = s.readDiscovered(ctx, listing)
	}

	found, err = start.conclude(ctx, found, ended, err)
	if err != nil || len(durableLess) == 0 {
		return found, err
	}

	return s.judgeNames(ctx, start.stage, found, durableLess)
}

// awaitDiscovery waits a start out under discovery's rule and returns the consumers at its end.
func (s *Sandbox) awaitDiscovery(ctx context.Context, start *bare) (corpus.Listing, interruption, error) {
	var listing corpus.Listing

	ended, err := start.observed.await(ctx, start.service.Exited(), s.discoverySettled(start, &listing))

	// The listing the wait last read can predate the exit.
	if err == nil && ended == targetExited {
		if listing, err = s.cfg.Corpus.Consumers(ctx); err != nil {
			err = fmt.Errorf("list the consumers: %w", err)
		}
	}

	return listing, ended, err
}

// conclude discards the start and completes what it found with how the start ended, what the bus
// refused it, and its exit row. err is what went wrong before the discard, if anything.
func (b *bare) conclude(ctx context.Context, found Discovery, ended interruption, err error) (Discovery, error) {
	exit, discardErr := b.discard(ctx)
	if err = errors.Join(err, discardErr); err != nil {
		return Discovery{}, fmt.Errorf("%s: %w", b.stage, err)
	}

	exit.Exited = exit.Exited || ended == targetExited
	found.Exit, found.Setup = exit, b.recorder.SetupCount()
	found.Refusals, found.ClosedAfterInfo = b.recorder.Refusals(), b.recorder.ClosedAfterInfoCount()

	if err := found.verdict(ended); err != nil {
		return found, fmt.Errorf("%s: %w", b.stage, err)
	}

	return found, nil
}

// judgeNames tells a consumer name that survives a start from one each start makes up. A name durable
// under itself survives by construction; each of the others is looked for again after one more start
// from the same checkpoint, run and ended exactly as the first. Only its listing is read: the first
// start's reads stand.
func (s *Sandbox) judgeNames(
	ctx context.Context,
	stage string,
	found Discovery,
	durableLess []string,
) (Discovery, error) {
	again, err := s.bareStart(ctx, stage+" (second start)")
	if err != nil {
		return Discovery{}, err
	}

	listing, ended, err := s.awaitDiscovery(ctx, again)

	listed := make([]Found, 0, len(listing.Corpus))
	for _, name := range listing.Corpus {
		listed = append(listed, Found{Name: name})
	}

	second, err := again.conclude(ctx, Discovery{Consumers: listed, Elsewhere: listing.Elsewhere}, ended, err)
	if err != nil {
		return second, err
	}

	unstable := corpus.Unstable(durableLess, listing.Corpus)
	for index := range found.Consumers {
		found.Consumers[index].Unstable = slices.Contains(unstable, found.Consumers[index].Name)
	}

	return found, nil
}

// verdict is what discovery found as an exit row: none when a consumer exists on the corpus stream,
// or when none exists anywhere and the service is still running — the check then runs unnamed, and its
// observation gate says what never happened.
func (d Discovery) verdict(ended interruption) error {
	switch {
	case len(d.Consumers) > 0:
		return nil
	case len(d.Elsewhere) > 0:
		return fmt.Errorf("%w: %s; the stream the check was pointed at is likely not the one it consumes from",
			ErrConsumesElsewhere, strings.Join(d.Elsewhere, ", "))
	case ended == targetExited:
		return fmt.Errorf("%w: %s", ErrExitedBeforeConsumer, d.diagnosis())
	default:
		return nil
	}
}

// diagnosis says why a service that exited before creating a consumer may have done so: how it
// exited, every JetStream request the bus refused it, and how many bus connections hung up after the
// greeting — what a client that requires TLS does.
func (d Discovery) diagnosis() string {
	parts := make([]string, 0, 1+len(d.Refusals)+1)
	parts = append(parts, "it "+d.Exit.Describe())

	for _, refusal := range d.Refusals {
		parts = append(parts, fmt.Sprintf("the bus refused %s (err_code %d: %s)",
			refusal.Subject, refusal.ErrCode, refusal.Description))
	}

	parts = append(parts, fmt.Sprintf("%d bus connections closed after the greeting without a byte",
		d.ClosedAfterInfo))

	return strings.Join(parts, "; ")
}

// readDiscovered reads every consumer on the corpus stream as the server holds it, on Stutter's own
// connection, and names those with no durable name of their own. Nothing is serialised: the read-back
// is the one legality input, and a rewrite read back would license faults against a contract the
// service never had.
func (s *Sandbox) readDiscovered(ctx context.Context, listing corpus.Listing) (Discovery, []string, error) {
	consumers := make([]Found, 0, len(listing.Corpus))

	var durableLess []string

	for _, name := range listing.Corpus {
		found, err := s.readConsumer(ctx, name)
		if err != nil {
			return Discovery{}, nil, err
		}

		durable, err := s.cfg.Corpus.Durable(ctx, name)
		if err != nil {
			return Discovery{}, nil, fmt.Errorf("read consumer %q: %w", name, err)
		}

		if !durable {
			durableLess = append(durableLess, name)
		}

		consumers = append(consumers, found)
	}

	return Discovery{Consumers: consumers, Elsewhere: listing.Elsewhere}, durableLess, nil
}

// readConsumer reads one consumer: what kind it is, and its configuration. A kind that cannot be a
// target, or a configuration with no legality row, is marked rather than returned as an error — it
// stops that consumer, not the others beside it.
func (s *Sandbox) readConsumer(ctx context.Context, name string) (Found, error) {
	kind, err := s.cfg.Corpus.Kind(ctx, name)
	if err != nil {
		return Found{}, fmt.Errorf("read consumer %q: %w", name, err)
	}

	found := Found{Name: name, Kind: kind}

	if kind.Refused() {
		found.Excluded = fmt.Sprintf("consumer kind %s: no fault is legal against it, "+
			"and one message in flight cannot be enforced", kind)

		return found, nil
	}

	config, err := s.cfg.Corpus.Policy(ctx, name)

	switch {
	case errors.Is(err, corpus.ErrUnmappable):
		found.Excluded = err.Error()
	case err != nil:
		return Found{}, fmt.Errorf("read consumer %q: %w", name, err)
	default:
		found.Policy = config
	}

	return found, nil
}

// discoverySettled is discovery's end rule: a consumer exists on any stream and the service has been
// quiet for the settle period, or the startup limit has passed since Start returned. listing holds the
// last listing read.
func (s *Sandbox) discoverySettled(start *bare, listing *corpus.Listing) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		read, err := s.cfg.Corpus.Consumers(ctx)
		if err != nil {
			return false, fmt.Errorf("list the consumers: %w", err)
		}

		*listing = read

		present := len(read.Corpus)+len(read.Elsewhere) > 0
		quiet := start.quiet() >= s.settle()

		return (present && quiet) || time.Since(start.started) >= s.startupLimit(), nil
	}
}

// startWatch is the bus hooks of a start that publishes nothing. It opens no attribution window, so
// everything the service does stays a setup count; it counts what the bus handed over, and notes when
// the service first asked JetStream for anything.
type startWatch struct {
	// requested is when the first JetStream API request went through the bus proxy, on the monotonic
	// clock; nil before it.
	requested atomic.Pointer[time.Time]
	delivered atomic.Int64
	core      atomic.Int64
}

// Delivered counts a consumer delivery. The start publishes nothing, but a stream a job filled can
// hold messages at the checkpoint.
func (w *startWatch) Delivered(natsproxy.Delivery) {
	w.delivered.Add(1)
}

// CoreDelivered counts a message handed to a core subscription.
func (w *startWatch) CoreDelivered(string) {
	w.core.Add(1)
}
