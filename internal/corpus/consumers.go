package corpus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/policy"
)

// ErrUnmappable means a consumer's configuration has a value the legality table has no row for, so no
// fault can be licensed against it.
var ErrUnmappable = errors.New("the consumer's configuration cannot be read as a delivery contract")

// Streams whose consumers are the client library's own machinery rather than a service's: a key/value
// bucket's and an object store's.
const (
	bucketStreamPrefix = "KV_"
	objectStreamPrefix = "OBJ_"
)

// Listing names the consumers on the bus.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Listing struct {
	// Corpus names the consumers on the bound stream, sorted.
	Corpus []string
	// Elsewhere names, as <stream>/<consumer>, every consumer on any other stream whose name does not
	// start with KV_ or OBJ_, sorted.
	Elsewhere []string
}

// Consumers lists every consumer on the bus: those on the bound stream, and those anywhere else.
//
// A bound stream that does not exist yet has no consumers, which is an answer rather than a failure:
// nothing may have created it so far. Any other failure to read is returned.
func (c *Corpus) Consumers(ctx context.Context) (Listing, error) {
	var listing Listing

	bound, err := c.stream.Stream(ctx, c.topic.Stream)

	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
	case err != nil:
		return Listing{}, fmt.Errorf("open the corpus stream: %w", err)
	default:
		listing.Corpus, err = consumerNames(ctx, bound)
		if err != nil {
			return Listing{}, fmt.Errorf("list the corpus stream's consumers: %w", err)
		}
	}

	listing.Elsewhere, err = c.elsewhere(ctx)
	if err != nil {
		return Listing{}, err
	}

	return listing, nil
}

// Policy reads a consumer on the bound stream as the server holds it, mapped onto the delivery
// contract legality is decided from. It is the read-back: whatever the service asked for, the server's
// defaults are what is in force.
func (c *Corpus) Policy(ctx context.Context, consumer string) (policy.Config, error) {
	info, err := c.consumerInfo(ctx, consumer)
	if err != nil {
		return policy.Config{}, err
	}

	config, err := mapConfig(info.Config)
	if err != nil {
		return policy.Config{}, fmt.Errorf("consumer %q: %w", consumer, err)
	}

	return config, nil
}

// elsewhere names every consumer on a stream other than the bound one, skipping the client library's
// own streams.
func (c *Corpus) elsewhere(ctx context.Context) ([]string, error) {
	lister := c.stream.StreamNames(ctx)

	var streams []string
	for name := range lister.Name() {
		streams = append(streams, name)
	}

	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("list the streams: %w", err)
	}

	var found []string

	for _, name := range streams {
		if name == c.topic.Stream || strings.HasPrefix(name, bucketStreamPrefix) ||
			strings.HasPrefix(name, objectStreamPrefix) {
			continue
		}

		stream, err := c.stream.Stream(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("open stream %q: %w", name, err)
		}

		names, err := consumerNames(ctx, stream)
		if err != nil {
			return nil, fmt.Errorf("list stream %q's consumers: %w", name, err)
		}

		for _, consumer := range names {
			found = append(found, name+"/"+consumer)
		}
	}

	slices.Sort(found)

	return found, nil
}

// consumerNames names every consumer on one stream, sorted.
func consumerNames(ctx context.Context, stream jetstream.Stream) ([]string, error) {
	lister := stream.ConsumerNames(ctx)

	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}

	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("list consumers: %w", err)
	}

	slices.Sort(names)

	return names, nil
}

// mapConfig reads a consumer configuration, as the server holds it, into the delivery contract
// legality is decided from. Every field is copied as read back; the filter, which the server accepts
// either as one subject or as several, becomes one sorted set.
func mapConfig(server jetstream.ConsumerConfig) (policy.Config, error) {
	var mode policy.AckMode

	// exhaustive wants every client policy listed, revive rejects the identical branch that would
	// produce. The default covers every policy with no row in the legality table.
	switch server.AckPolicy { //nolint:exhaustive // default refuses the policies with no legality row.
	case jetstream.AckExplicitPolicy:
		mode = policy.AckExplicit
	case jetstream.AckAllPolicy:
		mode = policy.AckAll
	case jetstream.AckNonePolicy:
		mode = policy.AckNone
	default:
		return policy.Config{}, fmt.Errorf("%w: AckPolicy %s", ErrUnmappable, server.AckPolicy)
	}

	return policy.Config{
		AckMode:        mode,
		BackOff:        slices.Clone(server.BackOff),
		FilterSubjects: filterSubjects(server),
		AckWait:        server.AckWait,
		MaxDeliver:     server.MaxDeliver,
		MaxAckPending:  server.MaxAckPending,
	}, nil
}

// filterSubjects is a consumer's filter as one sorted set, nil when it admits the whole stream.
func filterSubjects(server jetstream.ConsumerConfig) []string {
	var filters []string
	if server.FilterSubject != "" {
		filters = append(filters, server.FilterSubject)
	}

	filters = append(filters, server.FilterSubjects...)
	slices.Sort(filters)

	return slices.Compact(filters)
}
