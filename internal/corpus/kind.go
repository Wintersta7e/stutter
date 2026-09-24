package corpus

import (
	"context"
	"fmt"
	"slices"

	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerKind is what a consumer is as a target: whether it can be faulted and held to one message
// in flight at all.
type ConsumerKind uint8

const (
	// KindPull is a consumer its client pulls from. Checkable.
	KindPull ConsumerKind = iota
	// KindPush is a consumer the bus pushes to a deliver subject. Checkable: pausing it and holding it
	// to one message in flight both hold.
	KindPush
	// KindAckNone acknowledges nothing, so no redelivery fault is legal and nothing limits how many
	// messages it has in flight. Refused.
	KindAckNone
	// KindOrdered is a client's ordered consumer: unacknowledged, in memory, recreated under a new name
	// whenever the client decides. Refused.
	KindOrdered
)

// Refused reports whether a consumer of this kind cannot be checked.
func (k ConsumerKind) Refused() bool {
	return k == KindAckNone || k == KindOrdered
}

func (k ConsumerKind) String() string {
	switch k {
	case KindPull:
		return "pull"
	case KindPush:
		return "push"
	case KindAckNone:
		return "AckNone"
	case KindOrdered:
		return "ordered"
	default:
		return fmt.Sprintf("ConsumerKind(%d)", uint8(k))
	}
}

// Kind reads a consumer on the bound stream once and says what it is.
//
// There is no durable or ephemeral kind: one read cannot tell whether a name survives a start. A
// consumer the service names reads back with no durable name and still keeps its name; Durable and
// Unstable decide that.
func (c *Corpus) Kind(ctx context.Context, consumer string) (ConsumerKind, error) {
	info, err := c.consumerInfo(ctx, consumer)
	if err != nil {
		return 0, err
	}

	switch {
	case info.Config.AckPolicy == jetstream.AckNonePolicy && info.Config.MemoryStorage:
		return KindOrdered, nil
	case info.Config.AckPolicy == jetstream.AckNonePolicy:
		return KindAckNone, nil
	case info.Config.DeliverSubject != "":
		return KindPush, nil
	default:
		return KindPull, nil
	}
}

// Durable reports whether a consumer on the bound stream is durable under its own name, which makes
// its name stable with no second start to compare against. Every other consumer — one the service
// named without making it durable, one the client named for it — needs Unstable's second look.
func (c *Corpus) Durable(ctx context.Context, consumer string) (bool, error) {
	info, err := c.consumerInfo(ctx, consumer)
	if err != nil {
		return false, err
	}

	return info.Config.Durable != "" && info.Config.Durable == info.Name, nil
}

// Unstable names the consumers of an earlier start that a later start does not have, sorted: their
// names changed, so they cannot be found by name from one start to the next.
func Unstable(earlier, later []string) []string {
	var gone []string

	for _, name := range earlier {
		if !slices.Contains(later, name) {
			gone = append(gone, name)
		}
	}

	slices.Sort(gone)

	return slices.Compact(gone)
}

// consumerInfo reads one consumer on the bound stream, whatever its kind.
func (c *Corpus) consumerInfo(ctx context.Context, consumer string) (*jetstream.ConsumerInfo, error) {
	stream, err := c.stream.Stream(ctx, c.topic.Stream)
	if err != nil {
		return nil, fmt.Errorf("open the corpus stream: %w", err)
	}

	info, err := lookup(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("find consumer %q: %w", consumer, err)
	}

	return info, nil
}
