package harness_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
)

const (
	// pushSubject is where the bus pushes the push consumer's messages. Outside the corpus subjects, so
	// the subscription is only ever the consumer's.
	pushSubject = "deliver.reserve_stock"
	// pushBatch is how many unacknowledged messages the service's own consumer asks for. Well above one,
	// so a run that did not serialise it would have many in flight at once.
	pushBatch = 100
	// pushLinger is how long the handler waits before looking for more deliveries queued behind the one
	// it holds: long enough for the bus to push any it is allowed to.
	pushLinger = 5 * time.Millisecond
)

// pushed is a service consuming through a push consumer: the bus sends each message to a subject the
// service subscribes to, rather than answering its pulls. It measures how many messages it holds
// unacknowledged at once, which is what one message in flight is about.
type pushed struct {
	connection   *nats.Conn
	subscription *nats.Subscription
	dependency   net.Conn
	replies      *bufio.Reader
	// inFlight is the most deliveries seen held at once: the one being handled and those queued.
	inFlight atomic.Int64
}

// startPushed subscribes to the push subject, then creates the push consumer that delivers there.
func startPushed(ctx context.Context, at harness.Addresses, config policy.Config) (*pushed, error) {
	dialer := net.Dialer{Timeout: time.Second}

	dependency, err := dialer.DialContext(ctx, "tcp", at.Opaque[opaqueCache])
	if err != nil {
		return nil, fmt.Errorf("dial the unparsed dependency: %w", err)
	}

	service := &pushed{dependency: dependency, replies: bufio.NewReader(dependency)}

	service.connection, err = nats.Connect(at.NATS)
	if err != nil {
		return nil, fmt.Errorf("connect to the bus: %w", err)
	}

	service.subscription, err = service.connection.Subscribe(pushSubject, service.handle)
	if err != nil {
		return nil, fmt.Errorf("subscribe to the push subject: %w", err)
	}

	stream, err := jetstream.New(service.connection)
	if err != nil {
		return nil, fmt.Errorf("open jetstream: %w", err)
	}

	if _, err := stream.CreateOrUpdatePushConsumer(ctx, corpus.StreamName, jetstream.ConsumerConfig{
		Durable:        observedConsumer,
		DeliverSubject: pushSubject,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        config.AckWait,
		MaxDeliver:     config.MaxDeliver,
		MaxAckPending:  pushBatch,
	}); err != nil {
		return nil, fmt.Errorf("create the push consumer: %w", err)
	}

	return service, nil
}

// Close stops the subscription before the connections go.
func (p *pushed) Close(context.Context) (replay.Exit, error) {
	//nolint:errcheck // the connection is closed next either way.
	_ = p.subscription.Unsubscribe()

	p.connection.Close()

	_ = p.dependency.Close()

	return replay.Exit{}, nil
}

// Exited never fires: the service never stops by itself.
func (*pushed) Exited() <-chan struct{} {
	return nil
}

// handle reserves stock for one delivery, noting how many more the bus has already pushed behind it.
func (p *pushed) handle(msg *nats.Msg) {
	time.Sleep(pushLinger)

	// The count includes the delivery being handled: the client settles its accounting for one only
	// when it takes the next.
	if queued, _, err := p.subscription.Pending(); err == nil {
		held := int64(queued)

		for seen := p.inFlight.Load(); held > seen && !p.inFlight.CompareAndSwap(seen, held); {
			seen = p.inFlight.Load()
		}
	}

	var order struct {
		OrderID string `json:"order_id"`
		Qty     int    `json:"qty"`
	}

	if json.Unmarshal(msg.Data, &order) != nil {
		return
	}

	if _, err := fmt.Fprintf(p.dependency, "RESERVE %s %d\n", order.OrderID, order.Qty); err != nil {
		return
	}

	if _, err := p.replies.ReadString('\n'); err != nil {
		return
	}

	//nolint:errcheck // a settle that fails is the run ending underneath the service, and the proxy
	// reports that; retrying it here would add a delivery the run never asked for.
	_ = msg.Respond([]byte("+ACK"))
}

// TestAPushConsumerIsHeldToOneMessageInFlight: a push consumer asking for a batch has the bus push
// messages as fast as its own limit allows, so without serialising it the run would attribute a batch.
// Serialised, each delivery waits for the previous acknowledgement.
func TestAPushConsumerIsHeldToOneMessageInFlight(t *testing.T) {
	t.Parallel()

	const messages = 20

	orders := make([]string, 0, messages)
	for index := range messages {
		orders = append(orders, "ORD-PUSH-"+strconv.Itoa(index+1))
	}

	var service *pushed

	built, _ := quirkySandbox(t, observedConfig(), quirks{}, func(settings *harness.Config) {
		settings.Start = func(ctx context.Context, at harness.Addresses) (harness.Consumer, error) {
			started, err := startPushed(ctx, at, observedConfig())
			if err != nil {
				return nil, err
			}

			service = started

			return started, nil
		}
	}, orders...)

	result, err := built.Run(t.Context(), "clean-1", replay.Clean{}, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	held := service.inFlight.Load()
	t.Logf("%d deliveries, at most %d held at once", result.Delivered, held)

	if result.Delivered != messages {
		t.Fatalf("Delivered = %d, want %d", result.Delivered, messages)
	}

	if held != 1 {
		t.Errorf("the push consumer held %d messages at once, want 1", held)
	}
}
