package corpus

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Replay hands a corpus to the service under test, one message at a time.
type Replay struct {
	consumer jetstream.Consumer
}

// Next fetches the next delivery, waiting up to wait for one to arrive.
//
// It returns ErrDrained once the corpus is exhausted. A redelivery counts as a delivery: a message
// whose ack was withheld comes back through Next with a higher Deliveries.
func (r *Replay) Next(ctx context.Context, wait time.Duration) (*Delivery, error) {
	batch, err := r.consumer.FetchNoWait(1)
	if err != nil {
		return nil, fmt.Errorf("fetch from replay consumer: %w", err)
	}

	if delivery := first(batch); delivery != nil {
		return delivery, nil
	}

	if batchErr := batch.Error(); batchErr != nil {
		return nil, fmt.Errorf("replay batch: %w", batchErr)
	}

	// FetchNoWait returning nothing does not mean the corpus is drained: a message whose ack was
	// withheld is invisible until its AckWait expires. Fall back to a waiting fetch so redeliveries
	// are picked up rather than mistaken for the end of the corpus.
	waiting, err := r.consumer.Fetch(1, jetstream.FetchMaxWait(wait))
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("replay cancelled: %w", ctx.Err())
		}

		return nil, fmt.Errorf("fetch from replay consumer: %w", err)
	}

	if delivery := first(waiting); delivery != nil {
		return delivery, nil
	}

	if err := waiting.Error(); err != nil {
		return nil, fmt.Errorf("replay batch: %w", err)
	}

	return nil, ErrDrained
}

func first(batch jetstream.MessageBatch) *Delivery {
	for msg := range batch.Messages() {
		meta, err := msg.Metadata()
		if err != nil {
			// A message without JetStream metadata cannot be attributed or acknowledged
			// meaningfully. Terminate it rather than let it redeliver forever.
			_ = msg.Term() //nolint:errcheck // already on the failure path; nothing to report to.

			continue
		}

		return &Delivery{
			msg:        msg,
			Subject:    msg.Subject(),
			Payload:    msg.Data(),
			Seq:        meta.Sequence.Stream,
			Deliveries: meta.NumDelivered,
		}
	}

	return nil
}

// Delivery is one message handed to the service under test.
type Delivery struct {
	msg        jetstream.Msg
	Subject    string
	Payload    []byte
	Seq        uint64
	Deliveries uint64
}

// Ack acknowledges the message, so the bus considers it handled and does not redeliver.
func (d *Delivery) Ack() error {
	if err := d.msg.Ack(); err != nil {
		return fmt.Errorf("ack message %d: %w", d.Seq, err)
	}

	return nil
}

// Nak withholds acknowledgement, causing the server to redeliver.
//
// This is how both duplicate delivery and crash-before-ack are modelled. The bus cannot distinguish
// a consumer that died after performing its side effect from one that simply never acknowledged, so
// withholding the ack is not a simulation of that fault — it is the fault.
func (d *Delivery) Nak() error {
	if err := d.msg.Nak(); err != nil {
		return fmt.Errorf("nak message %d: %w", d.Seq, err)
	}

	return nil
}
