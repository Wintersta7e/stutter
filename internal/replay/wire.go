package replay

import (
	"fmt"

	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// WirePolicy injects a mutation into a service Stutter does not drive.
//
// A provisioned consumer pulls and acknowledges for itself, so the driver has no acknowledgement to
// withhold. The bus proxy does: an acknowledgement is a publish, and its subject carries the stream
// sequence and the attempt number — exactly what a Mutation's WithholdAck already decides on. The
// same rule therefore drives both models, rather than a second copy of it drifting out of step.
type WirePolicy struct {
	mutation Mutation
}

// NewWirePolicy adapts a mutation for wire injection, or refuses one that cannot be expressed there.
//
// Only the acknowledgement lever crosses over. HoldFor and Defer act on a delivery BEFORE the
// handler sees it, and a service Stutter does not dispatch to has already been handed the message
// by the time the proxy could act — expressing those needs the proxy to buffer and reorder
// deliveries, which it does not do. Refusing beats injecting something weaker under the same name.
func NewWirePolicy(mutation Mutation) (*WirePolicy, error) {
	switch mutation.(type) {
	case Clean, Duplicate, CrashBeforeAck:
		return &WirePolicy{mutation: mutation}, nil
	default:
		return nil, fmt.Errorf(
			"%w: %s needs the delivery held back before the service sees it, which the wire driver "+
				"cannot do", ErrUnsupported, mutation.Fault())
	}
}

// Withhold reports whether to swallow this acknowledgement, which makes the server redeliver.
func (p *WirePolicy) Withhold(ack natsproxy.Ack) bool {
	return p.mutation.WithholdAck(Message{Seq: ack.StreamSeq, Deliveries: ack.Deliveries})
}
