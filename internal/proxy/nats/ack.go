package nats

import (
	"encoding/json"
	"strconv"
	"strings"
)

// ackPrefix opens every JetStream acknowledgement subject.
const ackPrefix = "$JS.ACK."

// nextPrefix opens the subject a pull consumer asks the bus for its next message on.
const nextPrefix = "$JS.API.CONSUMER.MSG.NEXT."

// ackInProgress asks the server for more time instead of settling the message.
const ackInProgress = "+WPI"

// ackNegative asks the server to redeliver the message now.
const ackNegative = "-NAK"

// Acknowledgement subjects come in two shapes. Both end with the same seven fields; the longer one
// carries a domain and an account hash in front of them.
//
//	$JS.ACK.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<timestamp>.<pending>
//	$JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<timestamp>.<pending>
const (
	shortTokens = 9
	longTokens  = 11
	// trailing is how many fields follow the consumer name, and so how far back to count from the
	// end to find the stream regardless of which shape arrived.
	trailing = 5
)

// Ack is one acknowledgement a consumer sent, recovered from the subject it was published to.
//
// It is delivery bookkeeping, not a side effect: a service that acknowledges for itself does so on
// the same connection it does real work on, and recording an acknowledgement as an effect would put
// the tool's own fault injection into the sequence it is comparing.
type Ack struct {
	// Stream and Consumer name what is being acknowledged.
	Stream   string
	Consumer string
	// Payload distinguishes the kinds: "+ACK" acknowledges, "-NAK" asks for redelivery, "+WPI" asks
	// for more time, "+TERM" gives up. An empty payload is also an acknowledgement — the protocol
	// allows it and some clients send it.
	Payload []byte
	// StreamSeq is the message being acknowledged, which is what a per-message fault targets.
	StreamSeq uint64
	// Deliveries counts this message's deliveries so far, starting at one.
	Deliveries uint64
}

// InProgress reports whether the acknowledgement only asks the server for more time.
//
// It settles nothing: the handler is still working. Swallowing one would inject a redelivery the run
// never chose, and closing an attribution window on one would cut a handler off half way through its
// own work and report the rest of it against the next message.
func (a Ack) InProgress() bool {
	return string(a.Payload) == ackInProgress
}

// Negative reports whether the acknowledgement asks for the message to be redelivered.
//
// It is the only way a service Stutter does not call can say a delivery failed: there is no return
// value to read, so this is what stands in for a handler error on a driven run.
//
// Matched by prefix, as the server matches it: a NAK may carry a redelivery delay, as JSON or as a
// duration, and one with a delay refuses the delivery exactly as a bare one does.
func (a Ack) Negative() bool {
	return strings.HasPrefix(string(a.Payload), ackNegative)
}

// apiResponse is the shape every JetStream API reply shares. Only the presence of the error object
// is read: which error it was is the dependency's business, and interpreting it is exactly the thing
// the standing rule forbids.
type apiResponse struct {
	Error *apiError `json:"error"`
}

// apiError is present on every JetStream reply that declined the request. Only its presence is read.
type apiError struct {
	Code int `json:"code"`
}

// refused reports whether the bus answered a publish by declining to store it.
//
// A body that is not a JetStream API reply is not a refusal: a core NATS request-reply carries
// whatever the responder chose, and guessing at it would drop effects that really happened. The
// conservative direction is to keep the effect, because an effect wrongly kept is at worst a
// divergence a human can dismiss, while one wrongly dropped is a bug that was never reported.
func refused(payload []byte) bool {
	var answer apiResponse

	if err := json.Unmarshal(payload, &answer); err != nil {
		return false
	}

	return answer.Error != nil
}

// isAckSubject reports whether a publish is an acknowledgement rather than the service's own work.
func isAckSubject(subject string) bool {
	return strings.HasPrefix(subject, ackPrefix)
}

// isBookkeeping reports whether a publish is the consumer running its own delivery rather than the
// handler doing work.
//
// Two subjects qualify. An acknowledgement settles a message. A pull request is the service asking
// the bus for its next one — how many it sends, and when, depends on its client library's batching
// and on how fast the run went, so recording them would fail the determinism gate on traffic the
// handler never chose to send. Both would also put Stutter's own fault injection into the sequence
// it is comparing, since a withheld acknowledgement changes what follows it.
//
// The rest of $JS.API is deliberately NOT excluded: a handler that creates a stream or a bucket did
// something, and hiding it would report that handler as having done nothing.
func isBookkeeping(subject string) bool {
	return isAckSubject(subject) || strings.HasPrefix(subject, nextPrefix)
}

// parseAck recovers an acknowledgement from its subject.
//
// A subject that does not parse is reported as such rather than guessed at: withholding the wrong
// acknowledgement injects a fault against a message nobody chose, and forwarding one Stutter meant
// to withhold silently produces a clean run.
func parseAck(subject string, payload []byte) (Ack, bool) {
	if !isAckSubject(subject) {
		return Ack{}, false
	}

	tokens := strings.Split(subject, ".")
	if len(tokens) != shortTokens && len(tokens) != longTokens {
		return Ack{}, false
	}

	// Counted from the end, so both shapes are read the same way.
	consumerAt := len(tokens) - trailing - 1

	delivered, err := strconv.ParseUint(tokens[consumerAt+1], 10, 64)
	if err != nil {
		return Ack{}, false
	}

	streamSeq, err := strconv.ParseUint(tokens[consumerAt+2], 10, 64)
	if err != nil {
		return Ack{}, false
	}

	return Ack{
		Stream:     tokens[consumerAt-1],
		Consumer:   tokens[consumerAt],
		Payload:    payload,
		StreamSeq:  streamSeq,
		Deliveries: delivered,
	}, true
}
