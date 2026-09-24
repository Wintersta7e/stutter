package nats

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// ackPrefix opens every JetStream acknowledgement subject.
const ackPrefix = "$JS.ACK."

// nextPrefix opens the subject a pull consumer asks the bus for its next message on.
const nextPrefix = "$JS.API.CONSUMER.MSG.NEXT."

// ackInProgress asks the server for more time instead of settling the message.
const ackInProgress = "+WPI"

// ackNegative asks the server to redeliver the message now.
const ackNegative = "-NAK"

// The payloads that settle a message, as the server matches them: the first two exactly, the last two
// by prefix.
const (
	ackPositive = "+ACK"
	ackOK       = "+OK"
	ackNext     = "+NXT"
	ackTerm     = "+TERM"
)

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

// Settles reports whether the acknowledgement settles the message, exactly as the server decides it
// (nats-server v2.14.6, server/consumer.go, processAck): an empty payload, "+ACK" or "+OK" exactly, or
// anything opening "+NXT" or "+TERM". A payload the server does not match — "+ACK " with a trailing
// space, say — is ignored by it and leaves the message owed, so it settles nothing here either.
func (a Ack) Settles() bool {
	payload := string(a.Payload)

	return payload == "" || payload == ackPositive || payload == ackOK ||
		strings.HasPrefix(payload, ackNext) || strings.HasPrefix(payload, ackTerm)
}

// NakDelay is how long a NAK asks the server to wait before redelivering, parsed as the server parses
// it (server/consumer.go, processNak): the argument after "-NAK", as JSON {"delay": <nanoseconds>} when
// it opens with a brace and as a Go duration otherwise. It is zero for anything that is not a NAK, and
// for an argument that does not parse, which the server treats as a plain NAK.
func (a Ack) NakDelay() time.Duration {
	if !a.Negative() {
		return 0
	}

	argument := strings.TrimSpace(strings.TrimPrefix(string(a.Payload), ackNegative))
	if argument == "" {
		return 0
	}

	if strings.HasPrefix(argument, "{") {
		var options struct {
			Delay time.Duration `json:"delay"`
		}

		if err := json.Unmarshal([]byte(argument), &options); err != nil {
			return 0
		}

		return options.Delay
	}

	delay, err := time.ParseDuration(argument)
	if err != nil {
		return 0
	}

	return delay
}

// apiResponse is the shape every JetStream API reply shares. The presence of the error object decides
// whether the request was refused; what it says is carried to the report, never interpreted.
type apiResponse struct {
	Error *apiError `json:"error"`
}

// apiError is present on every JetStream reply that declined the request.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type apiError struct {
	Description string `json:"description"`
	Code        int    `json:"code"`
	ErrCode     int    `json:"err_code"`
}

// apiRefusal reads the error a JetStream reply declined a request with, and whether it declined it.
//
// A body that is not a JetStream API reply is not a refusal: a core NATS request-reply carries
// whatever the responder chose, and guessing at it would drop effects that really happened. The
// conservative direction is to keep the effect, because an effect wrongly kept is at worst a
// divergence a human can dismiss, while one wrongly dropped is a bug that was never reported.
func apiRefusal(payload []byte) (apiError, bool) {
	var answer apiResponse

	if err := json.Unmarshal(payload, &answer); err != nil || answer.Error == nil {
		return apiError{}, false
	}

	return *answer.Error, true
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
