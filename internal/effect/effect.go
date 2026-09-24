// Package effect models the side effects a service under test produces, and reduces them to a form
// that is comparable between two runs.
//
// Comparison is the whole product: a handler is non-idempotent exactly when a delivery fault makes
// its effect sequence differ from the sequence it produced on a clean run. That comparison is only
// meaningful if two clean runs already agree, so everything here exists to remove the parts of an
// effect that change for reasons the service under test is not responsible for.
package effect

// Kind classifies an intercepted side effect by the protocol that carried it.
type Kind string

const (
	// KindPostgres is a statement sent to a Postgres server.
	KindPostgres Kind = "pg.query"
	// KindHTTP is an outbound HTTP request.
	KindHTTP Kind = "http.request"
	// KindNATS is a message published back to the bus.
	KindNATS Kind = "nats.publish"
	// KindSMTP is an outbound mail submission.
	KindSMTP Kind = "smtp.send"
	// KindOpaque is egress on a protocol Stutter does not parse. It still detects divergence, but
	// cannot describe it, so a report made only of opaque effects is not actionable.
	KindOpaque Kind = "opaque"
)

// Effect is one side effect observed during a run.
//
// Only Canonical participates in comparison. Printable exists so a finding can be acted on without
// reading Stutter's source, and RawHash exists so that when the determinism gate fails it is
// possible to tell an unstable service apart from an incomplete normaliser.
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Effect struct {
	// Consumer is the NATS consumer that received the message. Findings attribute to it.
	Consumer string
	// Kind is the protocol that carried the effect.
	Kind Kind
	// Canonical is the normalised form, and the only field compared between runs.
	Canonical string
	// Printable is the human-readable rendering shown in a report.
	Printable string
	// RawHash is a keyed hash of the pre-normalisation bytes, for diagnosing the normaliser itself.
	RawHash string
	// Provenance records how the originating message's values were extracted, so a gate failure can
	// be attributed to a degraded extraction rather than blamed on the service.
	Provenance Mode
	// MessageSeq is the corpus message this effect is attributed to.
	MessageSeq uint64
	// Seq is the ordinal of this effect within its run, starting at zero.
	Seq int
	// Stubbed marks a dependency call whose reply came from Stutter rather than a real dependency.
	// It never participates in comparison; it qualifies how confidently a divergence can be ruled.
	Stubbed bool
	// OffScript marks a stubbed call absent from the clean run. It received the default response.
	OffScript bool
	// Read marks an effect that asked for data rather than changing it: a SELECT, a key/value lookup.
	// It still takes part in comparison — a read can call a function that writes, and nothing in the
	// request says so — but a divergence made only of reads is a guard looking again, not wrong data.
	Read bool
	// Late marks an effect that arrived after its message's quiesce window closed. Late effects are
	// a determinism hazard and are reported as an anomaly, never as a divergence.
	Late bool
	// Rejected marks an operation the dependency refused, which therefore changed nothing. It is kept
	// so a reader can see what the service attempted, and excluded from comparison by Compared.
	//
	// This is the ONE place a response is read. The standing rule is that divergence is decided by
	// what the service asked for, never by what the dependency answered — narrowed here to never
	// INTERPRET a response. Whether an operation happened at all is not an interpretation of it, and
	// without this a working idempotency guard reports as a divergence: the guard's second claim is
	// refused by the bus, the handler does nothing, and the refusal alone lengthens the sequence.
	Rejected bool
}

// Compared narrows a run's effects to the ones that may decide a verdict.
//
// A refused operation changed nothing, so two runs that differ only in refusals did the same work.
// Both the comparison and any position taken from it must use this same view: the gate reports an
// ordinal WITHIN this sequence, and indexing the unfiltered one names a different effect.
func Compared(effects []Effect) []Effect {
	kept := make([]Effect, 0, len(effects))

	for _, observed := range effects {
		if !observed.Rejected {
			kept = append(kept, observed)
		}
	}

	return kept
}

// Observation is the protocol-neutral input a proxy gives the recorder.
//
// Raw is canonicalised and hashed. Printable is kept for a human report. Stub metadata never enters
// the comparable form, because a stub is evidence about confidence rather than service behaviour.
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Observation struct {
	Raw       string
	Printable string
	Kind      Kind
	// Correlation names a reply the dependency still owes, so Reject can find this effect again when
	// the answer arrives. Empty when the protocol gives nothing to wait for.
	Correlation string
	Stubbed     bool
	OffScript   bool
	// Read marks a request for data rather than a change to it. See Effect.Read.
	Read bool
}

// Refusal is a request the bus declined before the service under test was handed its first message:
// the request's subject and the bus's own code, error code and description.
//
// It is kept because nothing else records it. A service whose startup request is refused — a stream
// created twice with different subjects, a replicated stream on a single server — never gets as far
// as consuming, and without the refusal the report could only say that it never did.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Refusal struct {
	// Subject is the request's subject, which names the API call and what it was about.
	Subject string
	// Description is the bus's own explanation.
	Description string
	// Code is the status-like code of the answer; ErrCode the bus's specific error.
	Code    int
	ErrCode int
}
