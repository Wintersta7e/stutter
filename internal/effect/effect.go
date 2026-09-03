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
	// Late marks an effect that arrived after its message's quiesce window closed. Late effects are
	// a determinism hazard and are reported as an anomaly, never as a divergence.
	Late bool
}
