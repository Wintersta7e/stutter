// Package policy derives which delivery faults a consumer's own configuration actually permits.
//
// A fuzzer that invents impossible interleavings produces unfixable reports and destroys trust in
// its own output, so every mutation Stutter emits has to correspond to something the recorded
// delivery contract genuinely allows. Each verdict carries the configuration clause that licensed
// or forbade it, because a finding a user cannot trace back to their own stream config is a finding
// they will not act on.
package policy

import (
	"strconv"
	"time"
)

// Fault names a delivery fault Stutter can inject.
type Fault string

const (
	// FaultNone injects nothing. It is the reference run, and is always permitted.
	FaultNone Fault = ""
	// FaultDuplicate delivers one stored message twice, modelling a lost acknowledgement.
	FaultDuplicate Fault = "duplicate"
	// FaultCrashBeforeAck withholds the acknowledgement after the side effect, modelling a consumer
	// that died between doing the work and reporting it.
	FaultCrashBeforeAck Fault = "crash_before_ack"
	// FaultReorder delivers two messages in the opposite order to the one recorded.
	FaultReorder Fault = "reorder"
	// FaultDelay holds a message past the deadline governing its attempt, so the server redelivers
	// while the first delivery is still being worked on.
	FaultDelay Fault = "delay"
	// FaultConcurrent delivers two messages at once.
	FaultConcurrent Fault = "concurrent"
	// FaultDrop never delivers a message, modelling an at-most-once path.
	FaultDrop Fault = "drop"
)

// AckMode mirrors the consumer's acknowledgement policy.
type AckMode string

const (
	// AckExplicit requires every message to be acknowledged individually.
	AckExplicit AckMode = "explicit"
	// AckAll acknowledges a message and everything before it.
	AckAll AckMode = "all"
	// AckNone acknowledges nothing, so nothing is ever redelivered.
	AckNone AckMode = "none"
)

// Config is the stream and consumer configuration in force when a corpus was recorded.
//
// It is an input to mutation selection, not diagnostic metadata: the faults Stutter is allowed to
// commit are read off it.
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Config struct {
	// AckMode is the consumer's acknowledgement policy.
	AckMode AckMode
	// BackOff is the per-attempt acknowledgement deadline curve.
	//
	// When set it OVERRIDES AckWait, and reading AckWait instead is not a harmless approximation: a
	// consumer that declared a two-minute wait while carrying a one-second first backoff entry
	// redelivered nearly every message, duplicating roughly one delivery in six in production. The
	// last entry governs every attempt through MaxDeliver.
	BackOff []time.Duration
	// FilterSubjects are the subjects this consumer receives. Ordering is guaranteed within one
	// subject, so reordering is only ever legal across two of them.
	FilterSubjects []string
	// AckWait is the declared ack window, used only when BackOff is empty.
	AckWait time.Duration
	// MaxDeliver caps delivery attempts per message. One means no redelivery exists.
	MaxDeliver int
	// MaxAckPending is how many messages may be outstanding at once. One forbids concurrency, and
	// with it reordering within a subject.
	MaxAckPending int
}

// Verdict reports whether a fault is permitted, and names the clause that decided it.
type Verdict struct {
	// Clause is the configuration that licensed or forbade the fault, quoted into any finding.
	Clause string
	// Permitted reports whether the bus may genuinely commit this fault.
	Permitted bool
}

// Deadline returns the acknowledgement deadline governing delivery attempt n, counting from one.
//
// BackOff wins over AckWait wherever it is set, and its final entry governs every later attempt.
func (c Config) Deadline(attempt int) time.Duration {
	if len(c.BackOff) == 0 {
		return c.AckWait
	}

	index := min(max(attempt-1, 0), len(c.BackOff)-1)

	return c.BackOff[index]
}

// Permits reports whether the recorded configuration allows a fault.
func (c Config) Permits(fault Fault) Verdict {
	switch fault {
	case FaultNone:
		return Verdict{Permitted: true, Clause: "no fault injected — this is the reference run"}
	case FaultDuplicate, FaultCrashBeforeAck, FaultDelay:
		return c.redelivery()
	case FaultReorder, FaultConcurrent:
		return c.concurrency()
	case FaultDrop:
		return c.drop()
	default:
		return Verdict{Permitted: false, Clause: "unknown fault " + string(fault)}
	}
}

// redelivery decides the three faults that all rest on the bus handing the same stored message over
// again.
//
// The stream's publish-side duplicate window is deliberately not consulted. It deduplicates
// ingestion of the same message id, not delivery: every redelivery of an already-stored message
// carries the same stream sequence and arrives regardless of that window.
func (c Config) redelivery() Verdict {
	if c.AckMode == AckNone {
		return Verdict{
			Permitted: false,
			Clause:    "AckPolicy: none — nothing is acknowledged, so nothing is ever redelivered",
		}
	}

	if c.MaxDeliver == 1 {
		return Verdict{
			Permitted: false,
			Clause:    "MaxDeliver: 1 — the bus delivers each message at most once",
		}
	}

	return Verdict{
		Permitted: true,
		Clause: "AckPolicy: " + string(c.AckMode) +
			" with MaxDeliver " + describeLimit(c.MaxDeliver) +
			" — an unacknowledged message is redelivered",
	}
}

func (c Config) concurrency() Verdict {
	if c.MaxAckPending == 1 {
		return Verdict{
			Permitted: false,
			Clause:    "MaxAckPending: 1 — only one message is ever outstanding, so delivery is ordered",
		}
	}

	if len(c.FilterSubjects) > 1 {
		return Verdict{
			Permitted: true,
			Clause: "MaxAckPending " + describeLimit(c.MaxAckPending) +
				" across " + strconv.Itoa(len(c.FilterSubjects)) +
				" filter subjects — ordering is guaranteed only within a subject",
		}
	}

	return Verdict{
		Permitted: true,
		Clause: "MaxAckPending " + describeLimit(c.MaxAckPending) +
			" — concurrent delivery is permitted, so arrival order is not guaranteed",
	}
}

func (c Config) drop() Verdict {
	if c.MaxDeliver == 1 {
		return Verdict{
			Permitted: true,
			Clause:    "MaxDeliver: 1 — a failed delivery is never retried",
		}
	}

	if c.AckMode == AckNone {
		return Verdict{
			Permitted: true,
			Clause:    "AckPolicy: none — an at-most-once path, so a message may simply not arrive",
		}
	}

	return Verdict{
		Permitted: false,
		Clause: "AckPolicy: " + string(c.AckMode) +
			" with MaxDeliver " + describeLimit(c.MaxDeliver) +
			" — the bus retries, so a silently dropped message models nothing",
	}
}

// describeLimit renders a JetStream limit, where a non-positive value means unlimited.
func describeLimit(value int) string {
	if value <= 0 {
		return "unlimited"
	}

	return strconv.Itoa(value)
}
