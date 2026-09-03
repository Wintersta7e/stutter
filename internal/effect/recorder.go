package effect

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// Recorder collects the effects of one run, attributing each to the message in flight.
//
// Attribution costs nothing because delivery is serial: exactly one message is outstanding at a
// time, so every effect observed while its window is open provably belongs to it. Effects that
// arrive after the window closes are kept and marked Late — they are a determinism hazard worth
// naming, not something to discard.
//
// Safe for concurrent use: the proxy records from its own goroutines.
type Recorder struct {
	canon   *Canonicaliser
	hashKey []byte
	effects []Effect
	window  window
	// setup counts effects observed before the first message was ever delivered.
	setup int
	// armed becomes true at the first Open. Before it, nothing is the service's response to traffic.
	armed bool
	mu    sync.Mutex
}

// Field order is dictated by govet's fieldalignment check, not by reading order.
type window struct {
	prov       *Provenance
	consumer   string
	messageSeq uint64
	open       bool
}

// NewRecorder builds a Recorder.
//
// hashKey keys the raw-bytes hash that diagnoses the normaliser when the determinism gate fails.
// Two runs being compared must share a key, or their raw hashes cannot be compared and the
// diagnosis is lost.
func NewRecorder(canon *Canonicaliser, hashKey []byte) *Recorder {
	return &Recorder{canon: canon, hashKey: hashKey}
}

// Open begins an attribution window for one message.
//
// The payload is parsed once here rather than per effect: provenance is a property of the message,
// and every effect recorded in this window shares it.
func (r *Recorder) Open(consumer string, messageSeq uint64, payload []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.armed = true
	r.window = window{
		consumer:   consumer,
		messageSeq: messageSeq,
		prov:       NewProvenance(payload),
		open:       true,
	}
}

// Record adds one observed side effect, canonicalising it against the open window's provenance.
//
// printable is the human-readable rendering; raw is the text that gets canonicalised. They are
// usually the same, and differ only where a protocol's readable form is not its comparable form.
func (r *Recorder) Record(kind Kind, raw, printable string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// An effect seen before the first message was delivered belongs to no message: it is the
	// service opening connections and its dependencies being made ready. Recording it would make
	// the sequence depend on which connection was established first, and on whether a bucket or a
	// schema already existed — neither of which is the service's response to traffic. Counted
	// rather than discarded silently, so a surprising count is visible.
	if !r.armed {
		r.setup++

		return
	}

	mode := ModeNone
	if r.window.prov != nil {
		mode = r.window.prov.Mode()
	}

	r.effects = append(r.effects, Effect{
		Consumer:   r.window.consumer,
		Kind:       kind,
		Canonical:  r.canon.Canonicalise(raw, r.window.prov),
		Printable:  printable,
		RawHash:    r.hash(raw),
		Provenance: mode,
		MessageSeq: r.window.messageSeq,
		Seq:        len(r.effects),
		Late:       !r.window.open,
	})
}

// Close ends the attribution window. Effects recorded afterwards are marked Late.
func (r *Recorder) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.window.open = false
}

// Effects returns the run's effects in the order they were observed.
func (r *Recorder) Effects() []Effect {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]Effect, len(r.effects))
	copy(out, r.effects)

	return out
}

// SetupCount reports how many effects were observed before the first message was delivered, and so
// were attributed to no message. A large count usually means a dependency is being provisioned
// through the proxy rather than beside it.
func (r *Recorder) SetupCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.setup
}

// LateCount reports how many effects arrived after their window closed. A non-zero count is the
// most likely reason the determinism gate fails against a real service.
func (r *Recorder) LateCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	var late int

	for _, item := range r.effects {
		if item.Late {
			late++
		}
	}

	return late
}

func (r *Recorder) hash(raw string) string {
	mac := hmac.New(sha256.New, r.hashKey)
	mac.Write([]byte(raw))

	return hex.EncodeToString(mac.Sum(nil))
}
