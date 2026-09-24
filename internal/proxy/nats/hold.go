package nats

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
)

// Hold keeps every message the bus sends the service under test waiting at the proxy while the corpus
// is published, then lets them through in the order they arrived.
//
// A service that publishes into the stream it consumes would otherwise have its output land between
// the corpus's own messages while they are still being published, and every sequence after it would
// be wrong. What is held is only delayed: each frame is read, noted — a delivery still opens its
// window when it is read — and forwarded on release, none dropped and none reordered. The client's
// own frames are never held.
//
// The zero value is released and ready to use.
type Hold struct {
	// gate is open while nil; while held, it is closed on release.
	gate chan struct{}
	mu   sync.Mutex
}

// Begin holds every message the bus sends from now until Release. Beginning a held Hold does nothing.
func (h *Hold) Begin() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.gate == nil {
		h.gate = make(chan struct{})
	}
}

// Release lets every held message through. Releasing a released Hold does nothing.
func (h *Hold) Release() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.gate != nil {
		close(h.gate)
		h.gate = nil
	}
}

// wait blocks while the hold is on. A nil Hold never holds.
func (h *Hold) wait() {
	if h == nil {
		return
	}

	h.mu.Lock()
	gate := h.gate
	h.mu.Unlock()

	if gate != nil {
		<-gate
	}
}

// Pull is one request a pull consumer's client made for its next messages, with the timers it gives
// up on. Holding deliveries past either loses a delivery, so they bound how long a hold may last.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Pull struct {
	Stream   string
	Consumer string
	// Expires is how long the request lives; zero when the client set none.
	Expires time.Duration
	// Heartbeat is the idle heartbeat the client expects; zero when it asked for none.
	Heartbeat time.Duration
	// NoWait asks for whatever is there now, and nothing later.
	NoWait bool
}

// Pulls is told of each pull request the service under test makes.
type Pulls interface {
	Pulled(pull Pull)
}

// pullRequest is the body of a pull request, of which only the timers are read.
type pullRequest struct {
	Expires   int64 `json:"expires"`
	Heartbeat int64 `json:"idle_heartbeat"`
	NoWait    bool  `json:"no_wait"`
}

// parsePull reads a pull request off its subject, $JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>, and
// its body. A body that does not parse gives no timers: the request is still reported, so a hold is
// never bounded by a timer the client did not ask for.
func parsePull(subject string, body []byte) (Pull, bool) {
	names, found := strings.CutPrefix(subject, nextPrefix)
	if !found {
		return Pull{}, false
	}

	stream, consumer, found := strings.Cut(names, ".")
	if !found {
		return Pull{}, false
	}

	pull := Pull{Stream: stream, Consumer: consumer}

	var request pullRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return pull, true
	}

	pull.Expires = time.Duration(request.Expires)
	pull.Heartbeat = time.Duration(request.Heartbeat)
	pull.NoWait = request.NoWait

	return pull, true
}
