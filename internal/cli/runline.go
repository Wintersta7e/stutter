package cli

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// The run classes a run line names, from what was asked of the run rather than from the check's own
// name for it.
const (
	runProbe     = "probe"
	runDiscovery = "discovery"
	runClean     = "clean"
	runFault     = "fault"
	runShrink    = "shrink"
)

// lockedWriter is the compose branch's stderr: every line is written whole, whichever goroutine
// writes it — a run line, the interrupt line, a teardown line.
type lockedWriter struct {
	out io.Writer
	mu  sync.Mutex
}

func newLockedWriter(out io.Writer) *lockedWriter {
	return &lockedWriter{out: out}
}

// Write writes p whole, so a writer that prints several lines in one call is not interleaved either.
func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	written, err := w.out.Write(p)
	if err != nil {
		return written, fmt.Errorf("write stderr: %w", err)
	}

	return written, nil
}

// line writes one line. A failed write to stderr has nowhere else to be reported.
func (w *lockedWriter) line(text string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	fmt.Fprintln(w.out, text)
}

// timedSession is one consumer check's session, printing one run line per run: success or error, the
// run's class, and how long the reset and the run took together on the monotonic clock — Stutter's
// own overhead is that elapsed time less the run's span on the bus.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type timedSession struct {
	started  time.Time
	inner    check.Session
	out      *lockedWriter
	consumer string
}

// Reset starts the run's clock, then resets.
func (s *timedSession) Reset(ctx context.Context) error {
	s.started = time.Now()

	return s.inner.Reset(ctx) //nolint:wrapcheck // a decorator: the inner session's error is the check's.
}

// Run runs, then prints the run line; the inner result and error are returned as they came.
func (s *timedSession) Run(
	ctx context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	result, err := s.inner.Run(ctx, name, mutation, retain)

	span := result.Span
	if err != nil {
		span = 0
	}

	s.out.line(runLine(s.consumer, runClass(mutation, retain), mutation.Fault(), time.Since(s.started), span))

	return result, err //nolint:wrapcheck // a decorator: the inner session's error is the check's.
}

// runClass is what a run was asked to do: a shrink keeps a subset of the messages, a clean run injects
// nothing, and every other run injects its fault.
func runClass(mutation replay.Mutation, retain []uint64) string {
	switch {
	case len(retain) > 0:
		return runShrink
	case mutation.Fault() == policy.FaultNone:
		return runClean
	default:
		return runFault
	}
}

// runLine is one run on stderr. consumer is empty for the probe start, discovery and the unnamed check.
func runLine(consumer, class string, fault policy.Fault, elapsed, span time.Duration) string {
	name := string(fault)
	if fault == policy.FaultNone {
		name = "none"
	}

	return "stutter: run consumer=" + consumer + " run=" + class + " fault=" + name +
		" elapsed=" + elapsed.String() + " span=" + span.String()
}

// checkStartLine opens a compose check on stderr: the check's ID and its private directory, which
// stdout never carries.
func checkStartLine(checkID, privateDir string) string {
	return "stutter: check id=" + checkID + " dir=" + privateDir
}

// logLine names one kept log file, never its content.
func logLine(path string) string {
	return "stutter: log " + path
}

// keepLine names a kept check and how to remove it.
func keepLine(checkID string) string {
	return "stutter: keep id=" + checkID + " " + keptText(checkID)
}

// interruptLine is printed once, at the first interrupt.
func interruptLine() string {
	return "stutter: " + interruptText
}
