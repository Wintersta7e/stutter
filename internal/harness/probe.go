package harness

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// stageProbe names the probe start in its errors.
const stageProbe = "the probe start"

// Probe is what the probe start learned: whether the service creates the corpus stream itself.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Probe struct {
	// Discovery is set when the probe start saw the stream and so doubled as discovery.
	Discovery *Discovery
	// Created says the stream existed when the probe start ended, so the service creates it.
	Created bool
}

// ProbeStart runs one start of the service that publishes nothing, from Config.Baseline, taken before
// any stream exists, to learn whether the service creates the corpus stream itself.
//
// It is expected to fail: the service starts against a stream that is not there. It ends when the
// stream exists, when the service has asked JetStream for something and then gone quiet for the settle
// period, when the service exits, or at the startup limit — only the last three leave the stream absent,
// and none of them is an error. A proxy that stops before the stream exists is.
func ProbeStart(ctx context.Context, cfg Config) (Probe, error) {
	sandbox, err := startOnly(cfg)
	if err != nil {
		return Probe{}, fmt.Errorf("%s: %w", stageProbe, err)
	}

	return sandbox.probe(ctx)
}

// probe runs the probe start and reads whether the stream exists at its end.
func (s *Sandbox) probe(ctx context.Context) (Probe, error) {
	start, err := s.bareStart(ctx, stageProbe)
	if err != nil {
		return Probe{}, err
	}

	_, err = start.observed.await(ctx, start.service.Exited(), s.probeEnded(start))

	// Ownership is read at the moment the start ended, before the service is removed.
	var created bool
	if err == nil {
		created, err = s.streamExists(ctx)
	}

	_, discardErr := start.discard(ctx)
	if err = errors.Join(err, discardErr); err != nil {
		return Probe{}, fmt.Errorf("%s: %w", stageProbe, err)
	}

	return Probe{Created: created}, nil
}

// probeEnded is the probe start's end rule: the stream exists; or the service has asked JetStream for
// something and been quiet for the settle period since; or the startup limit has passed since Start
// returned.
func (s *Sandbox) probeEnded(start *bare) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		exists, err := s.streamExists(ctx)
		if err != nil || exists {
			return exists, err
		}

		if quiet, requested := start.quietSinceRequest(); requested && quiet >= s.settle() {
			return true, nil
		}

		return time.Since(start.started) >= s.startupLimit(), nil
	}
}

// streamExists surveys the bus on Stutter's own connection for the corpus stream.
func (s *Sandbox) streamExists(ctx context.Context) (bool, error) {
	survey, err := s.cfg.Corpus.Survey(ctx)
	if err != nil {
		return false, fmt.Errorf("survey the bus: %w", err)
	}

	return survey.Exists(), nil
}

// quietSinceRequest is how long the service has been quiet, counted from no earlier than its first
// JetStream API request; false before that request. A service that has not touched JetStream yet is
// still starting, however long it has been silent.
func (b *bare) quietSinceRequest() (time.Duration, bool) {
	first := b.watch.requested.Load()
	if first == nil {
		return 0, false
	}

	return min(b.quiet(), time.Since(*first)), true
}

// Requested notes the first JetStream API request the service sends.
func (w *startWatch) Requested(string) {
	now := time.Now()
	w.requested.CompareAndSwap(nil, &now)
}
