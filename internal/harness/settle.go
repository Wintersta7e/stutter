package harness

import (
	"context"
	"fmt"
	"time"
)

// interruption is what ended a wait: its own condition, or something that made the condition moot.
type interruption uint8

const (
	// finished means the wait's own condition held.
	finished interruption = iota
	// targetExited means the service under test stopped by itself, so there was nothing left to wait
	// for.
	targetExited
)

// await is the one wait every start of the service under test goes through — a run's startup and its
// end alike — so each ends by the same rules.
//
// It checks until every drainPoll, first at once, and returns finished when until reports true. An
// error from until ends the wait with that error. It returns targetExited the moment exited closes,
// because a service that has stopped will never satisfy anything a start is waiting for, and waiting
// the condition out would only report the exit late as something else.
func (*egress) await(
	ctx context.Context,
	exited <-chan struct{},
	until func(context.Context) (bool, error),
) (interruption, error) {
	ticker := time.NewTicker(drainPoll)
	defer ticker.Stop()

	for {
		select {
		case <-exited:
			return targetExited, nil
		default:
		}

		done, err := until(ctx)
		if err != nil {
			return finished, err
		}

		if done {
			return finished, nil
		}

		select {
		case <-ctx.Done():
			return finished, fmt.Errorf("cancelled while waiting: %w", ctx.Err())
		case <-exited:
			return targetExited, nil
		case <-ticker.C:
		}
	}
}
