//go:build linux

package provision

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// The tests stand a closed ledger in for a dead owner, so its lock must be free the moment
// closeLedger returns — even while other goroutines of this process spawn children, each of which
// holds a copy of every open descriptor until its exec.
func TestAClosedLedgerIsFreeWhileChildrenSpawn(t *testing.T) {
	t.Parallel()

	// Attached, so concurrent calls log to one discarding sink instead of the pre-attach buffer.
	runner := newRunner(shimEnv(t, "exit 0\n"), nil)
	runner.attach("", nil, false)

	ctx, stop := context.WithCancel(t.Context())

	var (
		spawners sync.WaitGroup
		spawned  atomic.Int64
	)

	for range 1 {
		spawners.Go(func() {
			for ctx.Err() == nil {
				_, err := runner.call(ctx, request{verb: verbInfo, args: []arg{{val: infoTemplate}}})
				if err == nil {
					spawned.Add(1)
				} else if ctx.Err() == nil {
					t.Errorf("spawn: %v", err)
				}
			}
		})
	}

	const trials = 100

	held := closeAndProbe(t, newState(t), trials)

	stop()
	spawners.Wait()

	t.Logf("trials=%d still locked=%d children spawned=%d", trials, held, spawned.Load())

	if spawned.Load() == 0 {
		t.Error("no child was spawned while the ledgers closed: the test proves nothing")
	}

	if held != 0 {
		t.Errorf("%d of %d closed ledgers were still locked", held, trials)
	}
}

// closeAndProbe creates and closes trials ledgers in state, counting those whose lock a second
// open file description cannot take right after closeLedger returns.
func closeAndProbe(t *testing.T, state string, trials int) int {
	t.Helper()

	held := 0

	for range trials {
		id, err := newCheckID()
		if err != nil {
			t.Error(err)

			return held
		}

		led, err := createLedger(state, defaultHostFS(), testHeader(id))
		if err != nil {
			t.Error(err)

			return held
		}

		closeLedger(t, led)

		if !lockable(t, led.path) {
			held++
		}
	}

	return held
}
