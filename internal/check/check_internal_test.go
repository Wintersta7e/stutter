package check

import (
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// TestNoMutationWithholdsMoreThanTheCrashLoop: the delivery cap is sized for the crash loop, the most
// acknowledgements any fault withholds. A fault that withheld more would have its message capped before
// the delivery that goes through, and every such run would read as a message never settled.
func TestNoMutationWithholdsMoreThanTheCrashLoop(t *testing.T) {
	t.Parallel()

	permissive := &check{opts: Options{Config: policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{"orders.created", "orders.shipped"},
		AckWait:        time.Second,
		MaxDeliver:     -1,
		MaxAckPending:  10,
	}}}

	most := 0

	for _, fault := range faultOrder {
		mutation, buildable := permissive.mutationFor(fault, 1)
		if !buildable {
			continue
		}

		withheld := 0

		for delivery := uint64(1); delivery <= harness.DeliveryCap; delivery++ {
			if mutation.WithholdAck(replay.Message{Seq: 1, Deliveries: delivery}) {
				withheld++
			}
		}

		t.Logf("%s withholds %d of %d deliveries", fault, withheld, harness.DeliveryCap)

		most = max(most, withheld)
	}

	if most != policy.CrashLoopWithheld {
		t.Errorf("the most any fault withholds is %d, want the crash loop's %d", most, policy.CrashLoopWithheld)
	}
}
