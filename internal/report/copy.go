package report

import "github.com/Wintersta7e/stutter/internal/gate"

// The compose path's user-visible sentences, gathered in one file so they can be reviewed and reworded
// together. Tests assert the facts each one must state, never its wording.

// siblingReservation holds back a finding because another consumer of the same service was unstable:
// it names that consumer and how its runs differed.
func siblingReservation(consumer string, class gate.Class) string {
	return "held back: consumer " + consumer + " did not behave the same way twice (" + string(class) +
		"), so the service may be unstable under this consumer too"
}
