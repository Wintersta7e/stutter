package harness

import (
	"testing"
	"time"

	natsproxy "github.com/Wintersta7e/stutter/internal/proxy/nats"
)

// TestTheHoldBoundTakesItsLowestTerm: the hold is a tenth of the shortest timer that would redeliver
// or lose a message, raised to the floor a loaded machine's scheduling needs but never past half that
// timer, and names the timer that set it.
func TestTheHoldBoundTakesItsLowestTerm(t *testing.T) {
	t.Parallel()

	rows := []struct {
		name     string
		term     string
		pulls    []natsproxy.Pull
		deadline time.Duration
		want     time.Duration
	}{
		{name: "a deadline alone", deadline: time.Second, want: 250 * time.Millisecond, term: termDeadline},
		{
			name:     "a pull's expiry",
			deadline: time.Second,
			pulls:    []natsproxy.Pull{{Expires: 300 * time.Millisecond}},
			want:     150 * time.Millisecond,
			term:     termExpires,
		},
		{
			name:     "twice a pull's heartbeat",
			deadline: time.Second,
			pulls:    []natsproxy.Pull{{Expires: 300 * time.Millisecond, Heartbeat: 50 * time.Millisecond}},
			want:     50 * time.Millisecond,
			term:     termHeartbeat,
		},
		{name: "no deadline and no pull", want: 500 * time.Millisecond, term: termCeiling},
		{
			name:     "a no-wait pull",
			deadline: time.Second,
			pulls:    []natsproxy.Pull{{Expires: 10 * time.Millisecond, NoWait: true}},
			want:     250 * time.Millisecond,
			term:     termDeadline,
		},
		{name: "a push target", deadline: 2 * time.Second, want: 250 * time.Millisecond, term: termDeadline},
		{
			name:     "the shortest of several pulls",
			deadline: time.Second,
			pulls:    []natsproxy.Pull{{Expires: 400 * time.Millisecond}, {Expires: 200 * time.Millisecond}},
			want:     100 * time.Millisecond,
			term:     termExpires,
		},
		{name: "a tenth above the floor", deadline: 3 * time.Second, want: 300 * time.Millisecond, term: termDeadline},
	}

	t.Logf("%d rows", len(rows))

	if len(rows) == 0 {
		t.Fatal("no bounds to derive")
	}

	for _, row := range rows {
		var limit pullLimit
		for _, pull := range row.pulls {
			limit.lower(pull)
		}

		if bound, term := holdBound(row.deadline, limit); bound != row.want || term != row.term {
			t.Errorf("%s: bound = %s set by %s, want %s set by %s", row.name, bound, term, row.want, row.term)
		}
	}
}
