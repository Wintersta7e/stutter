package cli

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// runGrammar is the published run line, whole.
var runGrammar = regexp.MustCompile(`^stutter: run consumer=(\S*) run=(probe|discovery|clean|fault|shrink) ` +
	`fault=(\S+) elapsed=(\S+) span=(\S+)$`)

var errRunFailed = errors.New("the dependency died")

// brokenRun is the run the scripted session fails.
const brokenRun = "broken"

// scriptedSession is an inner session whose reset takes as long as it is told and whose runs end as
// scripted.
type scriptedSession struct {
	fails func(name string) bool
	reset time.Duration
	span  time.Duration
}

func (s *scriptedSession) Reset(context.Context) error {
	time.Sleep(s.reset)

	return nil
}

func (s *scriptedSession) Run(_ context.Context, name string, _ replay.Mutation, _ []uint64) (replay.Result, error) {
	if s.fails != nil && s.fails(name) {
		return replay.Result{}, errRunFailed
	}

	return replay.Result{Span: s.span}, nil
}

// TestEveryRunPrintsOneRunLine: every run of a consumer check, failed or not, is one line on stderr,
// named by what the check asked for rather than by what the check calls its pass.
func TestEveryRunPrintsOneRunLine(t *testing.T) {
	t.Parallel()

	var stderr strings.Builder

	inner := &scriptedSession{span: 3 * time.Millisecond, fails: func(name string) bool { return name == brokenRun }}
	timed := &timedSession{inner: inner, consumer: testService, out: newLockedWriter(&stderr)}

	runs := []struct {
		mutation replay.Mutation
		name     string
		retain   []uint64
	}{
		{replay.Clean{}, "clean-1", nil},
		{replay.Duplicate{Seq: 1}, "duplicate-3", nil},
		{replay.Clean{}, "shrink-clean-4", []uint64{1}},
		{replay.Duplicate{Seq: 1}, "shrink-faulted-5", []uint64{1}},
		{replay.CrashBeforeAck{Seq: 2, Times: 2}, brokenRun, nil},
	}

	for _, run := range runs {
		if err := timed.Reset(t.Context()); err != nil {
			t.Fatalf("Reset: %v", err)
		}

		result, err := timed.Run(t.Context(), run.name, run.mutation, run.retain)
		if run.name == brokenRun != errors.Is(err, errRunFailed) {
			t.Errorf("%s: Run error = %v, want the inner session's own", run.name, err)
		}

		if err == nil && result.Span != inner.span {
			t.Errorf("%s: Run changed the inner result: span %v", run.name, result.Span)
		}
	}

	lines := strings.Split(strings.TrimSuffix(stderr.String(), "\n"), "\n")
	classes := make([]string, 0, len(lines))

	for _, line := range lines {
		parts := runGrammar.FindStringSubmatch(line)
		if parts == nil {
			t.Errorf("run line %q does not match the published grammar", line)

			continue
		}

		if parts[1] != testService {
			t.Errorf("run line %q names consumer %q, want orders", line, parts[1])
		}

		classes = append(classes, parts[2])
	}

	t.Logf("lines=%d", len(lines))

	if got, want := strings.Join(classes, ","), "clean,fault,shrink,shrink,fault"; got != want {
		t.Errorf("run classes = %s, want %s", got, want)
	}

	if len(lines) != len(runs) {
		t.Fatalf("%d run lines for %d runs:\n%s", len(lines), len(runs), stderr.String())
	}
}

// TestARunLineTimesTheResetToo: Stutter's per-run overhead includes the restore before the run.
func TestARunLineTimesTheResetToo(t *testing.T) {
	t.Parallel()

	var stderr strings.Builder

	const reset = 50 * time.Millisecond

	timed := &timedSession{inner: &scriptedSession{reset: reset}, out: newLockedWriter(&stderr)}

	if err := timed.Reset(t.Context()); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if _, err := timed.Run(t.Context(), "clean-1", replay.Clean{}, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	parts := runGrammar.FindStringSubmatch(strings.TrimSpace(stderr.String()))
	if parts == nil {
		t.Fatalf("no run line in %q", stderr.String())
	}

	elapsed, err := time.ParseDuration(parts[4])
	if err != nil {
		t.Fatalf("elapsed %q: %v", parts[4], err)
	}

	if elapsed < reset {
		t.Errorf("elapsed = %v, want at least the %v reset", elapsed, reset)
	}
}

// TestStderrLinesDoNotInterleave: every compose-branch line is written whole, whichever goroutine
// writes it.
func TestStderrLinesDoNotInterleave(t *testing.T) {
	t.Parallel()

	const (
		writers = 8
		each    = 50
	)

	var (
		stderr strings.Builder
		group  sync.WaitGroup
	)

	out := newLockedWriter(&stderr)

	for writer := range writers {
		group.Go(func() {
			for line := range each {
				out.line(fmt.Sprintf("stutter: writer=%d line=%d %s", writer, line, strings.Repeat("x", 64)))
			}
		})
	}

	group.Wait()

	whole := regexp.MustCompile(`^stutter: writer=\d+ line=\d+ x{64}$`)
	lines := strings.Split(strings.TrimSuffix(stderr.String(), "\n"), "\n")

	for _, line := range lines {
		if !whole.MatchString(line) {
			t.Errorf("line %q was interleaved with another", line)
		}
	}

	if len(lines) != writers*each {
		t.Errorf("%d lines, want %d", len(lines), writers*each)
	}
}

// TestTheStderrLinesNameWhatTheyPromise: the check-start, log, keep and interrupt lines carry what a
// user and a test read from them.
func TestTheStderrLinesNameWhatTheyPromise(t *testing.T) {
	t.Parallel()

	const (
		checkID = "c0ffee42"
		dir     = "/tmp/stutter-c0ffee42"
		log     = dir + "/logs/target-1.log"
	)

	cases := map[string][]string{
		checkStartLine(checkID, dir): {"stutter: check ", "id=" + checkID, "dir=" + dir},
		logLine(log):                 {"stutter: log " + log},
		keepLine(checkID):            {"stutter: keep ", "id=" + checkID, "stutter clean --check " + checkID},
		interruptLine():              {"stutter: ", "second interrupt", "stutter clean"},
		sweepLine(provision.SweepResult{
			Swept: []string{"a"}, Unledgered: map[string][]provision.Listed{"b": {{}, {}}},
		}): {"stutter: sweep ", "swept=1", "unledgered=b:2"},
	}

	for line, wants := range cases {
		for _, want := range wants {
			if !strings.Contains(line, want) {
				t.Errorf("line %q does not carry %q", line, want)
			}
		}
	}

	t.Logf("lines=%d", len(cases))
}
