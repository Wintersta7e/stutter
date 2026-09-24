package testgate

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// errExpect means one named test did not end the way a check expected.
var errExpect = errors.New("expected outcome")

// Expect reads go test -json from r and checks that the top-level or sub test named test ended
// exactly once, with action (pass, fail or skip) and, for a skip, a reason beginning reasonPrefix.
// A renamed test fails: no event names it. It returns the outcome it read.
func Expect(r io.Reader, test, action, reasonPrefix string) (string, error) {
	found, err := endingsOf(r, test)
	outcomes := found.outcomes

	switch {
	case err != nil:
		return "", err
	case len(outcomes) == 0:
		return "", fmt.Errorf("%w: %s did not run: no pass, fail or skip event names it", errExpect, test)
	case len(outcomes) > 1:
		return "", fmt.Errorf("%w: %s ended %d times (%s)", errExpect, test, len(outcomes),
			strings.Join(outcomes, ", "))
	case outcomes[0] != action:
		return "", fmt.Errorf("%w: %s: %s, want %s", errExpect, test, outcomes[0], action)
	}

	got := test + ": " + action
	if action != actionSkip {
		return got, nil
	}

	reason := skipReason(found.output)
	if !strings.HasPrefix(reason, reasonPrefix) {
		return "", fmt.Errorf("%w: %s skipped with %q, want a reason beginning %q", errExpect, test, reason,
			reasonPrefix)
	}

	return got + " (" + reason + ")", nil
}

// endings is every pass, fail and skip event of one test, and everything it printed.
type endings struct {
	outcomes []string
	output   []string
}

// endingsOf reads the endings of the test named test. A line that is not a JSON event is an error.
func endingsOf(r io.Reader, test string) (endings, error) {
	var (
		found  endings
		decode error
	)

	err := readEvents(r, func(e testEvent, err error) {
		switch {
		case err != nil:
			decode = err
		case e.Test != test:
		case e.Action == actionOutput:
			found.output = append(found.output, e.Output)
		case e.Action == actionPass, e.Action == actionFail, e.Action == actionSkip:
			found.outcomes = append(found.outcomes, e.Action)
		default:
		}
	})
	if err == nil {
		err = decode
	}

	return found, err
}
