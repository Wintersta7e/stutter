package compose

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// MinVersion is the oldest compose plugin Stutter supports. Every parse shape this package reads was
// measured on it and on the newest release.
const MinVersion = "2.29.7"

// versionPattern reads a leading `MAJOR.MINOR.PATCH`, with an optional `v`; a suffix such as
// `-desktop.1` is ignored.
var versionPattern = regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)`)

// CheckVersion refuses a compose plugin below MinVersion. found is the version the engine reports
// for its compose plugin; empty means it reports none.
func CheckVersion(found string) error {
	found = strings.TrimSpace(found)
	if found == "" {
		return fmt.Errorf("%w: no compose plugin found; Stutter needs compose %s or later",
			ErrUnsupportedCompose, MinVersion)
	}

	got, ok := parseVersion(found)
	if !ok {
		return fmt.Errorf("%w: compose version %q is not MAJOR.MINOR.PATCH; Stutter needs compose %s or later",
			ErrUnsupportedCompose, found, MinVersion)
	}

	floor, _ := parseVersion(MinVersion)
	for index := range got {
		if got[index] != floor[index] {
			if got[index] < floor[index] {
				return fmt.Errorf("%w: compose %s is below %s, the oldest Stutter supports",
					ErrUnsupportedCompose, found, MinVersion)
			}

			return nil
		}
	}

	return nil
}

func parseVersion(text string) ([3]int, bool) {
	match := versionPattern.FindStringSubmatch(text)
	if match == nil {
		return [3]int{}, false
	}

	var out [3]int

	for index := range out {
		value, err := strconv.Atoi(match[index+1])
		if err != nil {
			return [3]int{}, false
		}

		out[index] = value
	}

	return out, true
}
