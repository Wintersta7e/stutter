package cli

// The command line's user-visible sentences, gathered in one file so they can be reviewed and
// reworded together. Tests assert the facts each one must state, never its wording.

// keptText follows the kept check's ID: what was kept, and the one command that removes it.
func keptText(checkID string) string {
	return "the check's resources are kept, stopped, until `stutter clean --check " + checkID + "` removes them"
}

// interruptText is printed once, at the first interrupt.
const interruptText = "teardown under way; a second interrupt exits at once and leaves the rest for `stutter clean`"
