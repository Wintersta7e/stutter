package replay

import (
	"strconv"
	"strings"
)

// Exit is how the service under test ended an observed run: whether it stopped by itself, and what
// the engine said about it.
//
// It carries no time. The engine reports a container's start and finish as wall-clock strings, and a
// duration read from them moves with every step of the host's wall clock; the only clock a run reads is
// the monotonic one.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Exit struct {
	// Log is the path of the service's log, never its content: a log can hold anything the service
	// printed, and the path is enough to go and read it.
	Log string
	// After is the recorded sequence of the last message delivered before the exit, 0 before the first.
	After uint64
	// Code is the exit code the engine read before Stutter stopped anything. Read after a kill, it
	// would always be the kill's own.
	Code int
	// Restarts is how many times the engine restarted the service during the run.
	Restarts int
	// Exited is true when the service stopped by itself, false when Stutter stopped it.
	Exited bool
	// OOMKilled is true when the engine killed the service for running out of memory.
	OOMKilled bool
}

// Describe says how the service ended, leaving out every part that is zero or empty.
func (e Exit) Describe() string {
	var described strings.Builder

	described.WriteString("exited with code ")
	described.WriteString(strconv.Itoa(e.Code))

	if e.OOMKilled {
		described.WriteString(", OOM-killed")
	}

	if e.Restarts > 0 {
		described.WriteString(", restarted " + strconv.Itoa(e.Restarts) + " times")
	}

	if e.Log != "" {
		described.WriteString("; log: " + e.Log)
	}

	return described.String()
}

// ExitError stops a run because the service under test exited by itself where no outcome can be read
// from the run: before its first delivery, or in a clean run before every message was done.
type ExitError struct {
	Exit Exit
}

func (e *ExitError) Error() string {
	where := " after message #" + strconv.FormatUint(e.Exit.After, 10)
	if e.Exit.After == 0 {
		where = " before its first delivery"
	}

	return "the service under test " + e.Exit.Describe() + where
}
