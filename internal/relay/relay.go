// Package relay is the program a relay container runs, and the framing both of its ends share.
//
// A containerised service reaches nothing but relays: each one listens on the service's private
// network under a dependency's names and pipes every connection to a listener on the host, where
// Stutter's in-process proxies observe it. A relay parses nothing and rewrites nothing. Its only
// addition to the byte stream is a fixed preamble at the start of each host-side connection, which
// names the port the service dialled and proves the connection came from one of this check's relays
// rather than from any other container that can reach the host.
//
// The package is compiled into the one stutter binary and runs inside a container holding only that
// binary, so it imports the standard library and the DNS message codec and nothing else: a lint rule
// enforces it.
package relay

import (
	"context"
	"fmt"
	"io"
)

const (
	// Ready opens the one line a relay prints on stdout once every socket it was given is bound. The
	// bound address follows it after a space.
	Ready = "relay: ready"
	// Verified opens the one line a verifier prints on stdout once the host answered its probe. The
	// address that answered follows it after a space.
	Verified = "relay: verified"
)

// Exit codes. The provisioner reads them only to name a failure: whatever the code, a relay that
// exits while a check needs it has failed the check.
const (
	// exitArgv is an argv the relay cannot read.
	exitArgv = 2
	// exitFailure is anything that went wrong once the argv was read: a bind, an accept, a dial, a
	// write to the signal connection.
	exitFailure = 4
)

// Run is the relay command: the mode word, then its flags. It returns the process exit code: 0 when
// ctx ends, which is how a stop signal arrives, and never before unless something failed.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return hostSystem().run(ctx, args, stdout, stderr)
}

// run dispatches on the mode word.
func (s system) run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	mode := ""
	if len(args) > 0 {
		mode = args[0]
	}

	switch mode {
	case modeServe:
		spec, err := Parse(args)
		if err != nil {
			return report(stderr, exitArgv, err)
		}

		return s.serve(ctx, spec, stdout, stderr)
	case modeVerify:
		verify, err := ParseVerify(args)
		if err != nil {
			return report(stderr, exitArgv, err)
		}

		return s.verify(ctx, verify, stdout, stderr)
	case modeCopy:
		copied, err := ParseCopy(args)
		if err != nil {
			return report(stderr, exitArgv, err)
		}

		if err := copyTree(copied.Src, copied.Dst); err != nil {
			return report(stderr, exitFailure, err)
		}

		return 0
	default:
		return report(stderr, exitArgv,
			fmt.Errorf("%w: unknown mode %q, want %s, %s or %s", errArgv, mode, modeServe, modeVerify, modeCopy))
	}
}

// report prints the one stderr line a failing relay leaves, and returns the exit code.
func report(stderr io.Writer, code int, err error) int {
	// stderr is the last place left to report to, so a failed write has nowhere to go.
	_, _ = fmt.Fprintf(stderr, "relay: %v\n", err)

	return code
}
