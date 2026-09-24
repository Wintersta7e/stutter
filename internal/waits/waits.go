// Package waits holds the bounds on every setup step a compose check waits for, each stated once so
// the steps that wait on it import the value rather than retype it.
//
// A wait is spent in full only when something has already failed: a step that works finishes long
// before its bound. Exceeding one ends the check in setup, exit 3, naming the step. Every value is an
// assumed, generous multiple of the baseline measured for its step, never a measurement itself, and
// every one is timed on the monotonic clock.
package waits

import "time"

const (
	// DependencyReady bounds a dependency coming up: a seed start, or an unparsed dependency's restore —
	// its depends_on condition, then each relayed port accepting a TCP connect on Stutter's direct path.
	// Measured: a Postgres seed was ready in 1.2 to 1.8 s. Exceeded: E6 for a seed, E17 for a restore.
	DependencyReady = 120 * time.Second
	// Job bounds one job's run to completion. The baseline is the user's own job, which Stutter cannot
	// measure in advance. Exceeded: E7.
	Job = 10 * time.Minute
	// PostgresRestore bounds a restored Postgres answering its readiness probe. Measured: 0.05 s or
	// less. Exceeded: E17.
	PostgresRestore = 30 * time.Second
	// RelayReady bounds a relay container printing its ready line. Measured: 0.20 to 0.27 s. Exceeded:
	// E5.
	RelayReady = 5 * time.Second
	// Verifier bounds the host-address verification, about one relay's run. Measured: 0.20 to 0.27 s.
	// Exceeded: E6.
	Verifier = 5 * time.Second
	// VerifierDial bounds the verifier's own dial, inside Verifier so a dial that hangs is reported as
	// the dial and not as the verifier.
	VerifierDial = 3 * time.Second
	// ProxyDrain bounds one run's proxies draining once the service is gone. Measured: a SIGKILL closes
	// the service's sockets at once, 151 ms kill to wait. Exceeded: E23.
	ProxyDrain = 5 * time.Second
)
