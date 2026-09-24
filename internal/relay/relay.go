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

const (
	// Ready opens the one line a relay prints on stdout once every socket it was given is bound. The
	// bound address follows it after a space.
	Ready = "relay: ready"
	// Verified opens the one line a verifier prints on stdout once the host answered its probe. The
	// address that answered follows it after a space.
	Verified = "relay: verified"
)
