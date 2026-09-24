package harness

import (
	"strconv"
	"strings"
)

// Endpoint keys: the names the invocation listeners are opened under, and a proxy failure is reported
// under. A dependency's key is EndpointKey's "<service>:<port>"; every reserved key has no colon, so the
// two can never collide.
const (
	// KeyBus is the bus.
	KeyBus = "bus"
	// KeyBusMonitor is the bus's monitoring port, piped and never recorded.
	KeyBusMonitor = "bus-monitor"
	// KeyHTTP is the cleartext HTTP stub.
	KeyHTTP = "http"
	// KeyHTTPS is the TLS HTTP stub.
	KeyHTTPS = "https"
	// KeyCatchAll is every other port of the stub relay.
	KeyCatchAll = "catch-all"
	// KeyDNSSignal is the stub relay's report of the DNS queries it answered.
	KeyDNSSignal = "dns-signal"
	// KeySeedBus is the bus as jobs reach it during the seed phase, unrecorded.
	KeySeedBus = "seed-bus"
	// KeyVerify is the verification listener, open only until the host's address is verified.
	KeyVerify = "verify"
	// keyDatabase is the database on the Go-caller path, where it has no service name of its own.
	keyDatabase = "database"
)

// Ports a relay pipes to a reserved key, stated once here.
const (
	// HTTPPort is the stub relay's cleartext HTTP port.
	HTTPPort uint16 = 80
	// HTTPSPort is the stub relay's TLS port.
	HTTPSPort uint16 = 443
	// MonitorPort is the bus's monitoring port.
	MonitorPort uint16 = 8222
)

// EndpointKey names a dependency's endpoint: its compose service and the container port the service
// dials. It is the one place the key's format is written.
func EndpointKey(service string, port uint16) string {
	return service + ":" + strconv.FormatUint(uint64(port), 10)
}

// ParseEndpointKey splits an endpoint key. It reports false for anything EndpointKey could not have
// written: no service, no colon, or a port outside 1–65535.
func ParseEndpointKey(key string) (string, uint16, bool) {
	at := strings.LastIndexByte(key, ':')
	if at <= 0 {
		return "", 0, false
	}

	port, err := strconv.ParseUint(key[at+1:], 10, 16)
	if err != nil || port == 0 {
		return "", 0, false
	}

	return key[:at], uint16(port), true
}

// Mode is how the service's containers reach the host's listeners.
type Mode uint8

const (
	// ModeGateway reaches them at the dependency network's gateway, an address of the host's own.
	ModeGateway Mode = iota + 1
	// ModeHostAlias reaches them at the address the engine resolves host.docker.internal to, which
	// forwards to the host's loopback.
	ModeHostAlias
)

// String names the mode as the report prints it.
func (m Mode) String() string {
	switch m {
	case ModeGateway:
		return "gateway"
	case ModeHostAlias:
		return "host-alias"
	default:
		return "unset"
	}
}
