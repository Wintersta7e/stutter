package nats

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
)

// ErrUnsupportedBus means the service under test reaches for a bus the one embedded server cannot
// stand in for: over TLS or websockets, in a JetStream domain, or with replicated streams. Such a
// service either cannot be observed or cannot start, and either reads as a handler that did nothing.
var ErrUnsupportedBus = errors.New("the service needs a bus the embedded server cannot stand in for")

// errCodeReplicas is the bus's error code for a stream or bucket asking for more replicas than a
// single server has.
const errCodeReplicas = 10074

// RefuseURLs refuses, before any start, a service configured to reach the bus over TLS or websockets:
// a tls://, ws:// or wss:// URL whose host is one of the bus's names, in any variable of its
// environment. The error names every such variable, in name order, and never a value — a bus URL can
// carry credentials.
func RefuseURLs(env map[string]string, busNames []string) error {
	var refused []string

	for _, name := range slices.Sorted(maps.Keys(env)) {
		if unsupportedTransport(env[name], busNames) {
			refused = append(refused, name)
		}
	}

	if len(refused) == 0 {
		return nil
	}

	return fmt.Errorf("%w: %s reach the bus over TLS or websockets, which the embedded bus does not serve",
		ErrUnsupportedBus, strings.Join(refused, ", "))
}

// unsupportedTransport reports whether any comma-separated URL in value reaches a bus name over TLS or
// websockets.
func unsupportedTransport(value string, busNames []string) bool {
	for raw := range strings.SplitSeq(value, ",") {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil {
			continue
		}

		switch strings.ToLower(parsed.Scheme) {
		case "tls", "ws", "wss":
		default:
			continue
		}

		if slices.ContainsFunc(busNames, func(bus string) bool { return strings.EqualFold(bus, parsed.Hostname()) }) {
			return true
		}
	}

	return false
}

// jetStreamDomain reports the domain a JetStream API subject addresses, as in $JS.<domain>.API.INFO.
func jetStreamDomain(subject string) (string, bool) {
	const tokens = 4

	parts := strings.SplitN(subject, ".", tokens)
	if len(parts) < tokens || parts[0] != "$JS" || parts[1] == "API" || parts[2] != "API" {
		return "", false
	}

	return parts[1], true
}

// refusedReplicas names what a replicated create was for: the request's last token, which for a
// key/value bucket is its backing stream KV_<bucket>.
func refusedReplicas(request string) string {
	last := request[strings.LastIndexByte(request, '.')+1:]
	if bucket, isBucket := strings.CutPrefix(last, "KV_"); isBucket {
		return "bucket " + bucket
	}

	return "stream " + last
}
