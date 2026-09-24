package nats

import "strings"

// apiPrefix opens every JetStream API request subject, pull requests included.
const apiPrefix = "$JS.API."

// Requests is told of every JetStream API request the service under test sends.
//
// It exists for a start that publishes nothing. Such a start can only tell a service that is still
// initialising from one that has given up by what it has done on the bus, and until the service has
// touched JetStream at all, its silence says nothing: a service that initialises for longer than the
// settle period before its first request would otherwise be read as one that never creates its stream.
// The first request is where "quiet" can begin.
//
// It is observation only: it changes no byte, no effect and no setup count.
type Requests interface {
	Requested(subject string)
}

// noteRequest reports a client publish that is a JetStream API request. Acknowledgements and core
// publishes are not: neither is the service asking JetStream for anything.
func (s *session) noteRequest(subject string) {
	if s.opts.Requests == nil || !strings.HasPrefix(subject, apiPrefix) {
		return
	}

	s.opts.Requests.Requested(subject)
}
