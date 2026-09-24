package http

import (
	"maps"
	"slices"
)

// HostTally is what the stub answered for one external host, over the runs a check kept: the calls of
// the run that captured the script and how many of them a declared route answered, and the calls of
// every committed run that were off the script. A report lists it, because every answered host is a
// dependency the stub impersonated.
type HostTally struct {
	// Host is the host as effects render it.
	Host string
	// Calls and Routed are the capture run's calls to the host, and how many a declared route matched.
	Calls  int
	Routed int
	// OffScript counts calls, over every committed run, that the capture run never made.
	OffScript int
	// TLS says a counted call arrived over TLS.
	TLS bool
}

// count adds one call, as a tally of its own, to a run's tally.
func (r *Run) count(call HostTally) {
	if r.tally == nil {
		r.tally = make(map[string]*HostTally)
	}

	counted, known := r.tally[call.Host]
	if !known {
		counted = &HostTally{Host: call.Host}
		r.tally[call.Host] = counted
	}

	counted.add(call)
}

// keep merges a committed run's tally into the script's. The caller holds the script's lock.
func (s *Script) keep(run *Run) {
	if s.tally == nil {
		s.tally = make(map[string]*HostTally)
	}

	for host, counted := range run.tally {
		kept, known := s.tally[host]
		if !known {
			kept = &HostTally{Host: host}
			s.tally[host] = kept
		}

		kept.add(*counted)
	}
}

// add adds another tally of the same host to t.
func (t *HostTally) add(other HostTally) {
	t.Calls += other.Calls
	t.Routed += other.Routed
	t.OffScript += other.OffScript
	t.TLS = t.TLS || other.TLS
}

// Tally is what the stub answered per host over the script's committed runs, in host order.
func (s *Script) Tally() []HostTally {
	s.mu.Lock()
	defer s.mu.Unlock()

	tally := make([]HostTally, 0, len(s.tally))
	for _, host := range slices.Sorted(maps.Keys(s.tally)) {
		tally = append(tally, *s.tally[host])
	}

	return tally
}

// SumTallies adds tallies up per host — a check's, from its consumer checks' — TLS if any of them saw
// it, in host order.
func SumTallies(checks ...[]HostTally) []HostTally {
	summed := make(map[string]*HostTally)

	for _, check := range checks {
		for _, counted := range check {
			kept, known := summed[counted.Host]
			if !known {
				kept = &HostTally{Host: counted.Host}
				summed[counted.Host] = kept
			}

			kept.add(counted)
		}
	}

	tally := make([]HostTally, 0, len(summed))
	for _, host := range slices.Sorted(maps.Keys(summed)) {
		tally = append(tally, *summed[host])
	}

	return tally
}
