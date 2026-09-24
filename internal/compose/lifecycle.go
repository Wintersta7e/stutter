package compose

import (
	"fmt"
	"slices"
	"time"
)

// Healthcheck is a service's `healthcheck` as compose normalised it. A zero timing or Retries is
// unset: the image's or the engine's default applies.
type Healthcheck struct {
	// Test is the test as compose wrote it, unescaped as every value a container sees:
	// `["CMD", argv…]`, `["CMD-SHELL", command]` or `["NONE"]`.
	Test []string
	// Interval is `healthcheck.interval`.
	Interval time.Duration
	// Timeout is `healthcheck.timeout`.
	Timeout time.Duration
	// StartPeriod is `healthcheck.start_period`.
	StartPeriod time.Duration
	// StartInterval is `healthcheck.start_interval`.
	StartInterval time.Duration
	// Retries is `healthcheck.retries`.
	Retries int
	// Disabled is `disable: true` or a `["NONE"]` test.
	Disabled bool
}

// Healthcheck returns service's healthcheck, and whether the service sets the key at all.
func (m *Model) Healthcheck(service string) (Healthcheck, bool, error) {
	svc, err := m.serviceOf(service)
	if err != nil {
		return Healthcheck{}, false, err
	}

	check := svc.Healthcheck
	if check == nil {
		return Healthcheck{}, false, nil
	}

	out := Healthcheck{
		Test:     unescapeAll(check.Test),
		Retries:  int(check.Retries),
		Disabled: check.Disable || slices.Equal(check.Test, []string{"NONE"}),
	}

	timings := []struct {
		into *time.Duration
		key  string
		text string
	}{
		{into: &out.Interval, key: "interval", text: check.Interval},
		{into: &out.Timeout, key: "timeout", text: check.Timeout},
		{into: &out.StartPeriod, key: "start_period", text: check.StartPeriod},
		{into: &out.StartInterval, key: "start_interval", text: check.StartInterval},
	}

	for _, timing := range timings {
		if timing.text == "" {
			continue
		}

		value, err := time.ParseDuration(timing.text)
		if err != nil {
			return Healthcheck{}, true, fmt.Errorf("%w: service %s: healthcheck.%s is not a duration",
				ErrModel, service, timing.key)
		}

		*timing.into = value
	}

	return out, true, nil
}

// Stop is how a service asks to be stopped. Each field is set only when its compose key is:
// otherwise the image's signal and the engine's grace period apply.
type Stop struct {
	// Signal is `stop_signal`.
	Signal string
	// Grace is `stop_grace_period`.
	Grace time.Duration
	// SignalSet reports that `stop_signal` is set.
	SignalSet bool
	// GraceSet reports that `stop_grace_period` is set.
	GraceSet bool
}

// Stop returns service's stop signal and grace period.
func (m *Model) Stop(service string) (Stop, error) {
	svc, err := m.serviceOf(service)
	if err != nil {
		return Stop{}, err
	}

	out := Stop{Signal: unescape(svc.StopSignal), SignalSet: svc.StopSignal != ""}

	if svc.StopGracePeriod != "" {
		grace, err := time.ParseDuration(svc.StopGracePeriod)
		if err != nil {
			return Stop{}, fmt.Errorf("%w: service %s: stop_grace_period is not a duration", ErrModel, service)
		}

		out.Grace, out.GraceSet = grace, true
	}

	return out, nil
}
