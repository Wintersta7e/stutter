package compose_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
)

func TestHealthcheckIsReadAsComposeWroteIt(t *testing.T) {
	t.Parallel()

	for _, release := range releases {
		model := modelFrom(t, string(golden(t, release)))

		cases := []struct {
			service string
			want    compose.Healthcheck
			set     bool
		}{
			{service: "db", set: true, want: compose.Healthcheck{
				Test:     []string{"CMD", "pg_isready", "-U", "x"},
				Interval: 5 * time.Second, Timeout: 3 * time.Second, Retries: 4,
				StartPeriod: 10 * time.Second, StartInterval: time.Second,
			}},
			{
				service: cacheService,
				set:     true,
				want:    compose.Healthcheck{Test: []string{"CMD-SHELL", "redis-cli ping"}},
			},
			{service: workerService, set: true, want: compose.Healthcheck{Disabled: true}},
			{service: jobService, set: true, want: compose.Healthcheck{Test: []string{"NONE"}, Disabled: true}},
			{service: toolsService},
			{service: target},
		}

		for _, tc := range cases {
			got, set, err := model.Healthcheck(tc.service)
			if err != nil {
				t.Errorf("compose %s: Healthcheck(%s): %v", release, tc.service, err)

				continue
			}

			if set != tc.set || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("compose %s: Healthcheck(%s) = %+v, %v; want %+v, %v",
					release, tc.service, got, set, tc.want, tc.set)
			}
		}

		if _, _, err := model.Healthcheck(absentService); err == nil || !strings.Contains(err.Error(), absentService) {
			t.Errorf("compose %s: Healthcheck(ghost) = %v, want an error naming the service", release, err)
		}
	}
}

func TestAnUnparseableHealthcheckTimingIsNamed(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"services": {"api": {"image": "a",
		"healthcheck": {"test": ["CMD", "true"], "interval": "soon"}}}}`)

	_, _, err := model.Healthcheck(target)
	if err == nil || !strings.Contains(err.Error(), "healthcheck.interval") || strings.Contains(err.Error(), "soon") {
		t.Errorf("Healthcheck = %v, want an error naming the key and not the value", err)
	}
}

func TestStopSignalAndGraceAreReadWhenSet(t *testing.T) {
	t.Parallel()

	for _, release := range releases {
		model := modelFrom(t, string(golden(t, release)))

		cases := []struct {
			service string
			want    compose.Stop
		}{
			{service: "db", want: compose.Stop{
				Signal: "SIGINT", Grace: 90 * time.Second, SignalSet: true, GraceSet: true,
			}},
			{service: cacheService, want: compose.Stop{Signal: "SIGTERM", SignalSet: true}},
			{service: workerService},
		}

		for _, tc := range cases {
			got, err := model.Stop(tc.service)
			if err != nil || got != tc.want {
				t.Errorf("compose %s: Stop(%s) = %+v, %v; want %+v", release, tc.service, got, err, tc.want)
			}
		}

		if _, err := model.Stop(absentService); err == nil || !strings.Contains(err.Error(), absentService) {
			t.Errorf("compose %s: Stop(ghost) = %v, want an error naming the service", release, err)
		}
	}
}
