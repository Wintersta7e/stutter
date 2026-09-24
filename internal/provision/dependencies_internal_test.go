package provision

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// The hand-built model's service names.
const (
	svcCache   = "cache"
	svcMigrate = "migrate"
	svcAPIDB   = "api-db"
	svcSeedA   = "seed-a"
	svcSeedB   = "seed-b"
	svcQueue   = "queue"
)

// endpoint is one TCP or UDP port of a hand-built dependency.
func endpoint(port uint16, protocol compose.Protocol) compose.Endpoint {
	return compose.Endpoint{Port: port, Protocol: protocol}
}

// classified is a classification holding one dependency of every role, as compose.Classify would give.
func classified() compose.Classification {
	one := func(port uint16, protocol compose.Protocol) []compose.Endpoint {
		return []compose.Endpoint{endpoint(port, protocol)}
	}

	return compose.Classification{Deps: []compose.Dependency{
		{Service: "db", Role: compose.RoleDatastore, InClosure: true, Endpoints: []compose.Endpoint{
			endpoint(5432, compose.ProtocolPG), endpoint(6000, compose.ProtocolOpaque),
		}},
		{Service: svcCache, Role: compose.RoleOther, InClosure: true, Endpoints: []compose.Endpoint{
			endpoint(6379, compose.ProtocolOpaque), endpoint(9000, compose.ProtocolHTTP),
			endpoint(5353, compose.ProtocolUDP),
		}},
		{Service: "bus", Role: compose.RoleBus, Endpoints: one(4222, compose.ProtocolNATS)},
		{Service: "peer", Role: compose.RoleSibling, Endpoints: one(8080, compose.ProtocolHTTP)},
		{Service: "idle", Role: compose.RoleUnused, Endpoints: one(7000, compose.ProtocolOpaque)},
		{Service: "sidecar", Role: compose.RoleAttached, Endpoints: one(7001, compose.ProtocolPG)},
		{Service: svcMigrate, Role: compose.RoleJob, InClosure: true},
	}}
}

// TestOnlyDatastoreAndOtherServicesStart keeps every other role out of the started set: a sibling, an
// unused service, the bus, an attached service and a job start nothing of their own, and a UDP port is
// never served.
func TestOnlyDatastoreAndOtherServicesStart(t *testing.T) {
	t.Parallel()

	started := startedDependencies(classified())

	names := make([]string, 0, len(started))
	for _, dep := range started {
		names = append(names, dep.Service)
	}

	postgres, opaque := endpointKeys(started)
	keys := slices.Concat(postgres, opaque)

	t.Logf("started=%d keys=%d", len(names), len(keys))

	if len(names) == 0 || len(keys) == 0 {
		t.Fatalf("started=%d keys=%d: the fixture starts nothing, so the test proves nothing", len(names), len(keys))
	}

	if want := []string{svcCache, "db"}; !slices.Equal(names, want) {
		t.Errorf("started = %v, want %v", names, want)
	}

	for _, key := range keys {
		for _, never := range []string{"bus:", "peer:", "idle:", "sidecar:", "migrate:", ":5353"} {
			if strings.Contains(key, never) {
				t.Errorf("key %q belongs to something that never starts or is never served", key)
			}
		}
	}
}

// TestKeysSplitByProtocol serves a Postgres endpoint with the Postgres proxy and every other TCP
// endpoint — an HTTP one included — with the opaque proxy.
func TestKeysSplitByProtocol(t *testing.T) {
	t.Parallel()

	postgres, opaque := endpointKeys(startedDependencies(classified()))

	if want := []string{"db:5432"}; !slices.Equal(postgres, want) {
		t.Errorf("postgres keys = %v, want %v", postgres, want)
	}

	if want := []string{"cache:6379", "cache:9000", "db:6000"}; !slices.Equal(opaque, want) {
		t.Errorf("opaque keys = %v, want %v", opaque, want)
	}
}

// dependsOnOf is a hand-built depends_on graph.
func dependsOnOf(graph map[string][]string) func(string) map[string]string {
	return func(service string) map[string]string {
		out := map[string]string{}
		for _, dep := range graph[service] {
			out[dep] = "service_started"
		}

		return out
	}
}

// TestLevelsFollowDependsOn starts what a service depends on a level before it; a dependency that does
// not start places nothing.
func TestLevelsFollowDependsOn(t *testing.T) {
	t.Parallel()

	levels := forwardLevels([]string{svcAPIDB, svcCache, "db", svcQueue}, dependsOnOf(map[string][]string{
		svcCache: {"db", "bus"},
		svcQueue: {svcCache},
		svcAPIDB: nil,
	}))

	want := [][]string{{svcAPIDB, "db"}, {svcCache}, {svcQueue}}
	if !slices.EqualFunc(levels, want, slices.Equal) {
		t.Errorf("levels = %v, want %v", levels, want)
	}
}

// TestJobsRunInTopologicalOrderTiesByName runs a job only after every job it depends on, and breaks
// ties by name; a job outside the target's closure is not discovered.
func TestJobsRunInTopologicalOrderTiesByName(t *testing.T) {
	t.Parallel()

	deps := []compose.Dependency{
		{Service: svcSeedB, Role: compose.RoleJob, InClosure: true},
		{Service: svcMigrate, Role: compose.RoleJob, InClosure: true},
		{Service: svcSeedA, Role: compose.RoleJob, InClosure: true},
		{Service: "stray", Role: compose.RoleJob, InClosure: false},
		{Service: "db", Role: compose.RoleDatastore, InClosure: true},
	}

	jobs := discoverJobs(deps, dependsOnOf(map[string][]string{
		svcSeedA: {svcMigrate, "db"},
		svcSeedB: {svcMigrate},
	}))

	if want := []string{svcMigrate, svcSeedA, svcSeedB}; !slices.Equal(jobs, want) {
		t.Errorf("jobs = %v, want %v", jobs, want)
	}
}

// TestNewDependenciesRefusesAZeroWait takes every wait from its one owner: a zero wait here is a
// caller that forgot one, never a default.
func TestNewDependenciesRefusesAZeroWait(t *testing.T) {
	t.Parallel()

	full := Waits{DependencyReady: time.Second, Job: time.Second, PostgresRestore: time.Second}

	for _, zero := range []struct {
		name  string
		waits Waits
	}{
		{name: "DependencyReady", waits: Waits{Job: full.Job, PostgresRestore: full.PostgresRestore}},
		{name: "Job", waits: Waits{DependencyReady: full.DependencyReady, PostgresRestore: full.PostgresRestore}},
		{name: "PostgresRestore", waits: Waits{DependencyReady: full.DependencyReady, Job: full.Job}},
	} {
		t.Run(zero.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewDependencies(DependencyConfig{
				Engine: &Engine{}, Model: &compose.Model{}, Network: &Network{},
				Helper: compose.Image{ID: "sha256:helper"}, Waits: zero.waits,
			})
			if !errors.Is(err, errDependencyConfig) || !strings.Contains(err.Error(), zero.name) {
				t.Errorf("NewDependencies() = %v, want a refusal naming %s", err, zero.name)
			}
		})
	}

	_, err := NewDependencies(DependencyConfig{
		Engine: &Engine{}, Model: &compose.Model{}, Network: &Network{}, Waits: full,
	})
	if !errors.Is(err, errDependencyConfig) || !strings.Contains(err.Error(), "helper") {
		t.Errorf("NewDependencies() without a helper image = %v, want a refusal naming the helper", err)
	}

	_, err = NewDependencies(DependencyConfig{
		Engine: &Engine{}, Model: &compose.Model{}, Network: &Network{}, Waits: full,
		Helper: compose.Image{ID: "sha256:helper"}, Classification: classified(),
	})
	if !errors.Is(err, errDependencyConfig) || !strings.Contains(err.Error(), "image") {
		t.Errorf("NewDependencies() with no images = %v, want a refusal naming a missing image", err)
	}
}
