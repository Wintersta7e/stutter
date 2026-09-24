package compose_test

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// storeService is a Postgres-speaking service several cases classify.
const storeService = "store"

// storeImage is the image ID lineage cases give a Postgres-like store.
const storeImage = "sha256:store"

// rolesModel has one service of every role but attached, and a profile-gated one.
const rolesModel = `{"name": "shop", "services": {
	"api": {
		"image": "example.test/api:1",
		"environment": {
			"NATS_URL": "nats://bus:4222",
			"DATABASE_URL": "postgres://db:5432/app",
			"MOCK_URL": "http://mock:8080/v1"
		},
		"depends_on": {
			"migrate": {"condition": "service_completed_successfully"},
			"db": {"condition": "service_healthy"}
		},
		"networks": {"default": null}
	},
	"migrate": {"image": "example.test/api:1", "command": ["migrate"], "networks": {"default": null}},
	"bus": {"image": "example.test/queue:2", "networks": {"default": null}},
	"db": {"image": "example.test/store:18", "networks": {"default": null}},
	"worker": {"build": {"context": "/project/worker"}, "networks": {"default": null}},
	"mock": {"image": "example.test/mock:1", "networks": {"default": null}},
	"island": {"image": "example.test/far:1", "networks": {"elsewhere": null}},
	"tools": {"image": "example.test/tools:1", "profiles": ["debug"], "networks": {"default": null}}
}}`

func classify(t *testing.T, model *compose.Model, images map[string]compose.Image,
	answers map[string]map[uint16]pg.Answer,
) compose.Classification {
	t.Helper()

	cls, err := compose.Classify(model, images, answers)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}

	return cls
}

func dependency(t *testing.T, cls compose.Classification, service string) compose.Dependency {
	t.Helper()

	for _, dep := range cls.Deps {
		if dep.Service == service {
			return dep
		}
	}

	t.Fatalf("no dependency %s in %+v", service, cls.Deps)

	return compose.Dependency{}
}

func endpoint(dep compose.Dependency, port uint16) (compose.Endpoint, bool) {
	for _, found := range dep.Endpoints {
		if found.Port == port && found.Protocol != compose.ProtocolUDP {
			return found, true
		}
	}

	return compose.Endpoint{}, false
}

func TestEveryRoleIsDerivedAndCounted(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, rolesModel)
	cls := classify(t, model, nil, nil)

	want := map[string]compose.Role{
		jobService: compose.RoleJob, busService: compose.RoleBus, "db": compose.RoleDatastore,
		workerService: compose.RoleSibling, mockService: compose.RoleOther, "island": compose.RoleUnused,
		"tools": compose.RoleOther,
	}

	classified := len(cls.Deps) + 1
	t.Logf("classified %d of %d", classified, len(model.Services()))

	if classified != len(model.Services()) || classified != len(want)+1 {
		t.Fatalf("classified %d of %d services, want %d", classified, len(model.Services()), len(want)+1)
	}

	roles := map[compose.Role]bool{}

	for _, dep := range cls.Deps {
		roles[dep.Role] = true

		if dep.Role != want[dep.Service] {
			t.Errorf("%s: role %s, want %s (evidence %v)", dep.Service, dep.Role, want[dep.Service], dep.Evidence)
		}
	}

	if len(roles) != 6 {
		t.Errorf("roles present = %v, want the six other than attached", slices.Collect(maps.Keys(roles)))
	}

	if db := dependency(t, cls, "db"); !db.Reachable || !db.InClosure || !slices.Contains(db.Names, "db") {
		t.Errorf("db = %+v, want reachable, in the closure, named db", db)
	}

	if started := cls.Started(); !slices.Equal(started, []string{target, "db", "migrate", "mock", "tools"}) {
		t.Errorf("Started = %v", started)
	}

	t.Run("attached", func(t *testing.T) {
		t.Parallel()

		attached := modelFrom(t, `{"name": "shop", "services": {"api": {"networks": {"default": null}},
			"side": {"network_mode": "service:api"}}}`)

		if _, err := compose.Classify(attached, nil, nil); !errors.Is(err, compose.ErrModel) ||
			!strings.Contains(err.Error(), "side") {
			t.Errorf("Classify with an attached service = %v, want ErrModel naming it", err)
		}
	})
}

func TestPgLineageMakesADatastore(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"volumes": [{"type": "bind", "source": "/project/dsn", "target": "/run/dsn"}],
			"networks": {"default": null}},
		"store": {"image": "example.test/store:18", "networks": {"default": null}}
	}}`)
	images := map[string]compose.Image{storeService: {ID: storeImage, Env: []string{"PG_MAJOR=18"}}}

	store := dependency(t, classify(t, model, images, nil), storeService)
	found, ok := endpoint(store, 5432)

	if store.Role != compose.RoleDatastore || !ok || found.Protocol != compose.ProtocolPG ||
		!strings.Contains(found.Evidence, "PG_MAJOR") || store.Lineage == "" {
		t.Errorf("store = %+v, want a datastore with pg on 5432 by lineage", store)
	}

	byCommand := map[string]compose.Image{storeService: {ID: storeImage, Cmd: []string{"/usr/bin/postgres"}}}
	if dep := dependency(t, classify(t, model, byCommand, nil), storeService); dep.Role != compose.RoleDatastore {
		t.Errorf("a postgres command = %+v, want a datastore", dep)
	}
}

// promotionModel has a Postgres-speaking service with no evidence but its exposed port.
const promotionModel = `{"name": "shop", "services": {
	"api": {"networks": {"default": null}},
	"store": {"image": "example.test/store:1", "expose": ["5433"], "networks": {"default": null}}
}}`

func TestAHandshakePromotesAnOpaqueEndpoint(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, promotionModel)

	before := classify(t, model, nil, nil)
	if !reflect.DeepEqual(before.Candidates(), map[string][]uint16{storeService: {5433}}) {
		t.Fatalf("Candidates = %v, want store's opaque 5433", before.Candidates())
	}

	after := classify(t, model, nil, map[string]map[uint16]pg.Answer{storeService: {5433: pg.AnswerPostgres}})
	store := dependency(t, after, storeService)
	found, _ := endpoint(store, 5433)

	if store.Role != compose.RoleDatastore || found.Protocol != compose.ProtocolPG || found.Evidence != "handshake" ||
		store.Answers[5433] != pg.AnswerPostgres {
		t.Errorf("store after the handshake = %+v, want a datastore promoted by the handshake", store)
	}

	none := classify(t, model, nil, map[string]map[uint16]pg.Answer{storeService: {5433: pg.AnswerNone}})
	if dep := dependency(t, none, storeService); dep.Role != compose.RoleOther {
		t.Errorf("store answering none = %+v, want it left other", dep)
	}
}

func TestAContradictedPgEndpointIsRefused(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"environment": {"DATABASE_URL": "postgres://db:5432/app"}, "networks": {"default": null}},
		"db": {"networks": {"default": null}}
	}}`)

	_, err := compose.Classify(model, nil, map[string]map[uint16]pg.Answer{"db": {5432: pg.AnswerOther}})
	if !errors.Is(err, compose.ErrHandshakeContradiction) || !errors.Is(err, compose.ErrModel) ||
		!strings.Contains(err.Error(), "db") || !strings.Contains(err.Error(), "5432") {
		t.Errorf("Classify = %v, want a handshake contradiction naming db and 5432", err)
	}
}

func TestADeclaredRoleReplacesTheDerivedOne(t *testing.T) {
	t.Parallel()

	file := declaring(t, t.TempDir(), "override.yaml")
	run := &fakeRun{whole: func([]string) ([]byte, int, error) {
		return []byte(`{"name": "shop", "services": {
			"api": {"environment": {"CACHE": "cache:6379"}, "networks": {"default": null}},
			"mock": {"networks": {"default": null}},
			"cache": {"networks": {"default": null}}
		}, "x-stutter": {"roles": {"mock": "datastore"}, "endpoints": {"cache": {"6379": "http"}}}}`), 0, nil
	}}

	model, err := compose.Parse(t.Context(), run.run, compose.Inputs{Service: target, Files: []string{file}})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	cls := classify(t, model, nil, nil)

	mock := dependency(t, cls, mockService)
	if found, ok := endpoint(mock, 5432); mock.Role != compose.RoleDatastore || !mock.Declared || !ok ||
		found.Protocol != compose.ProtocolPG {
		t.Errorf("mock = %+v, want a declared datastore given pg on 5432", mock)
	}

	cache := dependency(t, cls, "cache")
	if found, _ := endpoint(cache, 6379); found.Protocol != compose.ProtocolHTTP || found.Evidence != "x-stutter" {
		t.Errorf("cache 6379 = %+v, want the declared http", found)
	}

	want := []compose.Declaration{
		{File: file, Service: mockService, Key: "x-stutter.roles.mock", Value: "datastore"},
		{File: file, Service: "cache", Key: `x-stutter.endpoints.cache."6379"`, Value: "http"},
	}
	if !slices.Equal(cls.Declarations, want) {
		t.Errorf("Declarations = %+v, want %+v", cls.Declarations, want)
	}
}

func TestAnEncryptedDependencyEndpointIsRefused(t *testing.T) {
	t.Parallel()

	encrypted := func(cacheBody, mockBody string) string {
		return `{"name": "shop", "services": {
			"api": {"environment": {"CACHE": "rediss://:pw@cache:6380/0", "MOCK": "https://mock:8443/x"},
				"networks": {"default": null}},
			"cache": ` + cacheBody + `, "mock": ` + mockBody + `}}`
	}

	_, err := compose.Classify(modelFrom(t, encrypted(`{"networks": {"default": null}}`,
		`{"build": {"context": "/p"}, "networks": {"default": null}}`)), nil, nil)
	if !errors.Is(err, compose.ErrEncryptedEndpoint) || !strings.Contains(err.Error(), "cache") ||
		!strings.Contains(err.Error(), "6380") {
		t.Errorf("a started rediss dependency = %v, want ErrEncryptedEndpoint naming cache and 6380", err)
	}

	_, err = compose.Classify(modelFrom(t, encrypted(`{"build": {"context": "/p"}, "networks": {"default": null}}`,
		`{"networks": {"default": null}}`)), nil, nil)
	if !errors.Is(err, compose.ErrEncryptedEndpoint) || !strings.Contains(err.Error(), mockService) ||
		!strings.Contains(err.Error(), "8443") {
		t.Errorf("a started https dependency = %v, want ErrEncryptedEndpoint naming mock and 8443", err)
	}

	sibling := `{"build": {"context": "/p"}, "networks": {"default": null}}`
	if _, err := compose.Classify(modelFrom(t, encrypted(sibling, sibling)), nil, nil); err != nil {
		t.Errorf("encrypted endpoints on siblings = %v, want nil: siblings are never started", err)
	}
}

func TestCandidatesAreStillOpaqueEndpoints(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"environment": {"DATABASE_URL": "postgres://db:5432/app", "CACHE": "cache:6379"},
			"networks": {"default": null}},
		"db": {"expose": ["9187", "53/udp"], "networks": {"default": null}},
		"cache": {"networks": {"default": null}},
		"worker": {"build": {"context": "/p"}, "expose": ["7000"], "networks": {"default": null}}
	}}`)

	want := map[string][]uint16{"cache": {6379}, "db": {9187}}
	if got := classify(t, model, nil, nil).Candidates(); !reflect.DeepEqual(got, want) {
		t.Errorf("Candidates = %v, want %v", got, want)
	}
}

func TestPassTwoOnlyPromotesStartedServices(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"depends_on": {"db": {"condition": "service_started"}}, "networks": {"default": null}},
		"db": {"image": "example.test/store:18", "networks": {"default": null}},
		"worker": {"build": {"context": "/p"}, "networks": {"default": null}},
		"far": {"image": "example.test/store:18", "networks": {"elsewhere": null}}
	}}`)
	lineage := compose.Image{ID: storeImage, Env: []string{"PG_MAJOR=18"}}
	resolvable := map[string]compose.Image{"db": lineage, "worker": lineage, "far": lineage}

	first := classify(t, model, nil, nil)
	if dep := dependency(t, first, "db"); dep.Role != compose.RoleOther {
		t.Fatalf("db in pass 1 = %s, want other", dep.Role)
	}

	second := map[string]compose.Image{}

	for _, service := range first.Started() {
		if img, ok := resolvable[service]; ok {
			second[service] = img
		}
	}

	cls := classify(t, model, second, nil)

	for service, want := range map[string]compose.Role{
		"db": compose.RoleDatastore, workerService: compose.RoleSibling, "far": compose.RoleUnused,
	} {
		if dep := dependency(t, cls, service); dep.Role != want {
			t.Errorf("%s in pass 2 = %s, want %s", service, dep.Role, want)
		}
	}
}

func TestClassificationIsByteIdenticalAcrossCalls(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, rolesModel)
	first := fmt.Sprintf("%+v", classify(t, model, nil, nil))
	second := fmt.Sprintf("%+v", classify(t, model, nil, nil))

	if first != second || first == "" {
		t.Errorf("two classifications differ:\n%s\n%s", first, second)
	}
}
