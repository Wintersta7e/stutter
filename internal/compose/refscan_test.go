package compose_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// scanModel builds a model whose target names the bus, depends on a job migrate, and reaches an
// other dependency mock; each argument is a JSON environment object.
func scanModel(targetEnv, jobEnv, mockEnv string) string {
	return `{"name": "shop", "services": {
		"api": {"environment": ` + targetEnv + `,
			"depends_on": {"migrate": {"condition": "service_completed_successfully"}},
			"networks": {"default": null}},
		"migrate": {"environment": ` + jobEnv + `, "networks": {"default": null}},
		"mock": {"environment": ` + mockEnv + `, "networks": {"default": null}},
		"bus": {"expose": ["4222"], "networks": {"default": null}},
		"db": {"expose": ["5432"], "networks": {"default": null}}
	}}`
}

func TestAnExternalDatastoreHostIsRefusedForEveryViewer(t *testing.T) {
	t.Parallel()

	const bus = `"NATS_URL": "nats://bus:4222"`

	cases := []struct {
		name, model, service, host string
	}{
		{
			name: "target DSN outside the model", service: target, host: "db.corp.test",
			model: scanModel(`{`+bus+`, "DATABASE_URL": "postgres://db.corp.test:5432/app"}`, `{}`, `{}`),
		},
		{
			name: "job DSN on the host gateway", service: jobService, host: "host.docker.internal",
			model: scanModel(`{`+bus+`}`, `{"DATABASE_URL": "postgres://host.docker.internal:15432/app"}`, `{}`),
		},
		{
			name: "job DSN on localhost", service: jobService, host: "localhost",
			model: scanModel(`{`+bus+`}`, `{"PGHOST": "localhost"}`, `{}`),
		},
		{
			name: "other dependency on an IP literal", service: mockService, host: "10.0.0.7",
			model: scanModel(`{`+bus+`, "MOCK": "http://mock:8080"}`, `{}`, `{"CACHE": "redis://10.0.0.7:6379"}`),
		},
	}

	for _, tc := range cases {
		_, err := compose.Classify(modelFrom(t, tc.model), nil, nil)
		if !errors.Is(err, compose.ErrModel) || !strings.Contains(err.Error(), tc.service) ||
			!strings.Contains(err.Error(), tc.host) {
			t.Errorf("%s: Classify = %v, want ErrModel naming %s and %s", tc.name, err, tc.service, tc.host)
		}
	}

	inside := scanModel(`{`+bus+`, "DATABASE_URL": "postgres://db:5432/app"}`, `{"PGHOST": "db"}`, `{}`)
	if _, err := compose.Classify(modelFrom(t, inside), nil, nil); err != nil {
		t.Errorf("datastore references inside the model = %v, want accepted", err)
	}
}

func TestExactlyOneBusIdentity(t *testing.T) {
	t.Parallel()

	twoBuses := func(env string) string {
		return `{"name": "shop", "services": {
			"api": {"environment": ` + env + `, "networks": {"default": null}},
			"bus-a": {"expose": ["4222"], "networks": {"default": null}},
			"bus-b": {"expose": ["4222"], "networks": {"default": null}}
		}}`
	}

	none := `{"name": "shop", "services": {"api": {"networks": {"default": null}}}}`
	if _, err := compose.Classify(modelFrom(t, none), nil, nil); !errors.Is(err, compose.ErrModel) ||
		!strings.Contains(err.Error(), "no bus identity") {
		t.Errorf("no bus = %v, want ErrModel for no bus identity", err)
	}

	two := twoBuses(`{"EVENTS": "nats://bus-a:4222", "AUDIT": "nats://bus-b:4222"}`)
	if _, err := compose.Classify(modelFrom(t, two), nil, nil); !errors.Is(err, compose.ErrModel) ||
		!strings.Contains(err.Error(), "several bus identities") || !strings.Contains(err.Error(), "bus-a") ||
		!strings.Contains(err.Error(), "bus-b") {
		t.Errorf("two buses = %v, want ErrModel naming both", err)
	}

	unreferenced := twoBuses(`{}`)
	if _, err := compose.Classify(modelFrom(t, unreferenced), nil, nil); !errors.Is(err, compose.ErrModel) {
		t.Errorf("two bus services the target does not name = %v, want ErrModel", err)
	}

	seed := twoBuses(`{"NATS_URL": "nats://bus-a:4222,nats://bus-b:4222"}`)

	cls, err := compose.Classify(modelFrom(t, seed), nil, nil)
	if err != nil {
		t.Fatalf("a two-host seed list = %v, want one identity", err)
	}

	if !slices.Contains(cls.BusNames, "bus-a") || !slices.Contains(cls.BusNames, "bus-b") {
		t.Errorf("BusNames = %v, want both seed hosts", cls.BusNames)
	}

	external := `{"name": "shop", "services": {"api": {"environment": {"NATS_URL": "nats://queue.test:4222"},
		"networks": {"default": null}}}}`

	cls, err = compose.Classify(modelFrom(t, external), nil, nil)
	if err != nil || !slices.Equal(cls.BusNames, []string{"queue.test"}) {
		t.Errorf("a bus outside the model = %v, %v; want it the one bus name", cls.BusNames, err)
	}
}

func TestIPLiteralsAreDisclosedAsKeyAndHost(t *testing.T) {
	t.Parallel()

	model := `{"name": "shop", "services": {
		"api": {
			"environment": {"NATS_URL": "nats://bus:4222", "UPSTREAM": "10.1.2.3:9000", "LISTEN": "0.0.0.0:8080",
				"MOCK": "http://mock:8080"},
			"command": ["serve", "--peer", "127.0.0.1:6000"],
			"depends_on": {"migrate": {"condition": "service_completed_successfully"}},
			"networks": {"default": null}
		},
		"migrate": {"environment": {"SCHEMA_URL": "https://schema.example.test/x", "EVENTS": "nats://bus:4222"},
			"networks": {"default": null}},
		"mock": {"networks": {"default": null}},
		"bus": {"expose": ["4222"], "networks": {"default": null}}
	}}`

	cls := classify(t, modelFrom(t, model), nil, nil)

	want := []compose.Host{
		{Service: target, Key: "LISTEN", Host: "0.0.0.0"},
		{Service: target, Key: "UPSTREAM", Host: "10.1.2.3"},
		{Service: target, Key: "command[2]", Host: "127.0.0.1"},
	}
	if !slices.Equal(cls.Disclosed, want) {
		t.Errorf("Disclosed = %+v, want %+v", cls.Disclosed, want)
	}

	egress := []compose.Host{{Service: jobService, Key: "SCHEMA_URL", Host: "schema.example.test"}}
	if !slices.Equal(cls.SetupEgress, egress) {
		t.Errorf("SetupEgress = %+v, want %+v", cls.SetupEgress, egress)
	}

	if !slices.Contains(cls.SetupBusNames, busService) {
		t.Errorf("SetupBusNames = %v, want the bus as the job sees it", cls.SetupBusNames)
	}
}

func TestDanglingNamesReachTheStub(t *testing.T) {
	t.Parallel()

	model := `{"name": "shop", "services": {
		"api": {
			"environment": {"NATS_URL": "nats://bus:4222", "AUTH": "http://auth:8080/token",
				"CACHE": "cache.example.test:6379", "MOCK": "http://mock:80"},
			"external_links": ["legacy_db_1:legacy"],
			"networks": {"default": null}
		},
		"mock": {"networks": {"default": null}},
		"bus": {"expose": ["4222"], "networks": {"default": null}}
	}}`

	cls := classify(t, modelFrom(t, model), nil, nil)
	if want := []string{"auth", "legacy"}; !slices.Equal(cls.Dangling, want) {
		t.Errorf("Dangling = %v, want %v", cls.Dangling, want)
	}
}

func TestSharedMountsArePaired(t *testing.T) {
	t.Parallel()

	data := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")

	if err := os.Symlink(data, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	model := `{"name": "shop", "services": {
		"api": {
			"environment": {"NATS_URL": "nats://bus:4222", "MOCK": "http://mock:80"},
			"depends_on": {"migrate": {"condition": "service_completed_successfully"}},
			"volumes": [
				{"type": "bind", "source": ` + strconv.Quote(data) + `, "target": "/data", "read_only": true},
				{"type": "volume", "source": "shared", "target": "/shared"}
			],
			"networks": {"default": null}
		},
		"migrate": {"volumes": [{"type": "volume", "source": "shared", "target": "/out"}],
			"networks": {"default": null}},
		"mock": {"volumes": [{"type": "bind", "source": ` + strconv.Quote(link) + `, "target": "/mirror"}],
			"networks": {"default": null}},
		"bus": {"expose": ["4222"], "networks": {"default": null}}
	}, "volumes": {"shared": {"name": "shop_shared"}}}`

	resolved, err := filepath.EvalSymlinks(data)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	cls := classify(t, modelFrom(t, model), nil, nil)
	want := []compose.Shared{
		{Service: jobService, Path: "shared", Volume: true},
		{Service: mockService, Path: resolved},
	}

	t.Logf("shared pairs=%d", len(cls.Shared))

	if !slices.Equal(cls.Shared, want) {
		t.Errorf("Shared = %+v, want %+v", cls.Shared, want)
	}
}
