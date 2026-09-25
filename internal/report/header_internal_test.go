package report

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
	"github.com/Wintersta7e/stutter/internal/relay"
)

// secret is a value no header line may carry: it stands in for every environment and model value.
const secret = "SENTINEL-VALUE-8f3a"

const (
	baseFile     = "compose.yaml"
	overrideFile = "stutter.override.yaml"
	debugProfile = "debug"
	toolsService = "tools"
	cacheService = "cache"
)

// parsedModel is a compose project parsed the way the check parses one: two files in order, a
// profile-gated service present only when its profile is active, and a secret in the environment.
func parsedModel(t *testing.T, profiles ...string) *compose.Model {
	t.Helper()

	dir := t.TempDir()
	files := make([]string, 0, 2)

	for _, name := range []string{baseFile, overrideFile} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("services: {}\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}

		files = append(files, path)
	}

	run := func(_ context.Context, _ string, args []string, read compose.ConfigRead) ([]byte, int, error) {
		if read.Environment {
			return []byte("SECRET=" + secret + "\n"), 0, nil
		}

		services := `"orders":{"image":"orders:1","environment":{"SECRET":"` + secret + `"}}`
		if slices.Contains(args, debugProfile) {
			services += `,"` + toolsService + `":{"image":"tools:1","profiles":["` + debugProfile + `"]}`
		}

		return []byte(`{"name":"shop","services":{` + services + `}}`), 2, nil
	}

	model, err := compose.Parse(
		t.Context(),
		run,
		compose.Inputs{Service: testConsumer, Files: files, Profiles: profiles},
	)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	return model
}

// filledHeader has every row's producer filled, as a check that reached its end leaves it.
func filledHeader(t *testing.T) *Header {
	t.Helper()

	model := parsedModel(t, debugProfile)
	seeds := []provision.SeedRecord{{Service: "db", Path: provision.PathPostgres, Jobs: []string{"migrate"}}}
	hosts := []httpproxy.HostTally{{Host: "payments.example.test", Calls: 2, Routed: 1, TLS: true}}
	loaded := corpus.Loaded{Messages: make([]corpus.Message, 3), Files: []string{"1.a.json", "2.a.json", "3.a.json"}}

	return &Header{
		Model:   model,
		Timings: &harness.Timings{Startup: time.Minute, Quiesce: 200 * time.Millisecond, Drain: 9 * time.Second},
		Engine: &provision.Identity{
			CLIPath: "/usr/bin/docker", CLIVersion: "29.6.2", ServerVersion: "29.6.2", APIVersion: "1.55",
			Platform: "Docker Engine - Community", OS: "linux", Arch: "amd64", Endpoint: "unix:///var/run/docker.sock",
			Context: "default", EngineID: "engine-1", Compose: "5.3.1",
		},
		Image: &compose.Image{ID: "sha256:abc", OS: "linux", Arch: "amd64"},
		Classification: &compose.Classification{
			Deps: []compose.Dependency{
				{
					Service:   "db",
					Role:      compose.RoleDatastore,
					Endpoints: []compose.Endpoint{{Port: 5432, Protocol: "pg"}},
				},
				{
					Service:   cacheService,
					Role:      compose.RoleOther,
					Endpoints: []compose.Endpoint{{Port: 6379, Protocol: "opaque"}},
				},
				{Service: "audit", Role: compose.RoleSibling},
			},
			Disclosed:   []compose.Host{{Service: testConsumer, Key: "METRICS_URL", Host: "127.0.0.1"}},
			SetupEgress: []compose.Host{{Service: "migrate", Key: "SCHEMA_URL", Host: "schemas.example.test"}},
			Shared:      []compose.Shared{{Service: "db", Path: "/srv/shared"}},
			Declarations: []compose.Declaration{
				{File: model.Files()[1], Service: cacheService, Key: "roles", Value: "other"},
			},
		},
		Seeds: &seeds,
		Spec: &compose.Spec{
			Replaced:     []compose.Replaced{{Key: "healthcheck", What: "disabled; Stutter decides readiness"}},
			ProxyRemoved: []compose.Named{{Name: "HTTPS_PROXY", Source: "compose"}},
			Mounts:       []compose.Mount{{Kind: compose.MountBind, Source: "/srv/config", Target: "/config"}},
		},
		Corpus:  &loaded,
		Hosts:   &hosts,
		Counts:  &harness.ListenerCounts{Foreign: 1, Unattached: 2},
		Queries: []relay.Query{{Name: "telemetry.example.test", Type: 28}},
		Target:  compose.ImageRef{Service: "orders", Ref: "orders:1"},
		Given: Given{
			Stream: "ORDERS", Corpus: "corpus", Routes: "routes.json", Compose: []string{baseFile, overrideFile},
			Profiles: []string{debugProfile}, Consumers: []string{"reserve"}, Timings: []string{"--quiesce"},
		},
		Mode: harness.ModeGateway,
	}
}

// rowLabels are the thirteen rows in their order.
var rowLabels = []string{
	rowInputs, rowDeclarations, rowEngine, rowImage, rowHostMode, rowDependencies, rowSeeds, rowSpec,
	rowCorpus, rowHosts, rowDisclosure, rowReach, rowSetupEgress,
}

// rowOf returns the lines of one row: its label line and the indented lines under it.
func rowOf(lines []string, label string) []string {
	var row []string

	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, label):
			row = append(row, line)
		case len(row) > 0 && strings.HasPrefix(line, detailIndent):
			row = append(row, line)
		case len(row) > 0:
			return row
		default:
		}
	}

	return row
}

// TestEveryHeaderRowRenders: a header that reached every step renders each of its thirteen rows.
func TestEveryHeaderRowRenders(t *testing.T) {
	t.Parallel()

	lines := filledHeader(t).lines()
	rows := 0

	for _, label := range rowLabels {
		if len(rowOf(lines, label)) == 0 {
			t.Errorf("row %q is missing from the header:\n%s", label, strings.Join(lines, "\n"))

			continue
		}

		rows++
	}

	t.Logf("rows=%d", rows)

	if rows == 0 {
		t.Fatal("no header row rendered")
	}
}

// TestASeedRowForEveryStartedService: one line per started dependency, Postgres or not; none found
// when nothing was started.
func TestASeedRowForEveryStartedService(t *testing.T) {
	t.Parallel()

	records := []provision.SeedRecord{
		{Service: "db", Path: provision.PathPostgres, InitScripts: true},
		{Service: cacheService, Path: provision.PathOther, Killed: true, NotListening: []uint16{9000}},
	}

	row := rowOf((&Header{Seeds: &records}).lines(), rowSeeds)
	if len(row) != 1+len(records) {
		t.Fatalf("seed row = %q, want a head line and one line per started service", row)
	}

	for index, record := range records {
		if !strings.Contains(row[index+1], record.Service+" ("+record.Path+")") {
			t.Errorf("seed line %q does not name %s and its path", row[index+1], record.Service)
		}
	}

	if !strings.Contains(row[2], "killed") || !strings.Contains(row[2], "9000") {
		t.Errorf("the other-path seed line %q omits its SIGKILL or its silent port", row[2])
	}

	var none []provision.SeedRecord
	if got := rowOf(
		(&Header{Seeds: &none}).lines(),
		rowSeeds,
	); len(got) != 1 ||
		!strings.Contains(got[0], "none found") {
		t.Errorf("no started dependency renders %q, want none found", got)
	}
}

// TestHostAliasModeDisclosesTheHalfCloseLimit: the limit is the host-alias path's, and only its.
func TestHostAliasModeDisclosesTheHalfCloseLimit(t *testing.T) {
	t.Parallel()

	for mode, want := range map[harness.Mode]bool{harness.ModeHostAlias: true, harness.ModeGateway: false} {
		rendered := strings.Join((&Header{Mode: mode}).lines(), "\n")
		if got := strings.Contains(rendered, "half-close"); got != want {
			t.Errorf("%s mode: the half-close limit rendered = %t, want %t:\n%s", mode, got, want, rendered)
		}
	}
}

// TestTheHeaderRendersNamesNeverValues: stdout is diffable and shareable, so no environment, model or
// spec value ever reaches it — names, keys, paths and counts only.
func TestTheHeaderRendersNamesNeverValues(t *testing.T) {
	t.Parallel()

	header := filledHeader(t)
	header.Spec.Env = map[string]string{"DATABASE_URL": secret, "API_TOKEN": secret}
	header.Spec.Sysctls = map[string]string{"net.core.somaxconn": secret}
	header.Spec.Entrypoint = []string{secret}
	header.Spec.Cmd = []string{"--token", secret}
	header.Spec.User, header.Spec.WorkingDir, header.Spec.Hostname = secret, secret, secret
	header.Spec.Domainname, header.Spec.MacAddress = secret, secret
	header.Spec.SecurityOpt, header.Spec.GroupAdd = []string{secret}, []string{secret}

	// The model's environment value and the spec's eleven value fields carry the sentinel.
	const valuesScanned = 12

	rendered := strings.Join(header.lines(), "\n")

	if count := strings.Count(rendered, secret); count != 0 {
		t.Errorf("the header carries a value %d times:\n%s", count, rendered)
	}

	for _, name := range []string{"orders", "db", cacheService, "HTTPS_PROXY", "/config", "ORDERS"} {
		if !strings.Contains(rendered, name) {
			t.Errorf("the header omits the name %q", name)
		}
	}

	t.Logf("values-scanned=%d bytes=%d", valuesScanned, len(rendered))

	if len(rendered) == 0 {
		t.Fatal("the header rendered nothing")
	}
}

// TestHeaderRendersWhatTheFlagsChanged: each optional input shows up where it changed the check.
func TestHeaderRendersWhatTheFlagsChanged(t *testing.T) {
	t.Parallel()

	header := filledHeader(t)
	lines := header.lines()

	hosts := strings.Join(rowOf(lines, rowHosts), "\n")
	if !strings.Contains(hosts, "routed 1") {
		t.Errorf("a host a route answered does not render as routed:\n%s", hosts)
	}

	unrouted := []httpproxy.HostTally{{Host: "payments.example.test", Calls: 2}}
	header.Hosts = &unrouted

	if got := strings.Join(rowOf(header.lines(), rowHosts), "\n"); !strings.Contains(got, "default reply") {
		t.Errorf("a host no route answered does not render as default:\n%s", got)
	}

	inputs := strings.Join(rowOf(lines, rowInputs), "\n")
	if !strings.Contains(inputs, "profile "+debugProfile+": "+toolsService) {
		t.Errorf("--profile %s does not render with the service it brought in:\n%s", debugProfile, inputs)
	}

	plain := &Header{Model: parsedModel(t), Given: Given{Compose: []string{baseFile, overrideFile}}}
	if got := strings.Join(rowOf(plain.lines(), rowInputs), "\n"); strings.Contains(got, toolsService) {
		t.Errorf("without --profile the gated service renders anyway:\n%s", got)
	}

	if first, second := strings.Index(inputs, baseFile), strings.Index(inputs, overrideFile); first < 0 ||
		second < first {
		t.Errorf("the --compose files do not render in the order given:\n%s", inputs)
	}

	declared := strings.Join(rowOf(lines, rowDeclarations), "\n")
	if !strings.Contains(declared, "x-stutter cache roles → other ("+overrideFile+")") {
		t.Errorf("the override does not render with the file declaring it:\n%s", declared)
	}

	if !strings.Contains(declared, "--quiesce 200ms") || strings.Contains(declared, "--startup") {
		t.Errorf("the timings do not render as the harness used them, for the flags given only:\n%s", declared)
	}
}
