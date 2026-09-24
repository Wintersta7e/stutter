package dependencytest_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// TestDeadDependencyIsNeverCompared kills a restored dependency mid-run: the liveness check names it,
// so the run is never compared.
func TestDeadDependencyIsNeverCompared(t *testing.T) {
	t.Parallel()

	f, deps := readyToRestore(t, fixturePath(t, "datastore.yaml"), waitsForTests())
	restored(t, deps)

	if err := deps.Alive(t.Context()); err != nil {
		t.Fatalf("Alive() with every restore running = %v, want nil", err)
	}

	f.docker.Kill(t, text(f.serviceContainer(t, rules.KindRestore, "cache")["Id"]))

	err := deps.Alive(t.Context())
	if !errors.Is(err, provision.ErrDependencyDead) || !strings.Contains(err.Error(), "cache") {
		t.Errorf("Alive() after cache died = %v, want ErrDependencyDead naming cache", err)
	}
}

// TestEveryStartedServiceHasOneSeedRecord gives the report one account of every dependency the check
// started, and of nothing else.
func TestEveryStartedServiceHasOneSeedRecord(t *testing.T) {
	t.Parallel()

	f, deps := seeded(t, fixturePath(t, "datastore.yaml"))

	var started []string

	for _, dep := range f.cls.Deps {
		if dep.Role == compose.RoleDatastore || dep.Role == compose.RoleOther {
			started = append(started, dep.Service)
		}
	}

	records := deps.Records()
	t.Logf("started=%d records=%d", len(started), len(records))

	if len(started) == 0 || len(records) != len(started) {
		t.Fatalf("records=%d for started=%d, want one each and at least one", len(records), len(started))
	}

	for _, got := range records {
		if !slices.Contains(started, got.Service) || got.Path == "" {
			t.Errorf("record %+v names no started service or no path", got)
		}
	}
}

// owners reads which job's rows a restored cluster holds.
func owners(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()

	rows, err := conn.Query(t.Context(), "SELECT who FROM owned ORDER BY who")
	if err != nil {
		t.Fatalf("read owned: %v", err)
	}

	who, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read owned: %v", err)
	}

	return who
}

// TestTwoDatastoresSeedAndRestoreIndependently seeds, snapshots and restores two Postgres datastores:
// each cluster holds its own job's rows and only those.
func TestTwoDatastoresSeedAndRestoreIndependently(t *testing.T) {
	t.Parallel()

	f := newFixture(t, fixturePath(t, "two-datastores.yaml"), fixtureOptions{helper: true})
	deps := f.dependencies(t, waitsForTests())

	if err := deps.Seed(t.Context()); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}

	seeds := len(f.containersOfKind(t, rules.KindSeed))

	if err := deps.Jobs(t.Context()); err != nil {
		t.Fatalf("Jobs() error = %v", err)
	}

	if err := deps.Snapshot(t.Context()); err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	snapshots := 0

	for _, id := range f.docker.Listing(t, rules.LabelCheck+"="+f.eng.CheckID()).Images {
		image := inspected(t, f.docker.Inspect(t, dockertest.ObjectImage, id))
		if field(image, "Config", "Labels", rules.LabelKind) == string(rules.KindSnapshot) {
			snapshots++
		}
	}

	for start := range 2 {
		addrs, _ := restored(t, deps)

		restores := len(f.containersOfKind(t, rules.KindRestore))
		t.Logf("seeds=%d snapshots=%d restores in start %d=%d", seeds, snapshots, start+1, restores)

		if seeds != 2 || snapshots != 2 || restores != 2 {
			t.Errorf("seeds=%d, snapshots=%d, restores=%d; want 2 of each", seeds, snapshots, restores)
		}

		for service, want := range map[string]string{"db-a": "a", "db-b": "b"} {
			if got := owners(t, connect(t, addrs[service+":5432"])); !slices.Equal(got, []string{want}) {
				t.Errorf("start %d: %s holds rows of %v, want only %q", start+1, service, got, want)
			}
		}
	}
}
