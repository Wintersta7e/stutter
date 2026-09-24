package dependencytest_test

import (
	"context"
	"errors"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// count runs a count query against the Postgres at addr, directly: fixture reads, never recorded.
func count(t *testing.T, addr netip.AddrPort, query string) int {
	t.Helper()

	conn, err := pgx.Connect(t.Context(), "postgres://postgres@"+addr.String()+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to %s: %v", addr, err)
	}

	defer func() { _ = conn.Close(context.Background()) }()

	var n int
	if err := conn.QueryRow(t.Context(), query).Scan(&n); err != nil {
		t.Fatalf("%s at %s: %v", query, addr, err)
	}

	return n
}

// jobsRun builds a fixture, seeds it, and runs its jobs.
func jobsRun(t *testing.T, composeFile string) (*fixture, *provision.Dependencies, error) {
	t.Helper()

	f, deps := seeded(t, composeFile)

	return f, deps, deps.Jobs(t.Context())
}

// TestOnlyACompletedJobIsSeed runs a migrator the target waits to complete, and only that: its rows are
// in both seeds and its container is gone. A migrator the target merely starts is no seed at all.
func TestOnlyACompletedJobIsSeed(t *testing.T) {
	t.Parallel()

	t.Run("completed", func(t *testing.T) {
		t.Parallel()

		f, deps, err := jobsRun(t, fixturePath(t, "datastore.yaml"))
		t.Logf("discovered jobs=%d %v", len(deps.Discovered()), deps.Discovered())

		if err != nil {
			t.Fatalf("Jobs() error = %v", err)
		}

		if got := deps.Discovered(); !slices.Equal(got, []string{"migrate"}) {
			t.Errorf("Discovered() = %v, want [migrate]", got)
		}

		if left := f.containersOfKind(t, rules.KindJob); len(left) != 0 {
			t.Errorf("the engine still holds %d job containers: %v", len(left), left)
		}

		db := published(t, f.serviceContainer(t, rules.KindSeed, "db"), 5432)
		cache := published(t, f.serviceContainer(t, rules.KindSeed, "cache"), 5432)

		if n := count(t, db, "SELECT count(*) FROM app.ledger"); n != 1 {
			t.Errorf("db's seed holds %d of the job's rows, want 1", n)
		}

		if n := count(t, cache, "SELECT count(*) FROM seeded"); n != 1 {
			t.Errorf("cache's seed holds %d of the job's rows, want 1", n)
		}

		if jobs := record(t, deps, "db").Jobs; !slices.Equal(jobs, []string{"migrate"}) {
			t.Errorf("db's record names the jobs %v, want [migrate]", jobs)
		}
	})

	t.Run("started only", func(t *testing.T) {
		t.Parallel()

		_, deps, err := jobsRun(t, fixturePath(t, "migrator-started.yaml"))
		t.Logf("discovered jobs=%d %v", len(deps.Discovered()), deps.Discovered())

		if err != nil {
			t.Fatalf("Jobs() error = %v", err)
		}

		if got := deps.Discovered(); len(got) != 0 {
			t.Errorf("Discovered() = %v, want none: nothing waits for the migrator to complete", got)
		}

		if db := record(t, deps, "db"); len(db.Jobs) != 0 || db.InitScripts {
			t.Errorf("db's record = %+v, want no jobs and no init scripts: none found", db)
		}
	})
}

// failingCompose is a project whose one job exits 3.
func failingCompose() string {
	return `name: failing
services:
  app:
    image: example.test/app:1
    environment:
      NATS_URL: nats://bus:4222
    depends_on:
      broken:
        condition: service_completed_successfully
  bus:
    image: nats:2.14-alpine
  broken:
    image: postgres:18-alpine
    entrypoint: ["sh", "-c", "exit 3"]
`
}

// TestAFailingJobStopsTheSeed ends the seed phase on a job that exits non-zero, naming it and its code.
func TestAFailingJobStopsTheSeed(t *testing.T) {
	t.Parallel()

	_, _, err := jobsRun(t, writeCompose(t, failingCompose()))

	if !errors.Is(err, provision.ErrJob) || !strings.Contains(err.Error(), "broken") ||
		!strings.Contains(err.Error(), "exited 3") {
		t.Errorf("Jobs() = %v, want ErrJob naming broken and its exit code 3", err)
	}
}

// TestAJobWritesIntoTheDependencysTemplate shares a volume between a job and a started dependency: the
// job empties it, and the dependency's seed sees it empty.
func TestAJobWritesIntoTheDependencysTemplate(t *testing.T) {
	t.Parallel()

	f, _, err := jobsRun(t, fixturePath(t, "emptied.yaml"))
	if err != nil {
		t.Fatalf("Jobs() error = %v", err)
	}

	served := servedTree(t, published(t, f.serviceContainer(t, rules.KindSeed, "box"), treePort))
	delete(served, ".")

	if len(served) != 0 {
		t.Errorf("box's seed holds %d entries in the shared volume after the job emptied it: %v",
			len(served), slices.Sorted(maps.Keys(served)))
	}
}
