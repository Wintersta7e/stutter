package dependencytest_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// snapshotted builds a fixture under waits, seeds it, runs its jobs and takes its snapshot.
func snapshotted(t *testing.T, composeFile string, waits provision.Waits) (*fixture, *provision.Dependencies, error) {
	t.Helper()

	f := newFixture(t, composeFile, fixtureOptions{helper: true})
	deps := f.dependencies(t, waits)

	if err := deps.Seed(t.Context()); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}

	if err := deps.Jobs(t.Context()); err != nil {
		t.Fatalf("Jobs() error = %v", err)
	}

	return f, deps, deps.Snapshot(t.Context())
}

// TestSnapshotGuardRefusesDataOutsideTheLayer refuses to commit a Postgres whose cluster lives under
// its image's VOLUME, where a commit keeps nothing: the restore would come up healthy and empty.
func TestSnapshotGuardRefusesDataOutsideTheLayer(t *testing.T) {
	t.Parallel()

	_, deps, err := snapshotted(t, fixturePath(t, "guard-volume.yaml"), waitsForTests())
	if err == nil {
		// What the guard exists to stop: the restore comes up healthy, without the job's rows.
		addrs, _ := restored(t, deps)
		t.Fatalf("snapshot guard did not refuse; the first restore holds %d of the job's rows",
			count(t, addrs["db2:5432"], "SELECT count(*) FROM pg_tables WHERE tablename = 'filled'"))
	}

	if !errors.Is(err, provision.ErrSnapshot) || !strings.Contains(err.Error(), "db2") ||
		!strings.Contains(err.Error(), "PG_VERSION") {
		t.Errorf("Snapshot() = %v, want ErrSnapshot naming db2 and PG_VERSION", err)
	}
}

// TestDependencyGuardRefusesAnUncoveredVolume snapshots a Postgres datastore and an other dependency
// whose state lives under its image's VOLUME: both prove clean, and the datastore stopped gracefully.
func TestDependencyGuardRefusesAnUncoveredVolume(t *testing.T) {
	t.Parallel()

	f, deps, err := snapshotted(t, fixturePath(t, "datastore.yaml"), waitsForTests())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	images := f.docker.Listing(t, rules.LabelKind+"="+string(rules.KindSnapshot)).Images
	t.Logf("snapshot images of every check=%d; seeds left=%d", len(images), len(f.containersOfKind(t, rules.KindSeed)))

	if left := f.containersOfKind(t, rules.KindSeed); len(left) != 0 {
		t.Errorf("the engine still holds %d seed containers after the snapshot", len(left))
	}

	if db := record(t, deps, "db"); db.Killed {
		t.Errorf("db's record says it was killed at its stop: %+v", db)
	}

	addrs, _ := restored(t, deps)
	if n := count(t, addrs["cache:5432"], "SELECT count(*) FROM seeded"); n != 1 {
		t.Errorf("cache's first restore holds %d of the job's rows, want 1: its VOLUME was not snapshotted", n)
	}
}

// TestTheAwaitSetIsTimedFromTheLastJob observes a dependency that only listens after a job longer than
// the dependency wait: counted from the job's exit, its port is in the await set.
func TestTheAwaitSetIsTimedFromTheLastJob(t *testing.T) {
	t.Parallel()

	waits := waitsForTests()
	waits.DependencyReady = 4 * time.Second

	_, deps, err := snapshotted(t, fixturePath(t, "await-after-jobs.yaml"), waits)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if later := record(t, deps, "later"); len(later.NotListening) != 0 {
		t.Errorf("later's record says %v were not listening at seed, want none: its port opened after the job",
			later.NotListening)
	}
}

// TestASeedStoppedBySIGKILLIsRecorded stops a seed that ignores its stop signal: the engine kills it
// when the grace period ends, the record says so, and the snapshot still succeeds.
func TestASeedStoppedBySIGKILLIsRecorded(t *testing.T) {
	t.Parallel()

	waits := waitsForTests()
	waits.DependencyReady = 5 * time.Second

	_, deps, err := snapshotted(t, fixturePath(t, "emptied.yaml"), waits)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	box := record(t, deps, "box")
	if !box.Killed || !slices.Equal(box.NotListening, []uint16{5432}) {
		t.Errorf("box's record = %+v, want Killed and 5432 not listening at seed", box)
	}
}
