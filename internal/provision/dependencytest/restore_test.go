package dependencytest_test

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

// readyToRestore builds a fixture under waits and takes it through its seed, jobs and snapshot.
func readyToRestore(t *testing.T, composeFile string, waits provision.Waits) (*fixture, *provision.Dependencies) {
	t.Helper()

	f, deps, err := snapshotted(t, composeFile, waits)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	return f, deps
}

// restored restores every dependency for one start and returns where each endpoint is, and how long
// the restore took on the monotonic clock.
func restored(t *testing.T, deps *provision.Dependencies) (map[string]netip.AddrPort, time.Duration) {
	t.Helper()

	clock := startStopwatch()

	addrs, err := deps.Restore(t.Context())
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	return addrs, clock.elapsed()
}

// connect opens a direct connection to a restored Postgres: fixture reads, never recorded.
func connect(t *testing.T, addr netip.AddrPort) *pgx.Conn {
	t.Helper()

	conn, err := pgx.Connect(t.Context(), "postgres://postgres@"+addr.String()+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to %s: %v", addr, err)
	}

	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	return conn
}

// observed is what one restore of db answers to the same statement sequence.
type observed struct {
	xact, xmin, ctid, next, path string
}

// observe runs the same statement sequence on a fresh restore of db.
func observe(t *testing.T, addr netip.AddrPort) observed {
	t.Helper()

	conn := connect(t, addr)

	var got observed

	for _, step := range []struct {
		query string
		into  []any
	}{
		{query: "SELECT pg_current_xact_id()::text", into: []any{&got.xact}},
		{
			query: "INSERT INTO app.ledger (id, note) VALUES (nextval('app.ledger_ids'), 'restored') " +
				"RETURNING xmin::text, ctid::text, id::text",
			into: []any{&got.xmin, &got.ctid, &got.next},
		},
		{query: "SHOW search_path", into: []any{&got.path}},
	} {
		if err := conn.QueryRow(t.Context(), step.query).Scan(step.into...); err != nil {
			t.Fatalf("%s: %v", step.query, err)
		}
	}

	return got
}

// TestTwoRestoresOfOneSnapshotAreIdentical runs the same statements on two restores of one snapshot and
// reads the same transaction IDs, row versions, sequence values and search path from both: a restore
// is the snapshot's bytes, not a cleaned-up database.
func TestTwoRestoresOfOneSnapshotAreIdentical(t *testing.T) {
	t.Parallel()

	_, deps := readyToRestore(t, fixturePath(t, "datastore.yaml"), waitsForTests())

	seen := make([]observed, 0, 2)

	for start := range 2 {
		addrs, took := restored(t, deps)
		got := observe(t, addrs["db:5432"])

		t.Logf("restore %d took %v: %+v", start+1, took, got)

		seen = append(seen, got)
	}

	if seen[0] != seen[1] {
		t.Errorf("two restores of one snapshot differ:\n%+v\n%+v", seen[0], seen[1])
	}

	if seen[0].path != "app, public" {
		t.Errorf("search_path = %q, want the seeded %q", seen[0].path, "app, public")
	}
}

// TestDependencyVolumeStateSurvivesTheSnapshot restores a dependency whose state lives under its image's
// VOLUME: the job's row is there every start, and a row one start writes is gone at the next.
func TestDependencyVolumeStateSurvivesTheSnapshot(t *testing.T) {
	t.Parallel()

	_, deps := readyToRestore(t, fixturePath(t, "datastore.yaml"), waitsForTests())

	const starts = 3

	for start := range starts {
		addrs, _ := restored(t, deps)
		cache := addrs["cache:5432"]

		if n := count(t, cache, "SELECT count(*) FROM seeded"); n != 1 {
			t.Errorf("start %d: cache holds %d rows, want only the job's 1", start+1, n)
		}

		conn := connect(t, cache)
		if _, err := conn.Exec(t.Context(), "INSERT INTO seeded VALUES ($1)", 100+start); err != nil {
			t.Fatalf("start %d: write a row: %v", start+1, err)
		}
	}

	t.Logf("starts inspected=%d", starts)
}

// TestRestoreIdentityIsTheSnapshot refuses a restore that runs another cluster than its snapshot's.
func TestRestoreIdentityIsTheSnapshot(t *testing.T) {
	t.Parallel()

	_, deps := readyToRestore(t, fixturePath(t, "identity.yaml"), waitsForTests())

	_, err := deps.Restore(t.Context())
	if !errors.Is(err, provision.ErrRestore) || !strings.Contains(err.Error(), "db3") ||
		!strings.Contains(err.Error(), "identity mismatch") {
		t.Errorf("Restore() = %v, want ErrRestore naming db3 and the identity mismatch", err)
	}
}

// TestEveryStartRestoresEveryDependency reads each start's restores back from the engine: one running
// container per dependency, from its snapshot image, under the same names every start, on ports
// Stutter chose, with a per-run volume at every template target.
func TestEveryStartRestoresEveryDependency(t *testing.T) {
	t.Parallel()

	f, deps := readyToRestore(t, fixturePath(t, "datastore.yaml"), waitsForTests())
	aliases := map[string]string{}
	checked := 0

	for start := range 2 {
		restored(t, deps)

		for service, targets := range map[string][]string{
			"db": {"/var/run/postgresql"}, "cache": {"/var/lib/postgresql"},
		} {
			names := checkRestore(t, f, service, targets)

			if previous, seen := aliases[service]; seen && previous != names {
				t.Errorf("start %d: %s answers to %s, the start before to %s", start+1, service, names, previous)
			}

			aliases[service] = names
			checked++
		}

		if seeds := f.containersOfKind(t, rules.KindSeed); len(seeds) != 0 {
			t.Errorf("start %d: the engine holds %d seed containers", start+1, len(seeds))
		}
	}

	t.Logf("starts x services checked=%d", checked)

	if checked == 0 {
		t.Fatal("checked=0")
	}
}

// checkRestore reads one service's restore back from the engine and returns its aliases.
func checkRestore(t *testing.T, f *fixture, service string, targets []string) string {
	t.Helper()

	restore := f.serviceContainer(t, rules.KindRestore, service)

	if field(restore, "State", "Running") != any(true) {
		t.Errorf("%s's restore is not running", service)
	}

	image := inspected(t, f.docker.Inspect(t, dockertest.ObjectImage, text(field(restore, "Image"))))
	if field(image, "Config", "Labels", rules.LabelKind) != string(rules.KindSnapshot) {
		t.Errorf("%s's restore runs image %v, not a snapshot", service, field(restore, "Image"))
	}

	restoreVolumes := f.volumesOfKind(t, rules.KindRestore)
	mounts, _ := mountsOf(restore)

	for _, target := range targets {
		if name := text(field(mounts[target], "Name")); !restoreVolumes[name] {
			t.Errorf("%s's %s is mounted from %q, not a per-run volume", service, target, name)
		}
	}

	for _, port := range []uint16{5432} {
		if addr := published(t, restore, port); !addr.Addr().IsLoopback() || addr.Port() == 0 {
			t.Errorf("%s's %d is published at %v", service, port, addr)
		}
	}

	var names []string

	for _, network := range networksOf(restore) {
		for _, alias := range values(field(network, "Aliases")) {
			names = append(names, text(alias))
		}
	}

	slices.Sort(names)

	return strings.Join(names, ",")
}

// networksOf are a container inspect's networks.
func networksOf(container map[string]any) []any {
	networks, ok := field(container, "NetworkSettings", "Networks").(map[string]any)
	if !ok {
		return nil
	}

	out := make([]any, 0, len(networks))
	for _, network := range networks {
		out = append(out, network)
	}

	return out
}

// TestEmptiedVolumeStaysEmpty restores a volume the job emptied onto a path where the image holds
// content: every restore starts with it empty, since the template is its only source.
func TestEmptiedVolumeStaysEmpty(t *testing.T) {
	t.Parallel()

	waits := waitsForTests()
	waits.DependencyReady = 5 * time.Second

	_, deps := readyToRestore(t, fixturePath(t, "emptied.yaml"), waits)

	for start := range 2 {
		addrs, _ := restored(t, deps)

		served := servedTree(t, addrs["box:"+strconv.Itoa(treePort)])
		delete(served, ".")

		if len(served) != 0 {
			t.Errorf("start %d: box's volume holds %d entries, want none", start+1, len(served))
		}
	}
}

// TestARestoreWaitsForItsAwaitSet restores a dependency that listens two seconds after it starts: every
// restore waits for it, and never for the port it did not serve at seed.
func TestARestoreWaitsForItsAwaitSet(t *testing.T) {
	t.Parallel()

	waits := waitsForTests()
	waits.DependencyReady = 20 * time.Second

	_, deps := readyToRestore(t, fixturePath(t, "late.yaml"), waits)

	if slow := record(t, deps, "slow"); !slices.Equal(slow.NotListening, []uint16{7000}) {
		t.Errorf("slow's record = %+v, want 7000 not listening at seed", slow)
	}

	for start := range 2 {
		addrs, took := restored(t, deps)
		t.Logf("restore %d waited %v", start+1, took)

		if took < 2*time.Second || took >= waits.DependencyReady {
			t.Errorf("restore %d took %v, want at least the 2s slow takes to listen and less than its wait %v",
				start+1, took, waits.DependencyReady)
		}

		// Ready means answering now: a restore handed over before it listens is a dial failure.
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		answer, err := pg.Handshake(ctx, addrs["slow:5432"].String())

		cancel()

		if err != nil || answer != pg.AnswerPostgres {
			t.Errorf("restore %d: slow:5432 answered %q, %v the moment Restore returned, want Postgres", start+1,
				answer, err)
		}
	}
}

// TestTheHelperRefusesAFIFO restores a volume holding a named pipe: the copy refuses it by name.
func TestTheHelperRefusesAFIFO(t *testing.T) {
	t.Parallel()

	_, deps := readyToRestore(t, fixturePath(t, "fifo.yaml"), waitsForTests())

	_, err := deps.Restore(t.Context())
	if !errors.Is(err, provision.ErrRestore) || !strings.Contains(err.Error(), "pipe") {
		t.Errorf("Restore() = %v, want ErrRestore naming the pipe", err)
	}
}
