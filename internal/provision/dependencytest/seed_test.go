package dependencytest_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// seeded builds a fixture from composeFile, with the copy helper, and runs its seeds.
func seeded(t *testing.T, composeFile string) (*fixture, *provision.Dependencies) {
	t.Helper()

	f := newFixture(t, composeFile, fixtureOptions{helper: true})
	deps := f.dependencies(t, waitsForTests())

	if err := deps.Seed(t.Context()); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}

	return f, deps
}

// record is the seed record of one service.
func record(t *testing.T, deps *provision.Dependencies, service string) provision.SeedRecord {
	t.Helper()

	for _, got := range deps.Records() {
		if got.Service == service {
			return got
		}
	}

	t.Fatalf("no seed record for %s in %+v", service, deps.Records())

	return provision.SeedRecord{}
}

// mountsOf are a container inspect's mounts by destination, and its tmpfs targets.
func mountsOf(container map[string]any) (map[string]map[string]any, map[string]bool) {
	mounts, tmpfs := map[string]map[string]any{}, map[string]bool{}

	for _, m := range list(field(container, "Mounts")) {
		mounts[text(m["Destination"])] = m
	}

	for _, m := range list(field(container, "HostConfig", "Mounts")) {
		if m["Type"] == "tmpfs" {
			tmpfs[text(m["Target"])] = true
		}
	}

	return mounts, tmpfs
}

// published is where a container port is published, as the engine holds it.
func published(t *testing.T, container map[string]any, port uint16) netip.AddrPort {
	t.Helper()

	bindings := list(field(container, "NetworkSettings", "Ports", strconv.Itoa(int(port))+"/tcp"))
	if len(bindings) != 1 {
		t.Fatalf("port %d has %d bindings, want 1: %v", port, len(bindings), bindings)
	}

	ip, host := text(bindings[0]["HostIp"]), text(bindings[0]["HostPort"])

	addr, err := netip.ParseAddrPort(ip + ":" + host)
	if err != nil {
		t.Fatalf("port %d is published at %q:%q: %v", port, ip, host, err)
	}

	return addr
}

// TestSeedContainersFollowTheirPath reads each seed back from the engine: the Postgres seed keeps its
// cluster in the container layer and its image VOLUME on a tmpfs, the other seed keeps its VOLUME on
// a template volume, and nothing mounts a volume that is not a template or a writable bind.
func TestSeedContainersFollowTheirPath(t *testing.T) {
	t.Parallel()

	f, deps := seeded(t, fixturePath(t, "datastore.yaml"))
	templates := f.volumesOfKind(t, rules.KindTemplateVolume)
	seeds := f.containersOfKind(t, rules.KindSeed)

	t.Logf("containers inspected: seed=%d; template volumes=%d", len(seeds), len(templates))

	if len(seeds) != 2 {
		t.Fatalf("the engine holds %d seed containers, want 2 (db and cache)", len(seeds))
	}

	db, cache := f.serviceContainer(t, rules.KindSeed, "db"), f.serviceContainer(t, rules.KindSeed, "cache")
	dbMounts, dbTmpfs := mountsOf(db)
	cacheMounts, _ := mountsOf(cache)

	if env := values(field(db, "Config", "Env")); !slices.Contains(env, any("PGDATA=/stutter/pgdata")) {
		t.Errorf("db's environment %v holds no PGDATA=/stutter/pgdata", env)
	}

	if kind := field(dbMounts["/var/lib/postgresql"], "Type"); !dbTmpfs["/var/lib/postgresql"] || kind != "tmpfs" {
		t.Errorf("db's /var/lib/postgresql is a %v mount (tmpfs targets %v), want a tmpfs and no volume", kind, dbTmpfs)
	}

	for _, want := range []struct {
		mounts  map[string]map[string]any
		service string
		target  string
	}{
		{service: "db", target: "/var/run/postgresql", mounts: dbMounts},
		{service: "cache", target: "/var/lib/postgresql", mounts: cacheMounts},
	} {
		if name := text(field(want.mounts[want.target], "Name")); !templates[name] {
			t.Errorf("%s's %s is mounted from %q, not a template volume of the check", want.service, want.target, name)
		}
	}

	if test := values(field(db, "Config", "Healthcheck", "Test")); len(test) == 0 {
		t.Error("db's seed has no healthcheck")
	}

	if addr := published(t, db, 5432); !addr.Addr().IsLoopback() || addr.Port() == 0 {
		t.Errorf("db's 5432 is published at %v, want a loopback address and a chosen port", addr)
	}

	for _, container := range []map[string]any{db, cache} {
		requireNoStrayStorage(t, container, templates)
	}

	if mounts := record(t, deps, "db").Mounts; !slices.Contains(mounts, "/var/lib/postgresql dropped") {
		t.Errorf("db's record %q does not name /var/lib/postgresql dropped", mounts)
	}
}

// requireNoStrayStorage fails on an anonymous volume, a volume that is not a template of this check,
// or a writable bind.
func requireNoStrayStorage(t *testing.T, container map[string]any, templates map[string]bool) {
	t.Helper()

	for _, m := range list(field(container, "Mounts")) {
		name := text(m["Name"])
		writable := m["RW"] == any(true)

		switch {
		case m["Type"] == "volume" && !templates[name]:
			t.Errorf("%v mounts volume %q, which is not a template of the check", field(container, "Name"), name)
		case m["Type"] == "bind" && writable:
			t.Errorf("%v mounts %v writable", field(container, "Name"), m["Source"])
		default:
		}
	}
}

// buildTree lays out the tree a copy must reproduce: a 0600 file of another owner, a setgid directory,
// an absolute symlink, a hard-link pair, an empty directory, and every time in the past.
func buildTree(t *testing.T) string {
	t.Helper()

	root := filepath.Join(t.TempDir(), "tree")
	past := time.Date(2001, time.February, 3, 4, 5, 6, 0, time.UTC)

	for _, step := range []func() error{
		func() error { return os.MkdirAll(filepath.Join(root, "shared"), 0o750) },
		func() error { return os.Mkdir(filepath.Join(root, "empty"), 0o700) },
		func() error { return os.WriteFile(filepath.Join(root, "secret"), []byte("only mine\n"), 0o600) },
		func() error { return os.WriteFile(filepath.Join(root, "shared", "inner"), []byte("inner\n"), 0o600) },
		func() error { return os.Chmod(filepath.Join(root, "shared"), 0o750|fs.ModeSetgid) },
		func() error { return os.WriteFile(filepath.Join(root, "first"), []byte("two names\n"), 0o600) },
		func() error { return os.Link(filepath.Join(root, "first"), filepath.Join(root, "second")) },
		func() error { return os.Symlink("/etc/absolute-target", filepath.Join(root, "absolute")) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}

	if os.Geteuid() == 0 {
		if err := os.Lchown(filepath.Join(root, "secret"), 1234, 1234); err != nil {
			t.Fatal(err)
		}
	}

	for _, entry := range []string{"secret", "first", "shared/inner", "shared", "empty", "."} {
		if err := os.Chtimes(filepath.Join(root, entry), past, past); err != nil {
			t.Fatal(err)
		}
	}

	return root
}

// treeCompose is a project whose one other dependency binds dir writable at /work and serves /work.
func treeCompose(dir string) string {
	return fmt.Sprintf(`name: tree
x-stutter:
  roles:
    tree: other
  endpoints:
    tree:
      "5432": opaque
      "7000": opaque
services:
  app:
    image: example.test/app:1
    environment:
      NATS_URL: nats://bus:4222
    depends_on:
      tree:
        condition: service_started
  bus:
    image: nats:2.14-alpine
  tree:
    image: postgres:18-alpine
    init: true
    entrypoint: ["sh", "-c", %q, "/work"]
    expose: ["7000"]
    volumes:
      - type: bind
        source: %q
        target: /work
`, treeServer, dir)
}

// TestTheHelperCopiesATreeFaithfully copies a user directory through the production helper into the
// seed's template volume, and compares what the seed holds with the host tree entry by entry.
func TestTheHelperCopiesATreeFaithfully(t *testing.T) {
	t.Parallel()

	source := buildTree(t)
	want := hostTree(t, source)

	f, _ := seeded(t, writeCompose(t, treeCompose(source)))
	got := servedTree(t, published(t, f.serviceContainer(t, rules.KindSeed, "tree"), treePort))

	compared := compareTrees(t, want, got)
	t.Logf("entries compared=%d", compared)

	if compared == 0 {
		t.Fatal("entries compared=0: the tree is empty, so the comparison proves nothing")
	}
}

// boxCompose is a project whose other dependency binds dir writable at /work and file writable at
// /conf/app.conf, writes into /work at start, and serves /work.
func boxCompose(dir, file string) string {
	return fmt.Sprintf(`name: box
x-stutter:
  roles:
    box: other
  endpoints:
    box:
      "5432": opaque
      "7000": opaque
services:
  app:
    image: example.test/app:1
    environment:
      NATS_URL: nats://bus:4222
    depends_on:
      box:
        condition: service_started
  bus:
    image: nats:2.14-alpine
  box:
    image: postgres:18-alpine
    init: true
    entrypoint: ["sh", "-c", %q, "/work"]
    expose: ["7000"]
    volumes:
      - type: bind
        source: %q
        target: /work
      - type: bind
        source: %q
        target: /conf/app.conf
`, "echo written > /work/new.txt; "+treeServer, dir, file)
}

// TestAWritableBindIsCopiedNeverWritten gives a dependency its own copy of a writable directory bind:
// its writes reach the copy, the host tree is unchanged after the check, and a writable file bind is
// mounted read-only.
func TestAWritableBindIsCopiedNeverWritten(t *testing.T) {
	t.Parallel()

	t.Run("copied", func(t *testing.T) {
		t.Parallel()

		host := t.TempDir()
		dir, file := filepath.Join(host, "work"), filepath.Join(host, "app.conf")

		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}

		for _, write := range []string{filepath.Join(dir, "kept.txt"), file} {
			if err := os.WriteFile(write, []byte("host\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}

		before, err := compose.Fingerprint(t.Context(), []string{dir, file})
		if err != nil {
			t.Fatal(err)
		}

		f, deps := seeded(t, writeCompose(t, boxCompose(dir, file)))
		box := f.serviceContainer(t, rules.KindSeed, "box")

		if served := servedTree(t, published(t, box, treePort)); served["new.txt"].kind != kindFile {
			t.Errorf("the seed's /work holds %d entries and no new.txt: its write did not reach its copy", len(served))
		}

		if mounts, _ := mountsOf(box); mounts["/conf/app.conf"] == nil || mounts["/conf/app.conf"]["RW"] != any(false) {
			t.Errorf("the file bind reads back %v, want RW=false", mounts["/conf/app.conf"])
		}

		if mounts := record(t, deps, "box").Mounts; !slices.Contains(mounts, "/work copied") ||
			!slices.Contains(mounts, "/conf/app.conf read-only") {
			t.Errorf("box's record %q does not name /work copied and /conf/app.conf read-only", mounts)
		}

		f.eng.Close(context.WithoutCancel(t.Context()), provision.DiscardLogs)

		after, err := compose.Fingerprint(t.Context(), []string{dir, file})
		if err != nil {
			t.Fatal(err)
		}

		changed := before.Changed(after)
		t.Logf("entries fingerprinted=%d new=%d changed=%v", after.Entries(), after.Entries()-before.Entries(), changed)

		if before.Entries() == 0 || len(changed) != 0 || after.Entries() != before.Entries() {
			t.Errorf("the host tree changed: %v (%d entries before, %d after)", changed, before.Entries(),
				after.Entries())
		}
	})

	t.Run("missing source", func(t *testing.T) {
		t.Parallel()

		host := t.TempDir()
		missing := filepath.Join(host, "absent")

		f := newFixture(t, writeCompose(t, boxCompose(missing, filepath.Join(host, "absent.conf"))),
			fixtureOptions{helper: true})

		if err := f.dependencies(t, waitsForTests()).Seed(t.Context()); err == nil {
			t.Error("Seed() = <nil> for a bind source that does not exist")
		}

		if _, err := os.Lstat(missing); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("the missing source exists after the seed: %v", err)
		}
	})
}

// probeCompose is a project whose other dependency is healthy only when the file named by an argument
// holding spaces, both quotes and a dollar exists: an exec-form healthcheck the engine takes as a shell
// string must split back into exactly that argument.
func probeCompose() string {
	return `name: probe
x-stutter:
  roles:
    probe: other
  endpoints:
    probe:
      "5432": opaque
services:
  app:
    image: example.test/app:1
    environment:
      NATS_URL: nats://bus:4222
    depends_on:
      probe:
        condition: service_healthy
  bus:
    image: nats:2.14-alpine
  probe:
    image: postgres:18-alpine
    init: true
    entrypoint: ["sh", "-c", "touch \"$$0\"; exec sleep 3600", "/tmp/it's a \"b c\" $$HOME"]
    healthcheck:
      test: ["CMD", "test", "-f", "/tmp/it's a \"b c\" $$HOME"]
      interval: 1s
      retries: 30
`
}

// TestAnExecFormHealthcheckSurvivesTheShell seeds a dependency whose depends_on condition waits for an
// exec-form healthcheck: the engine holds it as one shell string, and the seed becomes ready only if
// that string split back into the original three arguments.
func TestAnExecFormHealthcheckSurvivesTheShell(t *testing.T) {
	t.Parallel()

	f, _ := seeded(t, writeCompose(t, probeCompose()))

	test := values(field(f.serviceContainer(t, rules.KindSeed, "probe"), "Config", "Healthcheck", "Test"))
	want := []any{"CMD-SHELL", `'test' '-f' '/tmp/it'\''s a "b c" $HOME'`}

	if !slices.Equal(test, want) {
		t.Errorf("the engine holds the healthcheck %q, want %q", test, want)
	}
}
