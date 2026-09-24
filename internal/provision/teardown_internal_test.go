//go:build linux

package provision

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// plantRecord records a created, verified resource of this check as its create would, and puts it
// on the fake engine. It stands in for the creates of later layers.
func plantRecord(
	t *testing.T, e *Engine, fake *fakeEngine, typ ResourceType, kind rules.Kind, service string,
) record {
	t.Helper()

	seq := e.book.led.next()
	rec := record{seq: seq, typ: typ, kind: kind, service: service, state: opVerified}
	obj := &fakeObject{labels: labelSet(e.id, kind, service)}

	rec.name = containerName(e.id, kind, seq)

	if typ == ResourceImage {
		rec.name = imageRef(e.id, kind, seq)
		obj.tags = []string{rec.name}
	}

	if typ == ResourceVolume {
		rec.name = volumeName(e.id, kind, seq)
		obj.id = rec.name
	}

	obj.name = rec.name
	rec.id = fake.add(typ, obj)

	appendBook(t, e.book,
		entry{Seq: seq, Op: opIntent, Type: typ, Kind: kind, Name: rec.name, Service: service},
		entry{Seq: seq, Op: opCreated, Type: typ, ID: rec.id},
		entry{Seq: seq, Op: opVerified, Type: typ})

	return rec
}

func appendBook(t *testing.T, b *book, entries ...entry) {
	t.Helper()

	for _, en := range entries {
		if err := b.note(en); err != nil {
			t.Fatal(err)
		}
	}
}

// stagePrivate puts the entries a finished check holds into its private directory.
func stagePrivate(t *testing.T, e *Engine) {
	t.Helper()

	for _, name := range []HostName{HostCA, HostStore, HostB0} {
		path, err := e.HostPath(name)
		if err != nil {
			t.Fatal(err)
		}

		if name == HostCA {
			writeTestFile(t, path)

			continue
		}

		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}

		writeTestFile(t, filepath.Join(path, "x"))
	}

	writeTestFile(t, filepath.Join(e.PrivateDir(), logsDir, "target-7.log"))
}

func writeTestFile(t *testing.T, path string) {
	t.Helper()

	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// entriesOf lists what a directory holds, relative, directories marked with a slash.
func entriesOf(t *testing.T, root string) []string {
	t.Helper()

	var out []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == root {
			return err
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		if d.IsDir() {
			rel += "/"
		}

		out = append(out, rel)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return out
}

// Logs outlive a check exactly when its outcome needs them: a verdict discards the directory, any
// other ending keeps only the logs, and a kept check keeps everything.
func TestRetentionFollowsTheOutcome(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		lastOp    op
		want      []string
		retention Retention
		keep      bool
	}{
		{name: "discard", retention: DiscardLogs},
		{
			name: "keep logs", retention: KeepLogs, want: []string{invocationLog, "logs/", "logs/target-7.log"},
			lastOp: opRetained,
		},
		{name: "keep", keep: true, retention: DiscardLogs, lastOp: opKept, want: []string{
			"bus/", "bus/B0/", "bus/B0/x", "bus/store/", "bus/store/x", "ca.pem", "docker-config/",
			invocationLog, "logs/", "logs/target-7.log",
		}},
	}

	kept, removed := 0, 0

	for _, tc := range cases {
		engine, fake := openFakeEngineOpts(t, Options{Keep: tc.keep})

		if _, err := engine.CreateNetwork(
			t.Context(),
			"service",
			true,
			netip.MustParsePrefix("10.231.11.0/24"),
		); err != nil {
			t.Fatal(err)
		}

		plantRecord(t, engine, fake, ResourceContainer, rules.KindTarget, "worker")
		stagePrivate(t, engine)

		down := engine.Close(t.Context(), tc.retention)
		ledgerPath := engine.book.led.path

		if tc.want == nil {
			if _, err := os.Lstat(engine.PrivateDir()); !errors.Is(err, fs.ErrNotExist) || down.PrivateDir != "" {
				t.Errorf("%s: the private directory remains: %v", tc.name, err)
			}

			if _, err := os.Lstat(ledgerPath); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("%s: the ledger remains: %v", tc.name, err)
			}

			removed++

			continue
		}

		got := entriesOf(t, engine.PrivateDir())
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: the private directory holds %v, want %v", tc.name, got, tc.want)
		}

		if info, err := os.Lstat(engine.PrivateDir()); err != nil || info.Mode().Perm() != 0o700 {
			t.Errorf("%s: the private directory is not 0700: %v", tc.name, err)
		}

		for _, log := range down.Logs {
			if _, err := os.Lstat(log); err != nil {
				t.Errorf("%s: Teardown.Logs names a missing file: %v", tc.name, err)
			}
		}

		if tc.retention == KeepLogs && len(down.Logs) == 0 {
			t.Errorf("%s: no log was kept", tc.name)
		}

		if got := lastCheckWide(t, ledgerPath); got != tc.lastOp {
			t.Errorf("%s: the ledger's last check-wide op is %q, want %q", tc.name, got, tc.lastOp)
		}

		kept += len(got)
	}

	t.Logf("kept=%d removed=%d", kept, removed)

	if kept == 0 {
		t.Fatal("nothing was kept")
	}
}

// lastCheckWide returns the ledger's last kept or retained op, if any.
func lastCheckWide(t *testing.T, path string) op {
	t.Helper()

	got, err := loadLedger(path)
	if err != nil {
		t.Fatal(err)
	}

	var last op

	for _, en := range got.entries {
		if en.Op.checkWide() {
			last = en.Op
		}
	}

	return last
}

// Teardown removes by type — containers, then networks, then volumes, then images — and within a
// type the newest first.
func TestTeardownOrderIsByType(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	image := plantRecord(t, engine, fake, ResourceImage, rules.KindSnapshot, "")
	first := plantRecord(t, engine, fake, ResourceContainer, rules.KindSeed, "db")

	if _, err := engine.CreateVolume(t.Context(), rules.KindTemplateVolume, "db"); err != nil {
		t.Fatal(err)
	}

	network, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.12.0/24"))
	if err != nil {
		t.Fatal(err)
	}

	second := plantRecord(t, engine, fake, ResourceContainer, rules.KindTarget, "worker")

	if down := engine.Close(t.Context(), DiscardLogs); down.Err != nil {
		t.Fatalf("Close: %v", down.Err)
	}

	var order []string

	for _, c := range fake.calls {
		switch c.verb {
		case "remove", "networkRemove", "volumeRemove", "imageRemove":
			order = append(order, c.verb+" "+c.argv[len(c.argv)-1])
		default:
		}
	}

	want := []string{
		"remove " + second.id, "remove " + first.id, "networkRemove " + network.ID(),
		"volumeRemove " + volumeName(engine.id, rules.KindTemplateVolume, first.seq+1), "imageRemove " + image.name,
	}

	if !slices.Equal(order, want) {
		t.Errorf("teardown removed %v, want %v", order, want)
	}
}

// Teardown runs to its own bound whatever the caller's context says, and a hung engine ends it at
// that bound with the failure recorded, never a verdict changed.
func TestTeardownIsBoundedAndUncancelled(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)

	network, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.13.0/24"))
	if err != nil {
		t.Fatal(err)
	}

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	if down := engine.Close(
		cancelled,
		DiscardLogs,
	); down.Err != nil ||
		fake.find(ResourceNetwork, network.ID()) != nil {
		t.Errorf("a cancelled caller stopped the teardown: %v", down.Err)
	}

	hung, hangFake := openFakeEngine(t)
	hung.teardownBound = 100 * time.Millisecond

	stuck, err := hung.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.14.0/24"))
	if err != nil {
		t.Fatal(err)
	}

	hangFake.hang = map[string]bool{"networkRemove": true}
	start := time.Now()
	down := hung.Close(t.Context(), DiscardLogs)

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("a hung engine held teardown for %s past a 100ms bound", elapsed)
	}

	if down.Err == nil || len(down.Leftovers) != 1 || down.Leftovers[0].ID != stuck.ID() {
		t.Errorf("Teardown = %+v, want the stuck network a leftover and an error", down)
	}

	if ops := entriesFor(t, hung, stuck.seq); ops[len(ops)-1] != opRemoveFailed {
		t.Errorf("the stuck network's ledger ends %v, want remove-failed", ops)
	}

	if _, err := os.Lstat(hung.book.led.path); err != nil {
		t.Errorf("the ledger of a failed teardown is gone: %v", err)
	}
}

// AUDIT-4: the teardown prints a count line per type, and a labelled resource outside the ledger is
// listed with its ID and never removed.
func TestTheAuditLineCountsEveryType(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)

	if _, err := engine.CreateNetwork(
		t.Context(),
		"service",
		true,
		netip.MustParsePrefix("10.231.15.0/24"),
	); err != nil {
		t.Fatal(err)
	}

	container := plantRecord(t, engine, fake, ResourceContainer, rules.KindTarget, "worker")
	appendBook(t, engine.book, entry{
		Seq: engine.book.led.next(), Op: opCreated, Type: ResourceVolume, Kind: rules.KindTarget,
		Name: strings.Repeat("a", 64), Parent: container.seq,
	})

	planted := fake.add(ResourceVolume, &fakeObject{
		id: "planted", name: "planted", labels: labelSet(engine.id, rules.KindTarget, ""),
	})

	down := engine.Close(t.Context(), DiscardLogs)

	if fake.find(ResourceVolume, planted) == nil {
		t.Error("the planted volume outside the ledger was removed")
	}

	if !slices.ContainsFunc(down.Listing, func(l Listed) bool { return l.ID == planted }) {
		t.Errorf("Listing %+v does not name the planted volume", down.Listing)
	}

	var text bytes.Buffer
	if _, err := down.WriteTo(&text); err != nil {
		t.Fatal(err)
	}

	for _, typ := range []ResourceType{ResourceContainer, ResourceNetwork, ResourceAnonymousVolume} {
		if !strings.Contains(text.String(), string(typ)+" created=1 ") {
			t.Errorf("no count line for %s in:\n%s", typ, text.String())
		}
	}

	if !strings.Contains(text.String(), planted) {
		t.Errorf("the listing line for %s is missing:\n%s", planted, text.String())
	}

	t.Logf("audit:\n%s", text.String())
}

// A kept check names every resource that holds something secret: snapshot images, containers made
// from a compose service, template and restore volumes. A relay and a helper hold none.
func TestAKeptCheckNamesEverySecretBearingResource(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngineOpts(t, Options{Keep: true})

	secret := map[string]record{
		"snapshot": plantRecord(t, engine, fake, ResourceImage, rules.KindSnapshot, "db"),
		"target":   plantRecord(t, engine, fake, ResourceContainer, rules.KindTarget, "worker"),
		"seed":     plantRecord(t, engine, fake, ResourceContainer, rules.KindSeed, "db"),
		"template": plantRecord(t, engine, fake, ResourceVolume, rules.KindTemplateVolume, "db"),
		"restore":  plantRecord(t, engine, fake, ResourceVolume, rules.KindRestore, "db"),
	}
	plain := []record{
		plantRecord(t, engine, fake, ResourceContainer, rules.KindRelay, ""),
		plantRecord(t, engine, fake, ResourceContainer, rules.KindHelper, ""),
	}

	down := engine.Close(t.Context(), DiscardLogs)
	holds := map[string]string{}

	for _, r := range down.Retained {
		holds[r.Name] = r.Holds
	}

	for what, rec := range secret {
		if holds[rec.name] == "" {
			t.Errorf("the kept %s %s is not named with what it holds: %+v", what, rec.name, down.Retained)
		}
	}

	for _, rec := range plain {
		if _, named := holds[rec.name]; named {
			t.Errorf("%s holds nothing secret but is named", rec.name)
		}
	}

	if kills := fake.verbCalls("kill"); len(kills) != 4 {
		t.Errorf("kill calls = %d, want one per kept container", len(kills))
	}

	t.Logf("retained=%d", len(down.Retained))
}
