//go:build linux

package provision

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// plant describes another check's ledger a test writes into the state directory.
type plant struct {
	edit      func(h *header)
	entries   func(id string) []entry
	objectID  string
	foreign   bool
	holdLock  bool
	kept      bool
	corrupt   bool
	noObject  bool
	privateOK bool
}

// planted is a ledger written for a test, with the container it owns.
type planted struct {
	led       *ledger
	check     string
	container string
	private   string
}

// selfHeader is the header this process writes: its host, boot, PID namespace and start time.
func selfHeader(t *testing.T) header {
	t.Helper()

	var h header
	if err := fillHost(&h); err != nil {
		t.Fatal(err)
	}

	return h
}

// plantLedger writes another check's ledger, owning one labelled container on the fake engine, as a
// dead process by default: this process's boot and namespace, but a start time no process has.
func plantLedger(t *testing.T, state string, fake *fakeEngine, p plant) planted {
	t.Helper()

	id, err := newCheckID()
	if err != nil {
		t.Fatal(err)
	}

	self := selfHeader(t)
	hdr := header{
		Check: id, EngineID: testIdentity().EngineID, Endpoint: testIdentity().Endpoint, Host: self.Host,
		BootID: self.BootID, PIDNS: self.PIDNS, PID: self.PID, StartTime: self.StartTime + 1,
		PrivateDir: filepath.Join(t.TempDir(), "stutter-"+id), Project: project(id),
	}

	if p.edit != nil {
		p.edit(&hdr)
	}

	led, err := createLedger(state, defaultHostFS(), hdr)
	if err != nil {
		t.Fatal(err)
	}

	out := planted{led: led, check: id, private: hdr.PrivateDir}
	labels := labelSet(id, rules.KindTarget, "")

	if p.foreign {
		labels = map[string]string{rules.LabelCheck: strings.Repeat("e", 32), rules.LabelKind: string(rules.KindTarget)}
	}

	if !p.noObject {
		out.container = fake.add(ResourceContainer, &fakeObject{
			id: p.objectID, name: containerName(id, rules.KindTarget, 1), labels: labels,
		})
	}

	name := containerName(id, rules.KindTarget, 1)
	entries := []entry{
		{Seq: 1, Op: opIntent, Type: ResourceContainer, Kind: rules.KindTarget, Name: name},
		{Seq: 1, Op: opCreated, Type: ResourceContainer, ID: out.container},
		{Seq: 1, Op: opVerified, Type: ResourceContainer},
	}

	if p.entries != nil {
		entries = p.entries(id)
	}

	if p.kept {
		entries = append(entries, entry{Op: opKept})
	}

	appendAll(t, led, entries...)

	if p.corrupt {
		appendRaw(t, led.path, "not json\n")
		appendAll(t, led, entry{Seq: 2, Op: opIntent, Type: ResourceVolume, Kind: rules.KindSeed, Name: "v"})
	}

	if p.privateOK {
		if err := os.Mkdir(hdr.PrivateDir, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	if !p.holdLock {
		closeLedger(t, led)
	} else {
		t.Cleanup(func() { closeLedger(t, led) })
	}

	return out
}

// Only a check whose owner is provably dead is swept: same engine, same host, its lock free, and
// either another boot or — in this PID namespace — no process with its PID and start time. There is
// no age rule.
func TestSweepEligibility(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), filepath.Join(t.TempDir(), "state")
	self := selfHeader(t)

	cases := []struct {
		name  string
		p     plant
		swept bool
	}{
		{name: "foreign engine", p: plant{edit: func(h *header) { h.EngineID = "engine-b" }}},
		{name: "foreign host", p: plant{edit: func(h *header) { h.Host = "elsewhere" }}},
		{name: "lock held", p: plant{holdLock: true}},
		{name: "live owner", p: plant{edit: func(h *header) { h.StartTime = self.StartTime }}},
		{name: "foreign PID namespace", p: plant{edit: func(h *header) { h.PIDNS = "pid:[1]" }}},
		{name: "kept", p: plant{kept: true}},
		{name: "corrupt", p: plant{corrupt: true}},
		{name: "another boot", p: plant{edit: func(h *header) {
			h.BootID, h.StartTime = "00000000-0000-0000-0000-000000000000", self.StartTime
		}}, swept: true},
		{name: "dead owner", p: plant{}, swept: true},
	}

	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}

	plants := make([]planted, len(cases))
	for i, tc := range cases {
		plants[i] = plantLedger(t, state, fake, tc.p)
	}

	engine := openFakeEngineWith(t, state, fake)
	result := engine.Sweep()
	eligible, skipped := 0, 0

	for i, tc := range cases {
		present := fake.find(ResourceContainer, plants[i].container) != nil
		if present == tc.swept {
			t.Errorf("%s: container present = %v, want swept = %v", tc.name, present, tc.swept)
		}

		if tc.swept {
			eligible++
		} else {
			skipped++
		}
	}

	t.Logf("eligible=%d skipped=%d swept=%v skipped=%v corrupt=%v", eligible, skipped, len(result.Swept),
		len(result.Skipped), result.Corrupt)

	if len(result.Swept) != eligible || len(result.Skipped)+len(result.Corrupt) != skipped || eligible == 0 ||
		skipped == 0 {
		t.Errorf("result swept %d skipped %d corrupt %d; want %d and %d", len(result.Swept), len(result.Skipped),
			len(result.Corrupt), eligible, skipped)
	}
}

// A dead ledger edited to name another resource's ID never removes it: every removal is verified
// against the ledger's own check first.
func TestAnEditedLedgerEntryIsVerifiedBeforeRemoval(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}

	decoy := plantLedger(t, state, fake, plant{foreign: true})
	engine := openFakeEngineWith(t, state, fake)

	if fake.find(ResourceContainer, decoy.container) == nil {
		t.Fatal("the decoy named by the edited ledger was removed")
	}

	if removes := fake.verbCalls(removeVerb); len(removes) != 0 {
		t.Errorf("rm was issued: %v", removes)
	}

	failed := engine.Sweep().Failed
	if len(failed) != 1 || failed[0].ID != decoy.container || failed[0].Check != decoy.check {
		t.Errorf("Failed = %+v, want the decoy reported against its ledger", failed)
	}
}

// A temporary ledger whose lock is free holds no intent — the rename precedes every create — and is
// deleted; one whose lock is held is a check still starting.
func TestALeftoverTmpLedgerIsDeleted(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}

	free := filepath.Join(state, strings.Repeat("1", 32)+".ledger.tmp")
	if err := os.WriteFile(free, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	held := filepath.Join(state, strings.Repeat("2", 32)+".ledger.tmp")
	if err := os.WriteFile(held, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	holder, err := os.Open(held)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFile(t, holder)

	if locked, err := defaultHostFS().tryLock(holder); err != nil || !locked {
		t.Fatalf("hold the lock: %v", err)
	}

	engine := openFakeEngineWith(t, state, fake)

	if _, err := os.Lstat(free); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the free temporary ledger remains: %v", err)
	}

	if _, err := os.Lstat(held); err != nil {
		t.Errorf("the held temporary ledger was deleted: %v", err)
	}

	if got := engine.Sweep().TmpDeleted; got != 1 {
		t.Errorf("TmpDeleted = %d, want 1", got)
	}
}

// Resources carrying the check label but in no ledger here — another host, another distro, a lost
// state directory — are listed by check ID and never removed.
func TestUnledgeredLabelledResourcesAreListedNeverRemoved(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), filepath.Join(t.TempDir(), "state")
	orphan := strings.Repeat("c", 32)
	id := fake.add(ResourceContainer, &fakeObject{
		name: containerName(orphan, rules.KindTarget, 1), labels: labelSet(orphan, rules.KindTarget, ""),
	})
	volume := fake.add(ResourceVolume, &fakeObject{
		id: volumeName(orphan, rules.KindSeed, 2), name: volumeName(orphan, rules.KindSeed, 2),
		labels: labelSet(orphan, rules.KindSeed, ""),
	})

	engine := openFakeEngineWith(t, state, fake)
	listed := engine.Sweep().Unledgered[orphan]

	if len(listed) != 2 || fake.find(ResourceContainer, id) == nil || fake.find(ResourceVolume, volume) == nil {
		t.Errorf("Unledgered[%s] = %+v; both resources must be listed and still present", orphan, listed)
	}

	for _, verb := range []string{removeVerb, "volumeRemove"} {
		if calls := fake.verbCalls(verb); len(calls) != 0 {
			t.Errorf("%s was issued: %v", verb, calls)
		}
	}

	t.Logf("unledgered=%d", len(listed))
}

// An intent resolved absent keeps its ledger: the daemon may still complete the create. Only a
// later sweep that finds it absent again lets the ledger go.
func TestAnAbsentIntentKeepsTheLedgerUntilConfirmed(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}

	dead := plantLedger(t, state, fake, plant{noObject: true, entries: func(id string) []entry {
		return []entry{{
			Seq: 1, Op: opIntent, Type: ResourceNetwork, Kind: rules.KindNetwork,
			Name: networkName(id, "service"),
		}}
	}})

	openFakeEngineWith(t, state, fake)

	if _, err := os.Lstat(dead.led.path); err != nil {
		t.Fatalf("the first sweep deleted a ledger holding a fresh absent: %v", err)
	}

	openFakeEngineWith(t, state, fake)

	if _, err := os.Lstat(dead.led.path); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the second sweep kept a ledger whose absent it confirmed: %v", err)
	}
}

// When the lock self-test is granted, locks are not enforced here, so no ledger can be told dead:
// the sweep removes nothing.
func TestADisabledSweepRemovesNothing(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}

	dead := plantLedger(t, state, fake, plant{})

	host := defaultHostFS()
	host.tryLock = func(*os.File) (bool, error) { return true, nil }

	engine := openFakeEngineHost(t, state, fake, host)

	if !engine.Sweep().Disabled || fake.find(ResourceContainer, dead.container) == nil {
		t.Errorf("a disabled sweep removed %s (Disabled=%v)", dead.container, engine.Sweep().Disabled)
	}

	for _, c := range fake.calls {
		if verbs()[verbOf(c.verb)].mutates {
			t.Errorf("a disabled sweep issued %s %v", c.verb, c.argv)
		}
	}
}

// verbOf finds a verb by its table name.
func verbOf(name string) verb {
	for v := range verbCount {
		if verbs()[v].name == name {
			return v
		}
	}

	return verbCount
}

// A dead check's private directory is reduced to its logs only when its recorded path is still a
// private directory of this user named for that check. Anything else is left and reported.
func TestAPrivateDirectoryOutsideItsRootIsNeverRemoved(t *testing.T) {
	t.Parallel()

	fake, state := newFakeEngine(), filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}

	target := t.TempDir()
	sentinel := filepath.Join(target, "keep")

	if err := os.WriteFile(sentinel, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	symlinked := plantLedger(t, state, fake, plant{noObject: true, entries: noEntries, edit: func(h *header) {
		link := filepath.Join(filepath.Dir(h.PrivateDir), "stutter-"+h.Check)
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}})
	misnamed := plantLedger(t, state, fake, plant{noObject: true, entries: noEntries, edit: func(h *header) {
		h.PrivateDir = target
	}})
	foreign := plantLedger(t, state, fake, plant{noObject: true, entries: noEntries, privateOK: true})
	theirs := filepath.Join(foreign.private, "theirs")

	if err := os.WriteFile(theirs, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	host := defaultHostFS()
	realOwner := host.owner
	host.owner = func(info fs.FileInfo) (int, bool) {
		if info.Name() == filepath.Base(foreign.private) {
			return host.euid + 1, true
		}

		return realOwner(info)
	}

	engine := openFakeEngineHost(t, state, fake, host)

	for _, kept := range []string{sentinel, theirs} {
		if _, err := os.Lstat(kept); err != nil {
			t.Errorf("a directory outside a check's root was reduced: %v", err)
		}
	}

	reported := map[string]bool{}

	for _, f := range engine.Sweep().Failed {
		if f.Type == ResourceHostPath {
			reported[f.Check] = true
		}
	}

	if !reported[symlinked.check] || !reported[misnamed.check] || !reported[foreign.check] {
		t.Errorf("Failed does not report all three refused directories: %+v", engine.Sweep().Failed)
	}
}

func noEntries(string) []entry { return nil }
