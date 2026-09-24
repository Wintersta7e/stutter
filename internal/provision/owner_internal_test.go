//go:build linux

package provision

import (
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// lastEntry decodes a ledger line.
func lastEntry(t *testing.T, line string) entry {
	t.Helper()

	var e entry
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		t.Fatalf("ledger line %q: %v", line, err)
	}

	return e
}

// entriesFor returns the ops the ledger holds for seq, in order.
func entriesFor(t *testing.T, e *Engine, seq int) []op {
	t.Helper()

	got, err := loadLedger(e.book.led.path)
	if err != nil {
		t.Fatal(err)
	}

	var ops []op

	for _, en := range got.entries {
		if en.Seq == seq {
			ops = append(ops, en.Op)
		}
	}

	return ops
}

// Every create reaches the engine only after its intent is on disk: a kill at any instruction
// leaves a ledger that names what might exist.
func TestTheIntentIsSyncedBeforeTheCreate(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)

	network, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.1.0/24"))
	if err != nil {
		t.Fatalf("CreateNetwork: %v", err)
	}

	if _, err := engine.CreateVolume(t.Context(), rules.KindTemplateVolume, "db"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	if err := engine.Remove(t.Context(), network); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	creates := 0

	for _, c := range fake.calls {
		if c.verb != "networkCreate" && c.verb != "volumeCreate" {
			continue
		}

		creates++

		before := lastEntry(t, c.lastLedger)
		if before.Op != opIntent || before.Name != c.argv[len(c.argv)-1] {
			t.Errorf("%s %v reached the engine after %+v, not its intent", c.verb, c.argv, before)
		}
	}

	t.Logf("creates=%d", creates)

	if creates != 2 {
		t.Fatalf("saw %d creates, want 2", creates)
	}
}

// A volume create returns a pre-existing volume of that name without applying any label. Such a
// volume is not provably this check's: it is never mounted and never removed.
func TestAVolumeReturnedWithoutOurLabelsIsNeverUsed(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	seq := engine.book.led.seq + 1
	name := volumeName(engine.id, rules.KindTemplateVolume, seq)
	fake.add(ResourceVolume, &fakeObject{id: name, name: name, labels: map[string]string{"owner": "user"}})

	_, err := engine.CreateVolume(t.Context(), rules.KindTemplateVolume, "db")
	if !errors.Is(err, ErrNotOurs) || !strings.Contains(err.Error(), name) {
		t.Fatalf("CreateVolume = %v, want ErrNotOurs naming %s", err, name)
	}

	if removes := fake.verbCalls("volumeRemove"); len(removes) != 0 {
		t.Errorf("the foreign volume was removed: %v", removes)
	}

	if got := fake.find(ResourceVolume, name); got == nil || got.labels["owner"] != "user" {
		t.Errorf("the foreign volume changed: %+v", got)
	}

	if ops := entriesFor(t, engine, seq); len(ops) != 2 || ops[0] != opIntent || ops[1] != opCreated {
		t.Errorf("ledger holds %v for the volume, want intent and created only", ops)
	}
}

// An intent without its created line is resolved by name: this check's resource is removed, a
// foreign one under the name is never touched, and nothing found is recorded absent.
func TestAnInterruptedCreateIsResolvedByName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		labels  func(e *Engine) map[string]string
		name    string
		want    op
		present bool
		gone    bool
	}{
		{name: "ours", present: true, labels: func(e *Engine) map[string]string {
			return ourLabels(e, rules.KindNetwork)
		}, want: opRemoved, gone: true},
		{name: "foreign", present: true, labels: func(*Engine) map[string]string {
			return map[string]string{rules.LabelCheck: strings.Repeat("f", 32), rules.LabelKind: "network"}
		}, want: opAbsent},
		{name: "missing", want: opAbsent},
	}

	for _, tc := range cases {
		engine, fake := openFakeEngine(t)
		seq := engine.book.led.next()
		name := networkName(engine.id, "service")

		if err := engine.book.note(entry{
			Seq: seq, Op: opIntent, Type: ResourceNetwork, Kind: rules.KindNetwork, Name: name,
		}); err != nil {
			t.Fatal(err)
		}

		var id string
		if tc.present {
			id = fake.add(ResourceNetwork, &fakeObject{name: name, labels: tc.labels(engine), subnet: "10.231.2.0/24"})
		}

		err := engine.resolveIntent(t.Context(), engine.book, *engine.book.records[seq])
		if tc.name == "foreign" && !errors.Is(err, ErrNotOurs) {
			t.Errorf("%s: resolveIntent = %v, want the foreign network reported", tc.name, err)
		}

		if ops := entriesFor(t, engine, seq); ops[len(ops)-1] != tc.want {
			t.Errorf("%s: ledger ends %v, want %s", tc.name, ops, tc.want)
		}

		if tc.present && (fake.find(ResourceNetwork, id) == nil) != tc.gone {
			t.Errorf("%s: network present afterwards = %v, want %v", tc.name, fake.find(ResourceNetwork, id) != nil,
				!tc.gone)
		}
	}
}

// An image reference that now resolves to a different ID was moved by someone else: the image it
// names is not the one the check made, so nothing is removed.
func TestAMovedReferenceIsNeverRemoved(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	seq := engine.book.led.next()
	ref := imageRef(engine.id, rules.KindSnapshot, seq)

	for _, en := range []entry{
		{Seq: seq, Op: opIntent, Type: ResourceImage, Kind: rules.KindSnapshot, Name: ref},
		{Seq: seq, Op: opCreated, Type: ResourceImage, ID: "sha256:" + strings.Repeat("a", 64)},
		{Seq: seq, Op: opVerified, Type: ResourceImage},
	} {
		if err := engine.book.note(en); err != nil {
			t.Fatal(err)
		}
	}

	moved := fake.add(ResourceImage, &fakeObject{
		id: "sha256:" + strings.Repeat("b", 64), tags: []string{ref}, labels: ourLabels(engine, rules.KindSnapshot),
	})

	err := engine.removeRecorded(t.Context(), engine.book, *engine.book.records[seq])
	if err == nil || !strings.Contains(err.Error(), "moved") {
		t.Errorf("removeRecorded = %v, want the moved reference refused", err)
	}

	if removes := fake.verbCalls("imageRemove"); len(removes) != 0 || fake.find(ResourceImage, moved) == nil {
		t.Errorf("the moved image was removed: %v", removes)
	}

	if ops := entriesFor(t, engine, seq); ops[len(ops)-1] != opRemoveFailed {
		t.Errorf("ledger ends %v, want remove-failed", ops)
	}
}

// A resource whose labels no longer name this check and kind is not provably the one created: the
// pre-remove verification refuses it every time.
func TestAResourceWhoseLabelsChangedIsNeverRemoved(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)

	network, err := engine.CreateNetwork(t.Context(), "bus", false, netip.MustParsePrefix("10.231.3.0/24"))
	if err != nil {
		t.Fatal(err)
	}

	fake.find(ResourceNetwork, network.ID()).labels[rules.LabelKind] = string(rules.KindTarget)

	if err := engine.Remove(t.Context(), network); err == nil {
		t.Error("a network whose labels changed was removed")
	}

	if removes := fake.verbCalls("networkRemove"); len(removes) != 0 {
		t.Errorf("networkRemove was issued: %v", removes)
	}

	if ops := entriesFor(t, engine, network.seq); ops[len(ops)-1] != opRemoveFailed {
		t.Errorf("ledger ends %v, want remove-failed", ops)
	}
}

// "Gone" is read from the engine: an inspect that fails counts as missing only on an engine that
// answers. An unreachable engine is a failure, never a removal.
func TestGoneIsReadFromTheEngineNotFromStderr(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	prefix := netip.MustParsePrefix("10.231.4.0/24")

	network, err := engine.CreateNetwork(t.Context(), "bus", false, prefix)
	if err != nil {
		t.Fatal(err)
	}

	fake.unreachable = true

	if err := engine.Remove(t.Context(), network); err == nil {
		t.Error("Remove against an unreachable engine reported success")
	}

	if ops := entriesFor(t, engine, network.seq); ops[len(ops)-1] != opRemoveFailed {
		t.Errorf("unreachable: ledger ends %v, want remove-failed", ops)
	}

	fake.unreachable = false
	delete(fake.objects[ResourceNetwork], network.ID())

	if err := engine.Remove(t.Context(), network); err != nil {
		t.Errorf("Remove of a network already gone = %v, want nil", err)
	}

	if ops := entriesFor(t, engine, network.seq); ops[len(ops)-1] != opRemoved {
		t.Errorf("reachable and missing: ledger ends %v, want removed", ops)
	}
}

// A read whose template fails on an object the engine holds exits non-zero, exactly as a read of a
// missing object does. It is unreadable, never absent: read as absent, a network just created was
// reported gone, and a listing's inspect would pass over a network it could not read.
func TestAnUnreadableObjectIsNeverAbsent(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	held := fake.add(ResourceNetwork, &fakeObject{name: "held", subnet: "10.231.11.0/24", labels: map[string]string{}})
	fake.unreadable = map[string]bool{networkTemplate: true}

	inspect := func(ref string) (bool, error) {
		var report networkReport

		req := request{verb: verbNetworkInspect, args: []arg{{val: networkTemplate}, {val: ref}}}

		return engine.read(t.Context(), req, &report)
	}

	found, err := inspect(held)
	if found || !errors.Is(err, ErrEngine) {
		t.Fatalf("read of a network the engine holds, through a failing template = %v, %v; want ErrEngine", found,
			err)
	}

	found, err = inspect("missing")
	if found || err != nil {
		t.Errorf("read of a missing network = %v, %v; want absent", found, err)
	}

	_, err = engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.12.0/24"))
	if err == nil || strings.Contains(err.Error(), "gone") {
		t.Errorf("CreateNetwork whose read-back fails = %v, want the read's failure", err)
	}
}

// Absent needs the presence inspect to have run and found nothing: one that never finished proves
// nothing, and the read is an error.
func TestAnUnfinishedPresenceReadIsNeverAbsent(t *testing.T) {
	t.Parallel()

	engine := &Engine{run: &fakeCaller{answer: func(req request) (result, error) {
		switch {
		case req.verb == verbVersion:
			return result{}, nil
		case req.args[0].val == presenceTemplate:
			return result{exit: -1}, ErrDeadline
		default:
			return result{exit: 1}, &CallError{Verb: "networkInspect", Code: 1}
		}
	}}}

	var report networkReport

	req := request{verb: verbNetworkInspect, args: []arg{{val: networkTemplate}, {val: "held"}}}

	found, err := engine.read(t.Context(), req, &report)
	if found || !errors.Is(err, ErrDeadline) {
		t.Errorf("read whose presence inspect never finished = %v, %v; want ErrDeadline", found, err)
	}
}

// A subnet that intersects a local interface would black-hole every container's traffic to that
// address: it is refused before any network create.
func TestAnOverlappingSubnetIsNeverCreated(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	engine.interfaces = func() ([]localAddr, error) {
		return []localAddr{{name: "eth0", prefix: netip.MustParsePrefix("10.231.5.9/20")}}, nil
	}

	refused := []string{"10.231.5.0/24", "10.231.0.0/16", "10.231.6.7/24", "fd00::/64"}
	for _, text := range refused {
		_, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix(text))
		if !errors.Is(err, ErrSubnetOverlap) {
			t.Errorf("CreateNetwork(%s) = %v, want ErrSubnetOverlap", text, err)
		}
	}

	if creates := fake.verbCalls("networkCreate"); len(creates) != 0 {
		t.Errorf("network create issued for a refused subnet: %v", creates)
	}

	t.Logf("refused=%d creates=0", len(refused))
}

// The engine refusing a requested subnet is decided first from what the engine holds afterwards: a
// network now holding an intersecting subnet makes it ErrSubnetTaken, and names that network.
func TestAnEngineRefusalIsSubnetTaken(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	fake.add(ResourceNetwork, &fakeObject{name: "theirs", subnet: "10.231.7.0/24", labels: map[string]string{}})

	_, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.7.128/25"))
	if !errors.Is(err, ErrSubnetTaken) {
		t.Fatalf("CreateNetwork = %v, want ErrSubnetTaken", err)
	}

	seq := engine.book.led.seq
	if ops := entriesFor(t, engine, seq); ops[len(ops)-1] != opAbsent {
		t.Errorf("the refused create's intent ends %v, want absent", ops)
	}
}

// A network being created or removed holds its pool while no listing shows it. Measured on a native
// 28.0.4 engine with networks coming and going: the refusal found no holder and was final, so the check
// failed where another subnet was free. The engine's own refusal of the pool is the subnet taken.
func TestARefusalByAnUnlistedNetworkIsSubnetTaken(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	fake.add(ResourceNetwork, &fakeObject{
		name: "leaving", subnet: "10.231.7.0/24", labels: map[string]string{}, unlisted: true,
	})

	_, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.7.0/24"))
	if !errors.Is(err, ErrSubnetTaken) {
		t.Fatalf("CreateNetwork = %v, want ErrSubnetTaken", err)
	}
}

// A network whose read-back differs from the request — another subnet, IPv6 on, not internal — is
// removed and the create fails naming the difference.
func TestANetworkWithTheWrongSubnetIsRemoved(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	fake.afterCreate = func(typ ResourceType, obj *fakeObject) {
		if typ == ResourceNetwork {
			obj.subnet = "10.231.9.0/24"
		}
	}

	_, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.8.0/24"))
	if err == nil || !strings.Contains(err.Error(), "10.231.9.0/24") {
		t.Fatalf("CreateNetwork = %v, want a failure naming the subnet read back", err)
	}

	if removes := fake.verbCalls("networkRemove"); len(removes) != 1 {
		t.Errorf("networkRemove calls = %v, want the wrong network removed", removes)
	}

	if ops := entriesFor(t, engine, engine.book.led.seq); ops[len(ops)-1] != opRemoved {
		t.Errorf("ledger ends %v, want removed", ops)
	}
}

// Occupied lists every local interface prefix and every engine network's IPv4 subnet, and
// InspectNetwork reads back what a created network is.
func TestOccupiedListsInterfacesAndNetworks(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	fake.add(ResourceNetwork, &fakeObject{name: "theirs", subnet: "172.18.0.0/16", labels: map[string]string{}})

	network, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.10.0/24"))
	if err != nil {
		t.Fatal(err)
	}

	occupied, err := engine.Occupied(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]bool{"127.0.0.0/8": false, "172.18.0.0/16": false, "10.231.10.0/24": false}
	for _, o := range occupied {
		want[o.Prefix.String()] = true
	}

	for prefix, seen := range want {
		if !seen {
			t.Errorf("Occupied %v does not hold %s", occupied, prefix)
		}
	}

	state, err := engine.InspectNetwork(t.Context(), network)
	if err != nil {
		t.Fatal(err)
	}

	if state.Subnet != netip.MustParsePrefix("10.231.10.0/24") || !state.Internal || state.IPv6 {
		t.Errorf("InspectNetwork = %+v", state)
	}
}
