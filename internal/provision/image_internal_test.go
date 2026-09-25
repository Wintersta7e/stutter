//go:build linux

package provision

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// dbRef is the reference of the image these tests hold locally.
const dbRef = "db:1"

// A present image is pinned without a pull; an absent one is pulled once, then pinned; a reference
// moved afterwards still resolves to the pinned ID; and nothing is pulled once a container exists.
func TestAnImageIsPinnedOnceAndNeverPulledAgain(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	present := fake.add(ResourceImage, &fakeObject{tags: []string{dbRef}, labels: map[string]string{}})
	fake.registry = map[string]*fakeObject{"app:2": {id: "sha256:" + strings.Repeat("2", 64), labels: nil}}

	pinned, err := engine.ResolveImage(t.Context(), dbRef, "")
	if err != nil || pinned.ID != present {
		t.Fatalf("ResolveImage(db:1) = (%s, %v), want %s", pinned.ID, err, present)
	}

	if pulls := fake.verbCalls("pull"); len(pulls) != 0 {
		t.Errorf("a present image was pulled: %v", pulls)
	}

	pulled, err := engine.ResolveImage(t.Context(), "app:2", "linux/amd64")
	if err != nil || pulled.ID != fake.registry["app:2"].id {
		t.Fatalf("ResolveImage(app:2) = (%s, %v)", pulled.ID, err)
	}

	if pulls := fake.verbCalls("pull"); len(pulls) != 1 || !slices.Contains(pulls[0], "--platform") {
		t.Errorf("pull calls = %v, want one, with the platform", pulls)
	}

	// Someone moves the tag: the check keeps running what it pinned.
	fake.find(ResourceImage, present).tags = nil
	fake.add(ResourceImage, &fakeObject{tags: []string{dbRef}, labels: map[string]string{}})

	again, err := engine.ResolveImage(t.Context(), dbRef, "")
	if err != nil || again.ID != present {
		t.Errorf("ResolveImage after a retag = (%s, %v), want the pinned %s", again.ID, err, present)
	}

	plantRecord(t, engine, fake, ResourceContainer, rules.KindTarget, testService)

	if _, err := engine.ResolveImage(t.Context(), "late:3", ""); err == nil {
		t.Error("an image was resolved after the first container")
	}

	if pulls := fake.verbCalls("pull"); len(pulls) != 1 {
		t.Errorf("pull calls after the first container: %v", pulls)
	}
}

// An inspect that fails on an engine that does not answer is an engine failure, never an absent
// image to pull.
func TestAbsentAndUnreadableAreToldApart(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	fake.unreachable = true

	_, err := engine.ResolveImage(t.Context(), dbRef, "")
	if err == nil || errors.Is(err, ErrImage) {
		t.Errorf("ResolveImage on an unreachable engine = %v, want an engine failure", err)
	}

	if pulls := fake.verbCalls("pull"); len(pulls) != 0 {
		t.Errorf("an unreadable image was pulled: %v", pulls)
	}
}

// A local read finds a present image without pulling or pinning it, reports one only a registry holds
// as absent without pulling it, and fails on an engine that does not answer.
func TestALocalImageReadNeitherPullsNorPins(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	present := fake.add(ResourceImage, &fakeObject{tags: []string{dbRef}, labels: map[string]string{}})
	fake.registry = map[string]*fakeObject{"app:2": {id: "sha256:" + strings.Repeat("2", 64), labels: nil}}

	image, found, err := engine.LocalImage(t.Context(), dbRef)
	if err != nil || !found || image.ID != present {
		t.Fatalf("LocalImage(db:1) = (%s, %v, %v), want %s", image.ID, found, err, present)
	}

	_, registryOnly, registryErr := engine.LocalImage(t.Context(), "app:2")
	if registryErr != nil || registryOnly {
		t.Errorf("LocalImage(app:2) = (%v, %v), want absent: only the registry holds it", registryOnly, registryErr)
	}

	if pulls := fake.verbCalls("pull"); len(pulls) != 0 {
		t.Errorf("a local read pulled: %v", pulls)
	}

	// The tag moves: had the read pinned db:1, resolving it would still return the old ID.
	fake.find(ResourceImage, present).tags = nil
	moved := fake.add(ResourceImage, &fakeObject{tags: []string{dbRef}, labels: map[string]string{}})

	resolved, resolveErr := engine.ResolveImage(t.Context(), dbRef, "")
	if resolveErr != nil || resolved.ID != moved {
		t.Errorf("ResolveImage after a local read and a retag = (%s, %v), want %s", resolved.ID, resolveErr, moved)
	}

	fake.unreachable = true

	if _, unreadable, readErr := engine.LocalImage(t.Context(), dbRef); readErr == nil || unreadable {
		t.Errorf("LocalImage on an unreachable engine = (%v, %v), want an engine failure", unreadable, readErr)
	}
}

// renderBuild is a render function that records what it was handed and renders it for the fake.
func renderBuild(seen *map[string]map[string]string) func(map[string]string, map[string]map[string]string) (
	[]byte, error,
) {
	return func(tags map[string]string, labels map[string]map[string]string) ([]byte, error) {
		*seen = labels
		model := fakeBuild{}

		for service, tag := range tags {
			model[service] = struct {
				Labels map[string]string `json:"labels"`
				Tag    string            `json:"tag"`
			}{Labels: labels[service], Tag: tag}
		}

		return json.Marshal(model)
	}
}

// A build hands every service its own labels — the target, a job and a dependency are different
// kinds — under a project name only this check has, reading the model from stdin.
func TestBuildHandsEveryServiceItsOwnLabels(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)

	var seen map[string]map[string]string

	built, err := engine.Build(t.Context(), BuildSpec{
		Render: renderBuild(&seen),
		Dir:    t.TempDir(),
		Services: map[string]rules.Kind{
			testService: rules.KindTarget,
			"migrate":   rules.KindJob,
			"db":        rules.KindDependency,
		},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for service, kind := range map[string]rules.Kind{testService: "target", "migrate": "job", "db": "dependency"} {
		if seen[service][rules.LabelKind] != string(kind) || seen[service][rules.LabelService] != service {
			t.Errorf("%s was handed labels %v", service, seen[service])
		}

		if built[service].ID == "" || !ours(built[service].Labels, engine.id, kind) {
			t.Errorf("%s built as %+v", service, built[service])
		}
	}

	calls := fake.verbCalls("composeBuild")
	if len(calls) != 1 {
		t.Fatalf("composeBuild calls = %v", calls)
	}

	argv := strings.Join(calls[0], " ")
	if !strings.HasPrefix(argv, "compose -p "+engine.Project()+" -f - build ") {
		t.Errorf("build argv = %q", argv)
	}
}

// An image the build did not label as this check's is not provably the one built: a named failure.
func TestABuildWhoseImageLacksOurLabelsIsRefused(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)
	fake.buildDropsLabels = true

	var seen map[string]map[string]string

	_, err := engine.Build(t.Context(), BuildSpec{
		Render: renderBuild(&seen), Dir: t.TempDir(), Services: map[string]rules.Kind{testService: rules.KindTarget},
	})
	if !errors.Is(err, ErrImage) || !strings.Contains(err.Error(), testService) {
		t.Errorf("Build = %v, want ErrImage naming the service", err)
	}
}

// An image intent without its created line is resolved by reference: this check's image is
// removed, one under the reference that is not this check's is never touched, and nothing found is
// absent.
func TestAnInterruptedImageIntentIsResolvedByReference(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		labels  func(e *Engine) map[string]string
		name    string
		want    op
		present bool
	}{
		{name: "ours", present: true, labels: func(e *Engine) map[string]string {
			return ourLabels(e, rules.KindSnapshot)
		}, want: opRemoved},
		{name: "someone else's", present: true, labels: func(*Engine) map[string]string {
			return map[string]string{rules.LabelCheck: strings.Repeat("f", 32), rules.LabelKind: "snapshot"}
		}, want: opAbsent},
		{name: "missing", want: opAbsent},
	} {
		engine, fake := openFakeEngine(t)
		seq := engine.book.led.next()
		ref := imageRef(engine.id, rules.KindSnapshot, seq)

		appendBook(
			t,
			engine.book,
			entry{Seq: seq, Op: opIntent, Type: ResourceImage, Kind: rules.KindSnapshot, Name: ref},
		)

		var id string
		if tc.present {
			id = fake.add(ResourceImage, &fakeObject{tags: []string{ref}, labels: tc.labels(engine)})
		}

		rec, _ := engine.book.record(seq)
		err := engine.resolveIntent(t.Context(), engine.book, rec)
		t.Logf("%s: resolveIntent = %v", tc.name, err)

		if ops := entriesFor(t, engine, seq); ops[len(ops)-1] != tc.want {
			t.Errorf("%s: ledger ends %v, want %s", tc.name, ops, tc.want)
		}

		if tc.present && (fake.find(ResourceImage, id) == nil) != (tc.want == opRemoved) {
			t.Errorf("%s: image present afterwards = %v", tc.name, fake.find(ResourceImage, id) != nil)
		}
	}
}

// Import and Commit label what they make as this check's, verified by inspect, and pin it.
func TestImportAndCommitPinWhatTheyMake(t *testing.T) {
	t.Parallel()

	engine, fake := openFakeEngine(t)

	relay, err := engine.Import(t.Context(), rules.KindRelay, strings.NewReader("tar"), []string{"CMD [\"/relay\"]"})
	if err != nil || !ours(relay.Labels, engine.id, rules.KindRelay) {
		t.Fatalf("Import = (%+v, %v)", relay, err)
	}

	rec := plantRecord(t, engine, fake, ResourceContainer, rules.KindSeed, "db")
	container := &Container{id: rec.id, name: rec.name, check: engine.id, kind: rules.KindSeed, seq: rec.seq}

	snapshot, err := engine.Commit(t.Context(), container, rules.KindSnapshot)
	if err != nil || !ours(snapshot.Labels, engine.id, rules.KindSnapshot) {
		t.Fatalf("Commit = (%+v, %v)", snapshot, err)
	}

	if !engine.pinnedID(relay.ID) || !engine.pinnedID(snapshot.ID) {
		t.Error("an image the check made is not pinned")
	}
}

// pinnedID reports an image ID this check pinned: resolved, built, imported or committed.
func (e *Engine) pinnedID(id string) bool {
	_, ok := e.pinnedImage(id)

	return ok
}
