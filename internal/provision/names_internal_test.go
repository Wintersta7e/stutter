package provision

import (
	"regexp"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

const testCheckID = "0123456789abcdef0123456789abcdef"

// checkScoped is the shape of every engine name a check gives a resource.
var checkScoped = regexp.MustCompile(`^stutter-[0-9a-f]{32}-[a-z0-9-]+$`)

// The engine's reverse DNS hands a container its own name, so a name that changed between runs would
// put a per-run value into a service that logs or stores it, and the determinism gate would refuse
// a correct service. Every run container keeps one name for the whole check; the probe start and
// discovery keep names of their own.
func TestARunContainerKeepsOneNameForTheCheck(t *testing.T) {
	t.Parallel()

	target3 := containerName(testCheckID, rules.KindTarget, 3)
	target40 := containerName(testCheckID, rules.KindTarget, 40)
	probe := containerName(testCheckID, rules.KindProbe, 3)
	discovery := containerName(testCheckID, rules.KindDiscovery, 3)

	if target3 != target40 {
		t.Errorf("the target is named %q in one run and %q in another", target3, target40)
	}

	if other := containerName(strings.Repeat("f", 32), rules.KindTarget, 3); other == target3 {
		t.Errorf("two checks name their target %q alike", other)
	}

	if probe == target3 || discovery == target3 || probe == discovery {
		t.Errorf("target %q, probe %q and discovery %q are not three names", target3, probe, discovery)
	}

	relay := containerName(testCheckID, rules.KindRelay, 5)
	if relay == containerName(testCheckID, rules.KindRelay, 6) || !strings.HasSuffix(relay, "-5") {
		t.Errorf("relay name %q does not carry its sequence number", relay)
	}

	names := []string{
		target3, probe, discovery, relay,
		containerName(testCheckID, rules.KindSeed, 9), networkName(testCheckID, "service"),
		volumeName(testCheckID, rules.KindTemplateVolume, 11), project(testCheckID),
	}
	for _, name := range names {
		if !checkScoped.MatchString(name) && name != "stutter-"+testCheckID {
			t.Errorf("name %q is not check-scoped", name)
		}
	}

	t.Logf("names=%d", len(names))
}

// Every image Stutter creates lives under its check in the reserved domain, with a tag the engine
// accepts.
func TestAnImageReferenceIsCheckScoped(t *testing.T) {
	t.Parallel()

	repository := regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*$`)
	tag := regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)

	for _, kind := range rules.Kinds() {
		ref := imageRef(testCheckID, kind, 7)

		want := "stutter.invalid/" + testCheckID + "/" + string(kind) + ":7"
		if ref != want {
			t.Errorf("imageRef = %q, want %q", ref, want)
		}

		path, version, _ := strings.Cut(strings.TrimPrefix(ref, rules.ImageDomain+"/"), ":")
		for component := range strings.SplitSeq(path, "/") {
			if !repository.MatchString(component) {
				t.Errorf("%q: path component %q is not repository-legal", ref, component)
			}
		}

		if !tag.MatchString(version) {
			t.Errorf("%q: tag %q is not tag-legal", ref, version)
		}
	}
}

// The `.test` key belongs to the verification suite; the product never writes it.
func TestTheTestLabelIsNeverWritten(t *testing.T) {
	t.Parallel()

	sets := 0

	for _, kind := range rules.Kinds() {
		for _, service := range []string{"", "orders"} {
			labels := labelSet(testCheckID, kind, service)
			sets++

			for key := range labels {
				if strings.HasSuffix(key, ".test") || !strings.HasPrefix(key, rules.Namespace+".") {
					t.Errorf("labelSet(%s, %q) writes %q", kind, service, key)
				}
			}

			if labels[rules.LabelCheck] != testCheckID || labels[rules.LabelKind] != string(kind) {
				t.Errorf("labelSet(%s) = %v, missing the check or kind", kind, labels)
			}

			if _, ok := labels[rules.LabelService]; ok != (service != "") {
				t.Errorf("labelSet(%s, %q): service label present = %v", kind, service, ok)
			}
		}
	}

	t.Logf("label sets=%d", sets)
}

// A check ID is 128 random bits, printed as 32 lowercase hex digits.
func TestACheckIDIsThirtyTwoHex(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool, 1000)

	for range 1000 {
		id, err := newCheckID()
		if err != nil {
			t.Fatal(err)
		}

		if !isCheckID(id) {
			t.Fatalf("check ID %q is not 32 lowercase hex", id)
		}

		if seen[id] {
			t.Fatalf("check ID %q repeated", id)
		}

		seen[id] = true
	}

	bad := []string{"", testCheckID[:31], strings.ToUpper(testCheckID), testCheckID + "0", "../" + testCheckID[3:]}
	for _, text := range bad {
		if isCheckID(text) {
			t.Errorf("isCheckID(%q) = true", text)
		}
	}
}
