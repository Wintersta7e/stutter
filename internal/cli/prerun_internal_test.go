package cli

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
)

// preRunOrder is the pre-run order: every refusal, then the engine and the ledger, the corpus and the
// model, the classification and the start fingerprint before the first mutation, the images, the
// networks and the classification containers, the frozen endpoint split, then the listeners, the
// verified address, the CA file, the relays and the bus.
var preRunOrder = []string{
	"preconditions", "static-self", "host-networking", "open", "corpus", "model", "classify", stepFingerprint,
	"refusals", "images", "reclassify", "bus-urls", "relay-image", "networks", stepClassificationContainers,
	stepDependencies, stepLayout, "listeners", "verify", "ca-file", "relays", stepBus, "seed", stepJobs, stepSnapshot,
	stepCheckpointB0, stepProbe, stepCheckpointB1, "discovery",
}

// Step names several ordering rules name.
const (
	stepClassificationContainers = "classification-containers"
	stepDependencies             = "dependencies"
	stepLayout                   = "layout"
	stepCheckpointB0             = "checkpoint-b0"
	stepCheckpointB1             = "checkpoint-b1"
	stepProbe                    = "probe"
	stepFingerprint              = "fingerprint"
	stepJobs                     = "jobs"
	stepSnapshot                 = "snapshot"
	stepBus                      = "bus"
)

func stepNames(steps []step) []string {
	names := make([]string, 0, len(steps))
	for _, each := range steps {
		names = append(names, each.name)
	}

	return names
}

// TestThePreRunOrderIsTheSpecOrder: the steps run in the order the spec rules, and each ordering rule
// it states holds.
func TestThePreRunOrderIsTheSpecOrder(t *testing.T) {
	t.Parallel()

	names := stepNames(newComposeCheck(composeRun{}, nil).prerunSteps())

	if !slices.Equal(names, preRunOrder) {
		t.Errorf("pre-run steps =\n  %v\nwant\n  %v", names, preRunOrder)
	}

	at := func(name string) int {
		index := slices.Index(names, name)
		if index < 0 {
			t.Fatalf("no %q step", name)
		}

		return index
	}

	rules := []struct {
		why    string
		before []string
		after  []string
	}{
		{"every host refusal precedes the engine and its ledger", []string{
			"preconditions", "static-self", "host-networking",
		}, []string{"open"}},
		{"the start walk and the key refusals precede the first mutation", []string{
			stepFingerprint, "refusals",
		}, []string{"images"}},
		{"a TLS bus URL is refused before anything starts", []string{"bus-urls"}, []string{"relay-image"}},
		{"classification runs between the networks and the listeners", []string{"networks"}, []string{
			stepClassificationContainers, stepDependencies, stepLayout,
		}},
		{"the endpoint split is frozen before any listener", []string{
			stepClassificationContainers, stepDependencies, stepLayout,
		}, []string{"listeners"}},
		{"the CA file follows the verified address", []string{"verify"}, []string{"ca-file"}},
		{"the bus opens after the relays", []string{"relays"}, []string{stepBus}},
		{"seed, jobs and snapshot run in that order before B0", []string{"seed"}, []string{
			stepJobs, stepSnapshot, stepCheckpointB0,
		}},
		{"jobs precede the snapshot", []string{stepJobs}, []string{stepSnapshot}},
		{"the snapshot precedes B0", []string{stepSnapshot}, []string{stepCheckpointB0}},
		{"the start walk precedes the probe start", []string{stepFingerprint}, []string{stepProbe}},
		{"the probe start restores B0", []string{stepCheckpointB0}, []string{stepProbe}},
		{"discovery restores B1", []string{stepCheckpointB1}, []string{"discovery"}},
	}

	for _, rule := range rules {
		for _, before := range rule.before {
			for _, after := range rule.after {
				if at(before) >= at(after) {
					t.Errorf("%s: %s runs at %d, %s at %d", rule.why, before, at(before), after, at(after))
				}
			}
		}

		t.Logf("checked: %s", rule.why)
	}
}

// TestTheUpstreamSourceServesEveryKey: every attach reads the latest restore and where the bus is now,
// never an address kept from before a restart.
func TestTheUpstreamSourceServesEveryKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	bus, err := corpus.Open(t.Context(), filepath.Join(dir, "store"), "ORDERS")
	if err != nil {
		t.Fatalf("open the bus: %v", err)
	}

	t.Cleanup(bus.Close)

	type call struct {
		restored     map[string]netip.AddrPort
		bus, monitor netip.AddrPort
	}

	var calls []call

	check := &composeCheck{bus: bus, upstreamMap: func(
		restored map[string]netip.AddrPort, busAddr, monitor netip.AddrPort,
	) (map[string]netip.AddrPort, error) {
		calls = append(calls, call{restored: restored, bus: busAddr, monitor: monitor})

		return restored, nil
	}}

	first := map[string]netip.AddrPort{"db:5432": netip.MustParseAddrPort("127.0.0.1:49153")}
	check.setRestored(first)

	if _, err = check.upstreams(t.Context()); err != nil {
		t.Fatalf("upstreams: %v", err)
	}

	checkpoint, err := bus.Checkpoint(t.Context(), filepath.Join(dir, "B0"))
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	if err = bus.Restore(t.Context(), checkpoint); err != nil {
		t.Fatalf("restore: %v", err)
	}

	second := map[string]netip.AddrPort{"db:5432": netip.MustParseAddrPort("127.0.0.1:49170")}
	check.setRestored(second)

	if _, err = check.upstreams(t.Context()); err != nil {
		t.Fatalf("upstreams after the restore: %v", err)
	}

	if len(calls) != 2 {
		t.Fatalf("the mapper was called %d times, want 2", len(calls))
	}

	for index, want := range []map[string]netip.AddrPort{first, second} {
		if calls[index].restored["db:5432"] != want["db:5432"] {
			t.Errorf("call %d got restore %v, want %v", index, calls[index].restored, want)
		}
	}

	nowBus, err := addrPortOf(bus.URL())
	if err != nil {
		t.Fatalf("the bus URL %q: %v", bus.URL(), err)
	}

	if calls[1].bus != nowBus || calls[1].monitor != bus.MonitorAddr() {
		t.Errorf("after the restore the mapper got bus %v monitor %v, want %v and %v",
			calls[1].bus, calls[1].monitor, nowBus, bus.MonitorAddr())
	}

	if calls[0].bus == calls[1].bus {
		t.Errorf("the bus kept port %v across a restart, so this proves nothing about re-reading it", calls[0].bus)
	}
}

// selfSigned is one PEM-encoded certificate.
func selfSigned(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "stutter test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// TestTheCAFileIsOnePemBlockReadOnly: the file every target is pointed at holds the check's CA and
// nothing else, and nothing in a container can change it.
func TestTheCAFileIsOnePemBlockReadOnly(t *testing.T) {
	t.Parallel()

	certificate := selfSigned(t)
	path := filepath.Join(t.TempDir(), "ca.pem")

	if err := writeCA(path, certificate); err != nil {
		t.Fatalf("writeCA: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	block, rest := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" || strings.TrimSpace(string(rest)) != "" {
		t.Errorf("the CA file is not exactly one CERTIFICATE block: %q", data)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if mode := info.Mode().Perm(); mode != 0o444 {
		t.Errorf("the CA file's mode = %o, want 444", mode)
	}

	twice := filepath.Join(t.TempDir(), "ca.pem")
	if err := writeCA(twice, append(slices.Clone(certificate), certificate...)); err == nil {
		t.Error("two certificates were written as the check's CA")
	}
}

// TestTheSeedBusBracketClosesOnEveryPath: jobs reach the bus only through the seed bus, which is closed
// whether they succeed or not — and never opened when there is no job.
func TestTheSeedBusBracketClosesOnEveryPath(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		jobsErr    error
		name       string
		discovered []string
		wantOpened int
	}{
		{name: "no job", wantOpened: 0},
		{name: "jobs succeed", discovered: []string{"migrate"}, wantOpened: 1},
		{name: "a job fails", discovered: []string{"migrate"}, jobsErr: errRunFailed, wantOpened: 1},
	} {
		var opened, ran, closed int

		err := runJobs(t.Context(), testCase.discovered,
			func(context.Context) error { opened++; return nil },
			func(context.Context) error { ran++; return testCase.jobsErr },
			func(context.Context) error { closed++; return nil },
		)

		if !errors.Is(err, testCase.jobsErr) || (testCase.jobsErr == nil) != (err == nil) {
			t.Errorf("%s: runJobs = %v, want %v", testCase.name, err, testCase.jobsErr)
		}

		if opened != testCase.wantOpened || ran != testCase.wantOpened || closed != testCase.wantOpened {
			t.Errorf("%s: opened %d, ran %d, closed %d; want %d each", testCase.name, opened, ran, closed,
				testCase.wantOpened)
		}
	}
}

// TestAJobOwnedStreamSkipsTheProbe: a stream that exists at B0 was made by a job, and the probe start
// only ever asks who makes it.
func TestAJobOwnedStreamSkipsTheProbe(t *testing.T) {
	t.Parallel()

	bus, err := corpus.Open(t.Context(), filepath.Join(t.TempDir(), "store"), "ORDERS")
	if err != nil {
		t.Fatalf("open the bus: %v", err)
	}

	t.Cleanup(bus.Close)

	absent, err := bus.Survey(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if !needsProbe(absent) {
		t.Error("a stream absent at B0 skips the probe start")
	}

	messages := []corpus.Message{{Subject: subjectCreated, Payload: []byte("{}"), Seq: 1}}
	if err = bus.Establish(t.Context(), absent, absent, messages); err != nil {
		t.Fatalf("make the stream: %v", err)
	}

	present, err := bus.Survey(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if needsProbe(present) {
		t.Error("a stream present at B0 runs the probe start")
	}
}

// TestAProbeThatBecameDiscoveryIsNotRepeated: a probe start that saw the stream appear read the
// consumers already, on the same start; a second start would only cost time.
func TestAProbeThatBecameDiscoveryIsNotRepeated(t *testing.T) {
	t.Parallel()

	discovered := harness.Discovery{Consumers: []harness.Found{{Name: "reserve"}}}
	calls := 0

	check := newComposeCheck(composeRun{}, nil)
	check.probed = &harness.Probe{Created: true, Discovery: &discovered}
	check.discover = func(context.Context) (harness.Discovery, error) {
		calls++

		return harness.Discovery{}, nil
	}

	if err := check.discovery(t.Context()); err != nil {
		t.Fatalf("discovery: %v", err)
	}

	if calls != 0 {
		t.Errorf("discovery ran %d more starts after the probe start doubled as it", calls)
	}

	if len(check.found.Consumers) != 1 || check.found.Consumers[0].Name != "reserve" {
		t.Errorf("the kept discovery is %+v, want the probe start's", check.found)
	}
}

// imageOrders is the service under test's image in the image fixtures.
const imageOrders = "orders:1"

// TestTheFirstPassSeesOnlyImagesAlreadyOnTheEngine: the first classification reads what the engine
// holds now, without pulling, for every service naming an image — and one that names none is not asked.
func TestTheFirstPassSeesOnlyImagesAlreadyOnTheEngine(t *testing.T) {
	t.Parallel()

	present := map[string]compose.Image{"postgres:18": {ID: "sha256:pg"}, imageOrders: {ID: "sha256:orders"}}

	var asked []string

	lookup := func(_ context.Context, ref string) (compose.Image, bool, error) {
		asked = append(asked, ref)
		image, found := present[ref]

		return image, found, nil
	}

	refs := []compose.ImageRef{
		{Service: testService, Ref: imageOrders},
		{Service: "db", Ref: "postgres:18"},
		{Service: "cache", Ref: "redis:7"},
		{Service: "worker", Build: true},
	}

	local, err := localImages(t.Context(), refs, lookup)
	if err != nil {
		t.Fatalf("localImages: %v", err)
	}

	if len(local) != 2 || local[testService].ID != "sha256:orders" || local["db"].ID != "sha256:pg" {
		t.Errorf("local images = %v, want the two the engine holds, by service", local)
	}

	if slices.Contains(asked, "") || len(asked) != 3 {
		t.Errorf("asked the engine for %q, want the three named images", asked)
	}
}

// TestImageResolutionFollowsThePullPolicy: pull_policy never forbids a pull, build builds, and a
// service with both an image and a build builds only when its image is absent.
func TestImageResolutionFollowsThePullPolicy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		ref     compose.ImageRef
		want    imageStep
		present bool
		refused bool
	}{
		{name: "build with no image", ref: compose.ImageRef{Build: true}, want: imageBuild},
		{
			name: "pull_policy build", ref: compose.ImageRef{Ref: imageOrders, Build: true, Policy: compose.PullBuild},
			present: true, want: imageBuild,
		},
		{name: "image and build, present", ref: compose.ImageRef{
			Ref: imageOrders, Build: true,
			Policy: compose.PullMissing,
		}, present: true, want: imageResolve},
		{name: "image and build, absent", ref: compose.ImageRef{
			Ref: imageOrders, Build: true,
			Policy: compose.PullMissing,
		}, want: imageBuild},
		{
			name: "never, present", ref: compose.ImageRef{Ref: imageOrders, Policy: compose.PullNever}, present: true,
			want: imageResolve,
		},
		{name: "never, absent", ref: compose.ImageRef{Ref: imageOrders, Policy: compose.PullNever}, refused: true},
		{
			name: "missing, absent", ref: compose.ImageRef{Ref: imageOrders, Policy: compose.PullMissing},
			want: imageResolve,
		},
	}

	for _, testCase := range cases {
		testCase.ref.Service = testService

		got, err := imageStepFor(testCase.ref, testCase.present)
		if testCase.refused {
			if !errors.Is(err, errImageAbsent) || !strings.Contains(err.Error(), testService) {
				t.Errorf("%s: = (%v, %v), want %v naming the service", testCase.name, got, err, errImageAbsent)
			}

			continue
		}

		if err != nil || got != testCase.want {
			t.Errorf("%s: = (%v, %v), want %v", testCase.name, got, err, testCase.want)
		}
	}

	t.Logf("cases=%d", len(cases))
}
