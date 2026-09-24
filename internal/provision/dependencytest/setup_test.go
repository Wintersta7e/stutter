package dependencytest_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
	"github.com/Wintersta7e/stutter/internal/topology"
)

const (
	// targetService is the service under test in every fixture. It is never started here.
	targetService = "app"
	// startupLimit bounds each classification handshake: the startup limit a check passes, shortened so
	// a port nothing serves costs the suite less.
	startupLimit = 10 * time.Second
)

func TestMain(m *testing.M) {
	dockertest.Main(m)
}

// engineSlots bounds how many of this package's tests use the engine at once: each starts Postgres
// clusters and copy helpers on the one engine the whole run shares.
var engineSlots = make(chan struct{}, 2)

// requireEngine is the gate every test here passes first, then its slot.
func requireEngine(t *testing.T) dockertest.Engine {
	t.Helper()

	engine := dockertest.Require(t)

	select {
	case engineSlots <- struct{}{}:
	case <-t.Context().Done():
		t.Fatal("the test ended waiting for an engine slot")
	}

	t.Cleanup(func() { <-engineSlots })

	return engine
}

// openEngine opens a check against the real engine. The test ends with it closed, and nothing carrying
// its label left behind.
func openEngine(t *testing.T) *provision.Engine {
	t.Helper()

	state := t.TempDir()
	//nolint:gosec // a directory needs its search bit; 0700 is Open's own requirement.
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}

	eng, err := provision.Open(t.Context(), provision.Options{StateDir: state, TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("provision.Open() error = %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if down := eng.Close(ctx, provision.DiscardLogs); len(down.Listing) != 0 {
			t.Errorf("the engine still holds %d resources of check %s: %+v", len(down.Listing), eng.CheckID(),
				down.Listing)
		}
	})

	return eng
}

// fixture is one check's compose model, pinned images and dependency network on the real engine.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type fixture struct {
	eng     *provision.Engine
	docker  *dockertest.Docker
	model   *compose.Model
	images  map[string]compose.Image
	network *provision.Network
	// helper is the relay image the copy helper runs from, when the fixture was asked for one.
	helper compose.Image
	cls    compose.Classification
}

// fixtureOptions say what a fixture prepares beyond the model, images and network.
type fixtureOptions struct {
	// classify runs the classification containers and applies their answers.
	classify bool
	// helper imports the relay image, which every copy runs from.
	helper bool
}

// fixturePath is a fixture in testdata, absolute as compose.Parse requires.
func fixturePath(t *testing.T, name string) string {
	t.Helper()

	path, err := filepath.Abs(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}

	return path
}

// newFixture parses composeFile, pins every started service's image, imports the relay image when
// asked, and creates the dependency network on a free subnet: everything the engine must hold before
// its first container, in that order.
func newFixture(t *testing.T, composeFile string, opts fixtureOptions) *fixture {
	t.Helper()

	engine := requireEngine(t)
	eng := openEngine(t)
	f := &fixture{eng: eng, docker: engine.Docker(t), images: map[string]compose.Image{}}

	f.docker.OwnCheck(t, eng.CheckID())

	model, err := compose.Parse(t.Context(), eng.ComposeConfig, compose.Inputs{
		Service: targetService, Files: []string{composeFile},
	})
	if err != nil {
		t.Fatalf("compose.Parse(%s) error = %v", composeFile, err)
	}

	f.model = model
	f.cls = f.classify(t, nil)
	f.pin(t)
	f.cls = f.classify(t, nil)

	if opts.helper {
		if f.helper, err = topology.Image(t.Context(), eng, engine.Binary(t, "./cmd/stutter")); err != nil {
			t.Fatalf("topology.Image() error = %v", err)
		}
	}

	if f.network, err = eng.CreateFreeNetwork(t.Context(), "dependency", false); err != nil {
		t.Fatalf("CreateFreeNetwork() error = %v", err)
	}

	if opts.classify {
		answers, classifyErr := provision.Classify(t.Context(), f.classifyConfig())
		if classifyErr != nil {
			t.Fatalf("provision.Classify() error = %v", classifyErr)
		}

		f.cls = f.classify(t, answers)
	}

	return f
}

// classify runs compose's classification over the images pinned so far.
func (f *fixture) classify(t *testing.T, answers map[string]map[uint16]pg.Answer) compose.Classification {
	t.Helper()

	cls, err := compose.Classify(f.model, f.images, answers)
	if err != nil {
		t.Fatalf("compose.Classify() error = %v", err)
	}

	return cls
}

// pin resolves the image of every service the check starts but the service under test, which these
// tests never start.
func (f *fixture) pin(t *testing.T) {
	t.Helper()

	started := slices.DeleteFunc(f.cls.Started(), func(service string) bool { return service == targetService })

	for _, ref := range f.model.Images(started) {
		image, err := f.eng.ResolveImage(t.Context(), ref.Ref, ref.Platform)
		if err != nil {
			t.Fatalf("ResolveImage(%s) error = %v", ref.Ref, err)
		}

		f.images[ref.Service] = image
	}
}

func (f *fixture) classifyConfig() provision.ClassifyConfig {
	return provision.ClassifyConfig{
		Engine: f.eng, Model: f.model, Images: f.images, Network: f.network, Classification: f.cls,
		Startup: startupLimit,
	}
}

// inspected decodes an engine inspect; nil when the engine holds no such object.
func inspected(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()

	if raw == nil {
		return nil
	}

	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode an inspect: %v", err)
	}

	return out
}

// field walks a decoded inspect by keys.
func field(value any, keys ...string) any {
	for _, key := range keys {
		m, ok := value.(map[string]any)
		if !ok {
			return nil
		}

		value = m[key]
	}

	return value
}

// containersOfKind are the containers the engine holds for this fixture's check with kind, read from
// the engine itself.
func (f *fixture) containersOfKind(t *testing.T, kind rules.Kind) []string {
	t.Helper()

	var out []string

	for _, id := range f.docker.Listing(t, rules.LabelCheck+"="+f.eng.CheckID()).Containers {
		got := inspected(t, f.docker.Inspect(t, dockertest.ObjectContainer, id))
		if field(got, "Config", "Labels", rules.LabelKind) == string(kind) {
			out = append(out, id)
		}
	}

	slices.Sort(out)

	return out
}

// stopwatch is a monotonic stopwatch.
type stopwatch struct {
	started time.Time
}

func startStopwatch() stopwatch { return stopwatch{started: time.Now()} }

func (s stopwatch) elapsed() time.Duration { return time.Since(s.started) }
