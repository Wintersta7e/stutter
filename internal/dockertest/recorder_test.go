package dockertest_test

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// checkID returns a random check ID for a test's own image scheme.
func checkID(t *testing.T) string {
	t.Helper()

	return random(t) + random(t)
}

// stutterImage imports, through docker, an image holding the static stutter binary as /stutter that
// prints its version and exits, declares a volume, and is labelled a target — never with a check
// label, which a real check's sweep would list.
func stutterImage(t *testing.T, engine dockertest.Engine, docker *dockertest.Docker, check string) string {
	t.Helper()

	binary, err := os.ReadFile(engine.Binary(t, "./cmd/stutter"))
	if err != nil {
		t.Fatal(err)
	}

	var layer bytes.Buffer

	w := tar.NewWriter(&layer)
	if err := w.WriteHeader(&tar.Header{Name: "stutter", Mode: 0o755, Size: int64(len(binary))}); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Write(binary); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return docker.Import(t, &layer, "stutter.invalid/"+check+"/test:1", []string{
		`ENTRYPOINT ["/stutter", "version"]`,
		"VOLUME /data",
		"LABEL " + rules.LabelKind + "=" + string(rules.KindTarget),
	})
}

func TestTheRecorderSeesEveryCallTheCLIMakes(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	recorder := engine.Recorder(t)
	docker := engine.DockerVia(t, recorder)
	check := checkID(t)

	image := stutterImage(t, engine, docker, check)
	id := docker.Create(t, dockertest.CreateSpec{Image: image, Network: "none"})
	docker.Start(t, id)

	if code := docker.Wait(t, id); code != 0 {
		t.Fatalf("the version container exited %d", code)
	}

	docker.Remove(t, id)

	for _, c := range recorder.Calls() {
		if c.Status == 0 {
			t.Errorf("call %d %s %s has no status", c.Seq, c.Method, c.Path)
		}
	}

	created := recorder.Created()
	if !slices.Contains(created.Containers, id) || !slices.Contains(created.Images, image) {
		t.Errorf("created %+v; want container %s and image %s", created, id, image)
	}

	s := recorder.Summary(check)
	if s.Calls == 0 || s.Mutating < 4 || s.Prune != 0 || s.ForeignTouched != 0 || s.Err() != nil {
		t.Fatalf("%s: %v; want calls>0 mutating>=4 prune=0 foreign-touched=0", s.Line(check), s.Err())
	}
}

func TestAForeignTargetIsCounted(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	direct := engine.Docker(t)
	id := direct.Create(t, dockertest.CreateSpec{Image: stutterImage(t, engine, direct, checkID(t))})

	recorder := engine.Recorder(t)
	engine.DockerVia(t, recorder).Start(t, id)

	check := checkID(t)
	s := recorder.Summary(check)

	if s.ForeignTouched != 1 || s.Err() == nil || !strings.Contains(s.Err().Error(), "/containers/"+id+"/start") {
		t.Fatalf("%s: %v; want foreign-touched=1 naming the start", s.Line(check), s.Err())
	}
}

func TestABypassedRecorderFails(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	recorder := engine.Recorder(t)
	engine.Docker(t).CreateVolume(t, "", nil)

	check := checkID(t)
	s := recorder.Summary(check)

	if line := s.Line(check); !strings.Contains(line, " calls=0 ") || s.Err() == nil {
		t.Fatalf("%s: %v; a recorder nothing went through must fail", line, s.Err())
	}
}

// isolated is a container on no network, so joining one is never what an audit reads.
func isolated(image string) dockertest.CreateSpec {
	return dockertest.CreateSpec{Image: image, Network: "none"}
}

func TestTheRecorderRecordsAnAnonymousVolumeAtCreate(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	recorder := engine.Recorder(t)
	docker := engine.DockerVia(t, recorder)
	check := checkID(t)

	id := docker.Create(t, isolated(stutterImage(t, engine, docker, check)))
	docker.Remove(t, id)

	var mine []dockertest.Inspect

	for _, in := range recorder.Inspects() {
		if in.ID == id {
			mine = append(mine, in)
		}
	}

	if len(mine) != 1 || mine[0].Phase != dockertest.PhaseCreate {
		t.Fatalf("inspects of the never-started container: %+v; want exactly one, at create", mine)
	}

	if m := mine[0].Mounts; len(m) != 1 || m[0].Type != "volume" || m[0].Destination != "/data" ||
		!slices.Contains(recorder.Created().Volumes, m[0].Name) {
		t.Fatalf("mounts %+v, created volumes %q; want the image's anonymous volume at /data, created", m,
			recorder.Created().Volumes)
	}

	if s := recorder.Summary(check); s.Err() != nil {
		t.Fatalf("%s: %v", s.Line(check), s.Err())
	}
}

// runState is a container's state as an inspect reports it.
type runState struct {
	Status string `json:"Status"` //nolint:tagliatelle // the engine's own field name
}

// containerState is the part of a container inspect that holds its state.
type containerState struct {
	State runState `json:"State"` //nolint:tagliatelle // the engine's own field name
}

func TestTheRecorderInspectsAtCreateAndAtStart(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	recorder := engine.Recorder(t)
	docker := engine.DockerVia(t, recorder)

	id := docker.Create(t, isolated(stutterImage(t, engine, docker, checkID(t))))
	docker.Start(t, id)
	docker.Wait(t, id)

	var phases []dockertest.Phase

	seq := 0

	for _, in := range recorder.Inspects() {
		if in.ID != id {
			continue
		}

		if in.Seq <= seq {
			t.Errorf("inspect at %s has seq %d after %d", in.Phase, in.Seq, seq)
		}

		seq = in.Seq
		phases = append(phases, in.Phase)

		var state containerState

		if err := json.Unmarshal(in.Raw, &state); err != nil {
			t.Fatal(err)
		}

		if in.Phase == dockertest.PhaseStart && state.State.Status != "running" && state.State.Status != "exited" {
			t.Errorf("at start the container is %q; want running or exited", state.State.Status)
		}
	}

	if !slices.Equal(phases, []dockertest.Phase{dockertest.PhaseCreate, dockertest.PhaseStart}) {
		t.Fatalf("inspect phases %q; want create then start", phases)
	}
}

func TestMountingAPreexistingVolumeIsForeign(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	direct := engine.Docker(t)
	volume := direct.CreateVolume(t, "", nil)

	recorder := engine.Recorder(t)
	docker := engine.DockerVia(t, recorder)
	check := checkID(t)

	spec := isolated(stutterImage(t, engine, docker, check))
	spec.Mounts = []string{"type=volume,source=" + volume + ",target=/data"}
	docker.Create(t, spec)

	if s := recorder.Summary(check); s.ForeignTouched != 1 || s.Err() == nil {
		t.Fatalf("%s: %v; a container mounting a volume that existed before the invocation touched it",
			s.Line(check), s.Err())
	}
}
