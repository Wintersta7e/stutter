package dockertest_test

import (
	"archive/tar"
	"bytes"
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
	id := docker.Create(t, dockertest.CreateSpec{Image: image})
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
