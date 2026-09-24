package dockertest_test

import (
	"archive/tar"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"slices"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
)

// unlabelledImage is an image no test made, so it carries no test label; CI's test job already pulls
// it for its database.
const unlabelledImage = "postgres:18-alpine"

// oneFileLayer returns a tar holding one small file: enough for an image nothing ever runs.
func oneFileLayer(t *testing.T) *bytes.Reader {
	t.Helper()

	var buf bytes.Buffer

	w := tar.NewWriter(&buf)
	body := []byte("decoy\n")

	if err := w.WriteHeader(&tar.Header{Name: "decoy.txt", Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatalf("tar header: %v", err)
	}

	if _, err := w.Write(body); err != nil {
		t.Fatalf("tar body: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}

	return bytes.NewReader(buf.Bytes())
}

func random(t *testing.T) string {
	t.Helper()

	b := make([]byte, 8)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// decoyImage imports a one-file image that declares a volume and names a command nothing runs.
func decoyImage(t *testing.T, docker *dockertest.Docker) string {
	t.Helper()

	return docker.Import(t, oneFileLayer(t), "decoy.invalid/label-"+random(t)+":1",
		[]string{"VOLUME /data", `CMD ["/decoy.txt"]`})
}

// configRead is the part of an inspect's Config a test reads: where containers and images keep their
// labels, and a container's environment.
type configRead struct {
	Labels map[string]string `json:"Labels"` //nolint:tagliatelle // the engine's own field name
	Env    []string          `json:"Env"`    //nolint:tagliatelle // the engine's own field name
}

// inspected is what a test reads of an inspect: labels wherever that kind keeps them.
type inspected struct {
	Labels map[string]string `json:"Labels"` //nolint:tagliatelle // the engine's own field name
	Config configRead        `json:"Config"` //nolint:tagliatelle // the engine's own field name
}

// labelsOf reads the labels an inspect of what reports.
func labelsOf(t *testing.T, what dockertest.Object, raw json.RawMessage) map[string]string {
	t.Helper()

	var read inspected
	if err := json.Unmarshal(raw, &read); err != nil {
		t.Fatalf("%s inspect: %v", what, err)
	}

	if what == dockertest.ObjectContainer || what == dockertest.ObjectImage {
		return read.Config.Labels
	}

	return read.Labels
}

func TestEveryTestResourceCarriesTheTestLabel(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	docker := engine.Docker(t)
	key, value := engine.TestLabel()

	image := decoyImage(t, docker)
	made := map[dockertest.Object]string{
		dockertest.ObjectImage:     image,
		dockertest.ObjectContainer: docker.Create(t, dockertest.CreateSpec{Image: image}),
		dockertest.ObjectNetwork:   docker.CreateNetwork(t, "stutter-decoy-"+random(t), netip.Prefix{}, nil),
		dockertest.ObjectVolume:    docker.CreateVolume(t, "", nil),
	}

	for what, id := range made {
		if got := labelsOf(t, what, docker.Inspect(t, what, id))[key]; got != value {
			t.Errorf("%s %s: label %s=%q, want %q", what, id, key, got, value)
		}
	}

	// An image's labels reach its containers, so an unlabelled container needs an image with none.
	bare := docker.Create(t, dockertest.CreateSpec{Image: unlabelledImage, Unlabelled: true, Cmd: []string{"true"}})

	read := labelsOf(t, dockertest.ObjectContainer, docker.Inspect(t, dockertest.ObjectContainer, bare))
	if got, ok := read[key]; ok {
		t.Errorf("an unlabelled container carries %s=%q", key, got)
	}
}

func TestAStartedContainerPublishesOnLoopback(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	docker := engine.Docker(t)

	// The database exits at once without its password, so a published port proves the environment
	// arrived; the value's spaces and "=" must arrive as written.
	env := map[string]string{"POSTGRES_PASSWORD": "pw-" + random(t), "DECOY_VALUE": " a=b "}
	id := docker.Create(t, dockertest.CreateSpec{Image: unlabelledImage, Env: env, Publish: []uint16{5432}})

	var read inspected
	if err := json.Unmarshal(docker.Inspect(t, dockertest.ObjectContainer, id), &read); err != nil {
		t.Fatalf("container inspect: %v", err)
	}

	for key, value := range env {
		if !slices.Contains(read.Config.Env, key+"="+value) {
			t.Errorf("the container's environment lacks %s exactly as written", key)
		}
	}

	docker.CopyIn(t, id, "/tmp", oneFileLayer(t))
	docker.Start(t, id)

	if addr := docker.Port(t, id, 5432); !addr.Addr().IsLoopback() || addr.Port() == 0 {
		t.Errorf("port 5432 published on %s; want a loopback address and a port", addr)
	}

	docker.Kill(t, id)

	if code := docker.Wait(t, id); code != 137 {
		t.Errorf("a killed container exited %d; want 137", code)
	}
}

func TestAnExtraTagKeepsTheImageID(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	docker := engine.Docker(t)

	base := "decoy.invalid/base-" + random(t) + ":1"
	extra := "decoy.invalid/extra-" + random(t) + ":1"
	image := docker.Import(t, oneFileLayer(t), base, nil)

	docker.AddTag(t, image, extra)

	tags := docker.ImageTags(t)
	if tags[base] != image || tags[extra] != image {
		t.Fatalf("after the extra tag: %s -> %q and %s -> %q; want both %s", base, tags[base], extra, tags[extra],
			image)
	}
}

//nolint:tparallel // the subtest must END, running its cleanups, before this test reads the engine.
func TestCleanupRemovesByExactID(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	docker := engine.Docker(t)
	key, value := engine.TestLabel()
	image := decoyImage(t, docker)

	var made map[dockertest.Object]string

	t.Run("create", func(t *testing.T) {
		inner := engine.Docker(t)
		made = map[dockertest.Object]string{
			dockertest.ObjectContainer: inner.Create(t, dockertest.CreateSpec{Image: image}),
			dockertest.ObjectNetwork:   inner.CreateNetwork(t, "stutter-decoy-"+random(t), netip.Prefix{}, nil),
			dockertest.ObjectVolume:    inner.CreateVolume(t, "", nil),
		}
	})

	for what, id := range made {
		if raw := docker.Inspect(t, what, id); raw != nil {
			t.Errorf("%s %s survived its creator's cleanup", what, id)
		}
	}

	listing := docker.Listing(t, key+"="+value)
	listed := map[dockertest.Object][]string{
		dockertest.ObjectContainer: listing.Containers,
		dockertest.ObjectNetwork:   listing.Networks,
		dockertest.ObjectVolume:    listing.Volumes,
	}

	for what, id := range made {
		if slices.Contains(listed[what], id) {
			t.Errorf("the label listing still holds %s %s", what, id)
		}
	}
}
