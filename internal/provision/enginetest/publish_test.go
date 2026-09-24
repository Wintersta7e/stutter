//go:build linux

package enginetest_test

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

const (
	// databasePort is the container port the published database listens on.
	databasePort = 5432
	// databaseReady bounds how long the database takes to answer: initdb runs before it listens.
	databaseReady = 60 * time.Second
	// markerFile is what the test copies in before the start, and reads back after it.
	markerFile = "stutter-marker"
	markerDir  = "/tmp"
)

// TestAStartRefusedForItsHostPortRunsOnAnother: another container takes the selected port after the
// create, so the engine refuses the start. The driver replaces the container, with the file copied into
// the first, and it serves on the port read after Start — not the one selected at create.
func TestAStartRefusedForItsHostPortRunsOnAnother(t *testing.T) {
	t.Parallel()

	docker := requireEngine(t).Docker(t)
	engine := openEngine(t, provision.Options{})

	spec := targetSpec(pinImage(t, engine, testImage), publishedNetwork(t, engine))
	spec.Spec.Env = map[string]string{"POSTGRES_PASSWORD": randomHex(8)}
	spec.Publish = []uint16{databasePort}

	container := createContainer(t, engine, spec)
	refused := container.ID()
	selected := boundHostPort(t, docker, refused)

	// Another container takes the selected port after the create, as a concurrent check's could. The
	// engine refuses that start on any engine; a host listener is refused only once WSL's relay has
	// noticed it — measured, a listener about 100 ms old let 6 of 8 starts through.
	occupier := docker.Create(t, dockertest.CreateSpec{
		Image: testImage, Env: spec.Spec.Env, PublishOn: map[uint16]uint16{databasePort: selected},
	})
	docker.Start(t, occupier)

	marker := randomHex(8)

	files := []provision.File{{Path: markerFile, Data: []byte(marker), Mode: 0o644}}
	if err := engine.CopyIn(t.Context(), container, markerDir, files); err != nil {
		t.Fatalf("CopyIn: %v", err)
	}

	startContainer(t, engine, container)

	if container.ID() == refused || docker.Inspect(t, dockertest.ObjectContainer, refused) != nil {
		t.Fatalf("the refused container %s is still in use or still on the engine", refused)
	}

	published, err := engine.Published(t.Context(), container, databasePort)
	if err != nil || published.Port() == selected {
		t.Fatalf("Published after Start = %s, %v; want a host port other than the refused %d", published, err,
			selected)
	}

	ready, cancel := context.WithTimeout(t.Context(), databaseReady)
	defer cancel()

	if answer, err := pg.Handshake(ready, published.String()); err != nil || answer != pg.AnswerPostgres {
		t.Errorf("the database at %s answered %q, %v; want Postgres", published, answer, err)
	}

	if got := copiedBack(t, engine, container); got != marker {
		t.Errorf("the replacement holds %q at %s/%s, want %q copied into the refused one", got, markerDir,
			markerFile, marker)
	}

	t.Logf("refused host port %d, served on %s", selected, published)
}

// publishedNetwork is a network a published port is reachable through: not internal.
func publishedNetwork(t *testing.T, engine *provision.Engine) *provision.Network {
	t.Helper()

	for range subnetAttempts {
		subnet, err := freeSubnet(t.Context(), engine)
		if err != nil {
			t.Fatalf("pick a subnet: %v", err)
		}

		network, err := engine.CreateNetwork(t.Context(), "published", false, subnet)
		if !errors.Is(err, provision.ErrSubnetTaken) {
			if err != nil {
				t.Fatalf("CreateNetwork: %v", err)
			}

			return network
		}
	}

	t.Fatal("every subnet tried was taken")

	return nil
}

// boundHostPort is the host port a created container's database port is bound to.
func boundHostPort(t *testing.T, docker *dockertest.Docker, id string) uint16 {
	t.Helper()

	bindings, ok := field(inspected(t, docker.Inspect(t, dockertest.ObjectContainer, id)), "HostConfig",
		"PortBindings", strconv.Itoa(databasePort)+"/tcp").([]any)
	if !ok || len(bindings) != 1 {
		t.Fatalf("container %s binds %v, want one host port", id, bindings)
	}

	text, isText := field(bindings[0], "HostPort").(string)

	port, err := strconv.ParseUint(text, 10, 16)
	if !isText || err != nil || port == 0 {
		t.Fatalf("container %s binds host port %q, not one Stutter selected", id, text)
	}

	return uint16(port)
}

// copiedBack reads the marker file back out of the running container.
func copiedBack(t *testing.T, engine *provision.Engine, container *provision.Container) string {
	t.Helper()

	out, err := engine.CopyOut(t.Context(), container, markerDir+"/"+markerFile)
	if err != nil {
		t.Fatalf("CopyOut: %v", err)
	}

	defer func() { _ = out.Close() }()

	archive := tar.NewReader(out)
	if _, nextErr := archive.Next(); nextErr != nil {
		t.Fatalf("read the copied archive: %v", nextErr)
	}

	data, err := io.ReadAll(archive)
	if err != nil {
		t.Fatalf("read the copied file: %v", err)
	}

	return string(data)
}
