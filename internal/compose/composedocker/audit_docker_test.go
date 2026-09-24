package composedocker_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// defaultsImage is imported with every default a container inherits from its image: a working
// directory, a user, an environment, a VOLUME, an exposed port, a healthcheck, a stop signal and a
// compose label, which a container the user's compose made would carry too.
func defaultsImage() []string {
	return []string{
		"WORKDIR /srv", "USER 1234:1234", "ENV FROM_IMAGE=yes", "VOLUME /data", "EXPOSE 8080",
		"HEALTHCHECK CMD true", "STOPSIGNAL SIGINT", "LABEL com.docker.compose.project=elsewhere",
		`CMD ["/absent"]`,
	}
}

// oneFileLayer is a layer holding one file: enough to import an image nothing is ever started from.
func oneFileLayer(t *testing.T) *bytes.Buffer {
	t.Helper()

	var out bytes.Buffer

	archive := tar.NewWriter(&out)

	if err := archive.WriteHeader(&tar.Header{Name: "marker", Mode: 0o644, Size: 1}); err != nil {
		t.Fatal(err)
	}

	if _, err := archive.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}

	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	return &out
}

// openAuditEngine opens a check against the real engine, closed when the test ends with nothing of it
// left behind.
func openAuditEngine(t *testing.T) *provision.Engine {
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		if down := eng.Close(ctx, provision.DiscardLogs); len(down.Listing) != 0 {
			t.Errorf("the engine still holds %d resources of check %s", len(down.Listing), eng.CheckID())
		}
	})

	return eng
}

// TestAuditPassesContainersTheEngineReallyMade creates a container from each real image through the
// driver, with nothing set in compose that the image could default, and audits what the engine holds:
// every row that inherits an image default must read the default the engine applied.
func TestAuditPassesContainersTheEngineReallyMade(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	docker := engine.Docker(t)

	imported := "example.test/audit-defaults:" + strings.ToLower(rand.Text()[:12])
	docker.Import(t, oneFileLayer(t), imported, defaultsImage())

	refs := map[string]string{
		"pg": "postgres:18-alpine", "kv": "redis:8-alpine", "bus": "nats:alpine", "defaults": imported,
	}

	var project strings.Builder

	project.WriteString("name: audit\nservices:\n")

	for service, ref := range refs {
		fmt.Fprintf(&project, "  %s:\n    image: %q\n", service, ref)
	}

	file := filepath.Join(t.TempDir(), "compose.yaml")
	if err := os.WriteFile(file, []byte(project.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	eng := openAuditEngine(t)

	model, err := compose.Parse(t.Context(), eng.ComposeConfig, compose.Inputs{Service: "pg", Files: []string{file}})
	if err != nil {
		t.Fatalf("compose.Parse() error = %v", err)
	}

	images := map[string]compose.Image{}

	for service, ref := range refs {
		if images[service], err = eng.ResolveImage(t.Context(), ref, ""); err != nil {
			t.Fatalf("ResolveImage(%s) error = %v", ref, err)
		}
	}

	network, err := eng.CreateFreeNetwork(t.Context(), "audit", false)
	if err != nil {
		t.Fatalf("CreateFreeNetwork() error = %v", err)
	}

	for service := range refs {
		auditOne(t, eng, model, network, service, images[service])
	}
}

// auditOne creates one service's container, never started, and audits what the engine holds.
func auditOne(
	t *testing.T, eng *provision.Engine, model *compose.Model, network *provision.Network, service string,
	img compose.Image,
) {
	t.Helper()

	spec, err := model.Spec(service, img, nil)
	if err != nil {
		t.Fatalf("Spec(%s) error = %v", service, err)
	}

	c, err := eng.CreateContainer(t.Context(), provision.ContainerSpec{
		Kind: rules.KindJob, Service: service, Spec: spec, Networks: []provision.NetworkAttach{{Network: network}},
	})
	if err != nil {
		t.Fatalf("create %s: %v", service, err)
	}

	got, err := eng.Inspect(t.Context(), c)
	if err != nil {
		t.Fatalf("inspect %s: %v", service, err)
	}

	checked, differ, err := compose.Audit(spec, img, got)
	t.Logf("%s: rows checked=%d differ=%d (working_dir %q, user %q)", service, checked, differ, got.WorkingDir,
		got.User)

	if err != nil || differ != 0 || checked == 0 {
		t.Errorf("Audit(%s) = %d checked, %d differ, %v; want every row to match what the engine made", service,
			checked, differ, err)
	}
}
