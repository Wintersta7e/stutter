package dockertest

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// fatalTB is a real test whose Fatalf panics instead of ending the goroutine, so a test can assert
// that a helper refused.
type fatalTB struct {
	testing.TB

	message string
}

func (f *fatalTB) Fatalf(format string, args ...any) {
	f.message = fmt.Sprintf(format, args...)

	panic(stopped{outcome: outcomeFatal})
}

// refused runs fn and returns the fatal message it ended with, or "" when it returned normally.
func refused(t *testing.T, fn func(f *fatalTB)) string {
	t.Helper()

	f := &fatalTB{TB: t}

	func() {
		defer func() {
			if v := recover(); v != nil {
				if _, ok := v.(stopped); !ok {
					panic(v)
				}
			}
		}()

		fn(f)
	}()

	return f.message
}

func randomID(t *testing.T, n int) string {
	t.Helper()

	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

func TestTheDecoyHelperRemovesOnlyWhatItCreated(t *testing.T) {
	t.Parallel()

	engine := Require(t)
	first, second := engine.Docker(t), engine.Docker(t)

	stranger := randomID(t, 32)
	message := refused(t, func(f *fatalTB) { first.Remove(f, stranger) })

	if message == "" || !strings.Contains(message, stranger) {
		t.Fatalf("Remove of an ID no helper created returned %q; want a refusal naming it", message)
	}

	if calls := first.calls.Load(); calls != 0 {
		t.Fatalf("the refused removal made %d docker calls; want 0", calls)
	}

	volume := first.CreateVolume(t, "", nil)
	second.Remove(t, volume)

	if raw := first.Inspect(t, ObjectVolume, volume); raw != nil {
		t.Fatalf("volume %s survived its removal by a second helper", volume)
	}
}

func TestADecoyNameThatExistsRefusesTheTest(t *testing.T) {
	t.Parallel()

	engine := Require(t)
	docker := engine.Docker(t)
	name := "stutter-decoy-" + randomID(t, 8)

	docker.CreateVolume(t, name, nil)

	if message := refused(t, func(f *fatalTB) { docker.CreateVolume(f, name, nil) }); !strings.Contains(
		message, name) {
		t.Fatalf("a second volume named %s was not refused: %q", name, message)
	}

	before := docker.calls.Load()

	for _, reserved := range []string{"stutter-test-pg", "stutter-target-db"} {
		message := refused(t, func(f *fatalTB) {
			docker.Create(f, CreateSpec{Name: reserved, Image: "decoy.invalid/never:1"})
		})
		if !strings.Contains(message, reserved) {
			t.Fatalf("a container named %s was not refused: %q", reserved, message)
		}
	}

	if after := docker.calls.Load(); after != before {
		t.Fatalf("refusing the reserved names made %d docker calls; want 0", after-before)
	}
}

// checkContainer creates, outside the helper's registry, a container labelled as a Stutter check's:
// the stand-in for one the product created. It is removed by its exact ID when t ends.
func checkContainer(t *testing.T, docker *Docker, checkID string) string {
	t.Helper()

	key, value := docker.engine.TestLabel()
	id := docker.must(t, call{verb: verbCreate, args: []string{
		"--label", rules.LabelCheck + "=" + checkID, "--label", key + "=" + value, "postgres:18-alpine", "true",
	}})

	t.Cleanup(func() {
		if a := docker.run(t, call{verb: verbRemove, args: []string{id}}); a.exit != 0 {
			t.Errorf("removing %s: %s", id, a.stderr)
		}
	})

	return id
}

func TestKillRefusesAContainerOfAnUnregisteredCheck(t *testing.T) {
	t.Parallel()

	engine := Require(t)
	docker := engine.Docker(t)
	checkID := randomID(t, 16)
	id := checkContainer(t, docker, checkID)

	docker.OwnCheck(t, randomID(t, 16))

	before := docker.calls.Load()

	message := refused(t, func(f *fatalTB) { docker.Kill(f, id) })
	if !strings.Contains(message, "refusing to kill") {
		t.Fatalf("a container of an unregistered check was killed: %q", message)
	}

	if calls := docker.calls.Load() - before; calls != 1 {
		t.Fatalf("the refused kill made %d docker calls; want 1, its inspect", calls)
	}

	docker.OwnCheck(t, checkID)

	if message := refused(t, func(f *fatalTB) { docker.Kill(f, id) }); message != "" {
		t.Fatalf("a container of a registered check was refused: %q", message)
	}
}

func TestAnUnlabelledContainerNeedsAnUnlabelledImage(t *testing.T) {
	t.Parallel()

	engine := Require(t)
	docker := engine.Docker(t)
	image := docker.Import(t, strings.NewReader(""), "", []string{`CMD ["/none"]`})

	message := refused(t, func(f *fatalTB) {
		docker.Create(f, CreateSpec{Image: image, Unlabelled: true})
	})
	if !strings.Contains(message, "carries") {
		t.Fatalf("an unlabelled container from a test-labelled image was not refused: %q", message)
	}
}
