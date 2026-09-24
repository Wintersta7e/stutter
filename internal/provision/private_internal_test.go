//go:build linux

package provision

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// modeInfo reports a mode other than the one on disk.
type modeInfo struct {
	fs.FileInfo

	mode fs.FileMode
}

func (m modeInfo) Mode() fs.FileMode { return m.mode }

// A check-private directory holds the CA, the bus store and every log: it is made fresh, never
// reused, and refused unless it is a local directory this user alone reaches. A directory the
// check made and then refused is removed; one that was already there is left as it was.
func TestAPrivateDirectoryWithTheWrongModeIsRefused(t *testing.T) {
	t.Parallel()

	t.Run("wrong mode", func(t *testing.T) {
		t.Parallel()

		host := defaultHostFS()
		host.lstat = func(path string) (fs.FileInfo, error) {
			info, err := os.Lstat(path)
			if err != nil {
				return nil, err
			}

			return modeInfo{FileInfo: info, mode: fs.ModeDir | 0o777}, nil
		}

		path := filepath.Join(t.TempDir(), "stutter-"+testCheckID)

		_, err := makePrivate(path, newTestLedger(t, defaultHostFS()), host)
		if !errors.Is(err, ErrPrivateDir) || !strings.Contains(err.Error(), "0777") {
			t.Errorf("makePrivate = %v, want ErrPrivateDir naming the mode", err)
		}

		if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("the refused directory %s it made remains: %v", path, err)
		}
	})

	t.Run("pre-existing", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "stutter-"+testCheckID)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}

		sentinel := filepath.Join(path, "theirs")
		if err := os.WriteFile(sentinel, nil, 0o600); err != nil {
			t.Fatal(err)
		}

		_, err := makePrivate(path, newTestLedger(t, defaultHostFS()), defaultHostFS())
		if !errors.Is(err, ErrPrivateDir) {
			t.Errorf("makePrivate over an existing directory = %v, want ErrPrivateDir", err)
		}

		if _, err := os.Lstat(sentinel); err != nil {
			t.Errorf("the existing directory was touched: %v", err)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		t.Parallel()

		target := t.TempDir()
		path := filepath.Join(t.TempDir(), "stutter-"+testCheckID)

		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}

		_, err := makePrivate(path, newTestLedger(t, defaultHostFS()), defaultHostFS())
		if !errors.Is(err, ErrPrivateDir) {
			t.Errorf("makePrivate over a symlink = %v, want ErrPrivateDir", err)
		}

		if info, err := os.Lstat(path); err != nil || info.Mode()&fs.ModeSymlink == 0 {
			t.Errorf("the symlink was touched: %v", err)
		}
	})
}

// The private directory's layout has one owner. The checkpoints sit beside the store, never inside
// it, because a restore replaces the store.
func TestTheLayoutHasOneOwner(t *testing.T) {
	t.Parallel()

	engine := openShimEngine(t, "")
	want := map[HostName]string{HostCA: "ca.pem", HostStore: "bus/store", HostB0: "bus/B0", HostB1: "bus/B1"}
	got := make(map[HostName]string, len(want))

	for name, rel := range want {
		path, err := engine.HostPath(name)
		if err != nil {
			t.Fatalf("HostPath(%s): %v", name, err)
		}

		got[name] = path

		if path != filepath.Join(engine.PrivateDir(), rel) {
			t.Errorf("HostPath(%s) = %s, want %s under the private directory", name, path, rel)
		}

		if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("HostPath(%s) created the entry itself: %v", name, err)
		}
	}

	for _, checkpoint := range []HostName{HostB0, HostB1} {
		if filepath.Dir(got[checkpoint]) != filepath.Dir(got[HostStore]) ||
			strings.HasPrefix(got[checkpoint], got[HostStore]+string(filepath.Separator)) {
			t.Errorf("checkpoint %s is not a sibling of the store %s", got[checkpoint], got[HostStore])
		}
	}

	if info, err := os.Lstat(filepath.Dir(got[HostStore])); err != nil || info.Mode().Perm() != 0o700 {
		t.Errorf("the bus directory is not 0700: %v", err)
	}

	if _, err := engine.HostPath("bus/../../etc"); !errors.Is(err, errUnknownHostName) {
		t.Errorf("an unknown host name was admitted: %v", err)
	}
}

// snakeKey is a key the invocation log may carry.
var snakeKey = regexp.MustCompile(`^[a-z]+(_[a-z]+)*$`)

// The invocation log has three kinds of line, each a JSON object with snake_case keys; a token from
// the compose model's process fields is written as a placeholder.
func TestTheInvocationLogHasThreeLineKinds(t *testing.T) {
	t.Parallel()

	const secret = "model-entrypoint-4d1f"

	engine := openShimEngine(t, "")

	if _, err := engine.run.call(t.Context(), request{
		verb: verbInfo, args: []arg{{val: secret, model: true}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := engine.LogHold(Hold{Term: "fetch", Length: 3 * time.Millisecond, Bound: time.Second}); err != nil {
		t.Fatal(err)
	}

	if _, err := engine.HostPath(HostStore); err != nil {
		t.Fatal(err)
	}

	kinds := map[string]int{}

	for _, line := range readLines(t, filepath.Join(engine.PrivateDir(), invocationLog)) {
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatalf("line %q: %v", line, err)
		}

		for key := range fields {
			if !snakeKey.MatchString(key) {
				t.Errorf("line %q: key %q is not snake_case", line, key)
			}
		}

		kind, ok := fields["kind"].(string)
		if !ok {
			t.Errorf("line %q has no kind", line)
		}

		kinds[kind]++

		if strings.Contains(line, secret) {
			t.Errorf("a model token reached the log: %s", line)
		}
	}

	t.Logf("kinds=%v", kinds)

	if kinds["call"] < 4 || kinds["hold"] != 1 || kinds["hostpath"] < 2 {
		t.Errorf("log kinds = %v, want the precondition and test calls, one hold, the bus and store paths", kinds)
	}

	if !strings.Contains(readFile(t, filepath.Join(engine.PrivateDir(), invocationLog)), `"<model>"`) {
		t.Error("the model token was not written as <model>")
	}
}

// What a call carries on stdin — a container's environment — never reaches the log.
func TestNoEnvironmentValueReachesTheLog(t *testing.T) {
	t.Parallel()

	const sentinel = "SECRET_TOKEN=5b8e0c2a-stdin-only"

	engine := openShimEngine(t, "")

	if _, err := engine.run.call(t.Context(), request{
		verb: verbInfo, args: []arg{{val: "--env-file"}, {val: "/dev/stdin"}},
		stdin: strings.NewReader(sentinel + "\n"),
	}); err != nil {
		t.Fatal(err)
	}

	log := readFile(t, filepath.Join(engine.PrivateDir(), invocationLog))
	if strings.Contains(log, "5b8e0c2a") {
		t.Errorf("the environment reached the invocation log:\n%s", log)
	}

	if !strings.Contains(log, "/dev/stdin") {
		t.Errorf("the call itself is missing from the log:\n%s", log)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}
