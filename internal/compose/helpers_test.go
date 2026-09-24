package compose_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// call is one ConfigFunc invocation the fake saw.
type call struct {
	dir  string
	args []string
	read compose.ConfigRead
}

// fakeRun stands in for `docker compose … config`. whole answers a whole-project read, given the
// profiles it was asked for; narrow and environment answer the other two reads.
type fakeRun struct {
	whole       func(profiles []string) ([]byte, int, error)
	narrow      func(service string) ([]byte, error)
	environment []byte
	calls       []call
	mu          sync.Mutex
}

func (f *fakeRun) run(_ context.Context, dir string, args []string, read compose.ConfigRead) ([]byte, int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, call{dir: dir, args: slices.Clone(args), read: read})
	f.mu.Unlock()

	switch {
	case read.Environment:
		return f.environment, 0, nil
	case read.Service != "":
		out, err := f.narrow(read.Service)

		return out, 0, err
	default:
		return f.whole(profilesIn(args))
	}
}

func (f *fakeRun) seen() []call {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.calls)
}

// profilesIn returns the `--profile` values of a compose argument list.
func profilesIn(args []string) []string {
	var out []string

	for index := 0; index+1 < len(args); index++ {
		if args[index] == "--profile" {
			out = append(out, args[index+1])
		}
	}

	return out
}

const (
	// target is the service under test in every model the tests parse.
	target = "api"
	// toolsService is a profile-gated service.
	toolsService = "tools"
	// resolvedEnvironment is compose's resolved environment for every model modelFrom parses: the
	// two variables the kitchen model's environment-sourced config and secret read.
	resolvedEnvironment = "HOME=/home/x\nCONFIG_VALUE=config-value\nSECRET_VALUE=secret-value\n"
	// cacheService and jobService are dependencies several models share.
	cacheService = "cache"
	jobService   = "migrate"
	// Values several tests share.
	privateNamespace = "private"
	pathEntry        = "PATH=/bin"
	sslCertFile      = "SSL_CERT_FILE"
	dataTarget       = "/data"
	hostnameKey      = "hostname"
	// absentService is a service no model has.
	absentService = "ghost"
	// pidKey, ipcKey and utsKey are namespace keys several tests name.
	pidKey = "pid"
	ipcKey = "ipc"
	utsKey = "uts"
)

// releases are the compose releases whose rendering of the kitchen model is committed.
var releases = []string{"2.29.7", "5.5.1"}

// project writes a compose file into a fresh project directory and returns its absolute path.
func project(t *testing.T, name string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte("services: {}\n"), 0o600); err != nil {
		t.Fatalf("write compose file: %v", err)
	}

	return path
}

// golden reads one committed rendering of the kitchen model.
func golden(t *testing.T, release string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "model", "kitchen-"+release+".json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}

	return data
}

// modelFrom parses jsonText as compose would print it, for the target service.
func modelFrom(t *testing.T, jsonText string) *compose.Model {
	t.Helper()

	run := &fakeRun{
		whole:       func([]string) ([]byte, int, error) { return []byte(jsonText), 0, nil },
		environment: []byte(resolvedEnvironment),
	}

	file := project(t, "compose.yaml")
	if strings.Contains(jsonText, `"x-stutter"`) {
		file = declaring(t, filepath.Dir(file), "compose.yaml")
	}

	model, err := compose.Parse(t.Context(), run.run, compose.Inputs{Service: target, Files: []string{file}})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	return model
}
