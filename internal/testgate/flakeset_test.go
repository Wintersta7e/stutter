package testgate_test

import (
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// databaseVariable is spelled in two halves so this file never holds the name it stands for: a test
// file holding it is exactly what makes a package a database package.
const databaseVariable = "STUTTER_TEST_" + "POSTGRES"

const dockertestPath = "github.com/Wintersta7e/stutter/internal/dockertest"

// goList is `go list -test -json` for: a plain package; a database reader; a package whose test
// binary reaches the Docker helper through a helper package; a package without tests; the Docker
// helper itself.
const goList = `{
	"ImportPath": "example.test/plain",
	"Dir": "/src/plain",
	"TestGoFiles": ["plain_test.go"]
}
{
	"ImportPath": "example.test/plain [example.test/plain.test]",
	"Dir": "/src/plain",
	"ForTest": "example.test/plain",
	"TestGoFiles": ["plain_test.go"]
}
{
	"ImportPath": "example.test/plain.test",
	"Dir": "/src/plain",
	"Deps": ["example.test/plain [example.test/plain.test]", "testing"]
}
{
	"ImportPath": "example.test/db",
	"Dir": "/src/db",
	"XTestGoFiles": ["db_test.go"]
}
{
	"ImportPath": "example.test/db.test",
	"Dir": "/src/db",
	"Deps": ["testing"]
}
{
	"ImportPath": "example.test/viahelper",
	"Dir": "/src/viahelper",
	"XTestGoFiles": ["v_test.go"]
}
{
	"ImportPath": "example.test/viahelper.test",
	"Dir": "/src/viahelper",
	"Deps": ["example.test/helper", "` + dockertestPath + ` [example.test/viahelper.test]", "testing"]
}
{
	"ImportPath": "example.test/notests",
	"Dir": "/src/notests",
	"GoFiles": ["n.go"]
}
{
	"ImportPath": "` + dockertestPath + `",
	"Dir": "/src/dockertest",
	"TestGoFiles": ["require_internal_test.go"],
	"XTestGoFiles": ["engine_test.go"]
}
{
	"ImportPath": "` + dockertestPath + `.test",
	"Dir": "/src/dockertest",
	"Deps": ["testing"]
}
`

func testFiles(path string) ([]byte, error) {
	switch filepath.ToSlash(path) {
	case "/src/db/db_test.go":
		return []byte(`var dsn = os.Getenv("` + databaseVariable + `")`), nil
	case "/src/plain/plain_test.go", "/src/viahelper/v_test.go", "/src/dockertest/require_internal_test.go",
		"/src/dockertest/engine_test.go":
		return []byte("package x\n"), nil
	default:
		return nil, fs.ErrNotExist
	}
}

func TestTheFlakeSetIsDerived(t *testing.T) {
	t.Parallel()

	flake, err := testgate.FlakeSet(strings.NewReader(goList), testFiles)
	if err != nil {
		t.Fatalf("FlakeSet: %v", err)
	}

	checks := []struct {
		name string
		got  []string
		want []string
	}{
		{
			name: "with tests", got: flake.WithTests,
			want: []string{"example.test/db", "example.test/plain", "example.test/viahelper", dockertestPath},
		},
		{name: "database", got: flake.Postgres, want: []string{"example.test/db"}},
		{name: "docker", got: flake.Docker, want: []string{"example.test/viahelper", dockertestPath}},
		{name: "derived", got: flake.Derived, want: []string{"example.test/plain"}},
	}

	for _, c := range checks {
		if !slices.Equal(c.got, c.want) {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
}

func TestAnEmptyFlakeSetFails(t *testing.T) {
	t.Parallel()

	only := `{"ImportPath": "example.test/db", "Dir": "/src/db", "XTestGoFiles": ["db_test.go"]}`
	if flake, err := testgate.FlakeSet(strings.NewReader(only), testFiles); err == nil {
		t.Fatalf("FlakeSet = %+v; an empty derived set must fail", flake)
	}

	if _, err := testgate.FlakeSet(strings.NewReader("{not json"), testFiles); err == nil {
		t.Fatal("a malformed package list must fail")
	}

	missing := `{"ImportPath": "example.test/gone", "Dir": "/src/gone", "TestGoFiles": ["g_test.go"]}`
	if _, err := testgate.FlakeSet(strings.NewReader(missing), testFiles); err == nil {
		t.Fatal("an unreadable test file must fail, never classify")
	}
}
