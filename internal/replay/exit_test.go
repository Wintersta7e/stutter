package replay_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/replay"
)

// engineTimestamps are the wall-clock strings the engine reports a container's lifetime in. A duration
// taken from them lies on a host whose wall clock steps, so no production code may name them.
var engineTimestamps = []string{"StartedAt", "FinishedAt"}

// TestNoDurationIsReadFromEngineTimestamps: every duration is taken on the monotonic clock. The engine's
// start and finish times are wall-clock strings, and a wall clock that steps forward mid-run would move
// a timing by the size of the step — so no production Go file names them, and an exit carries no time
// at all.
func TestNoDurationIsReadFromEngineTimestamps(t *testing.T) {
	t.Parallel()

	scanned := 0

	for _, root := range []string{"../../internal", "../../cmd"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}

			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			scanned++

			for _, found := range timestampNames(t, path) {
				t.Errorf("%s names %s: a duration read from the engine's wall clock", path, found)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	t.Logf("scanned %d files", scanned)

	if scanned == 0 {
		t.Fatal("scanned no Go files, so the scan proves nothing")
	}

	exit := reflect.TypeFor[replay.Exit]()
	for field := range exit.Fields() {
		if field.Type == reflect.TypeFor[time.Time]() || field.Type == reflect.TypeFor[time.Duration]() {
			t.Errorf("replay.Exit.%s is a %s: an exit carries no time", field.Name, field.Type)
		}
	}
}

// timestampNames reports every identifier and string literal in a Go file that names an engine
// timestamp. Comments are not read: a comment explaining why they are never decoded is fine.
func timestampNames(t *testing.T, path string) []string {
	t.Helper()

	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var found []string

	ast.Inspect(parsed, func(node ast.Node) bool {
		var text string

		switch typed := node.(type) {
		case *ast.Ident:
			text = typed.Name
		case *ast.BasicLit:
			if typed.Kind == token.STRING {
				text = typed.Value
			}
		default:
			// Nothing else in a file can carry a name.
		}

		for _, name := range engineTimestamps {
			if strings.Contains(text, name) {
				found = append(found, name)
			}
		}

		return true
	})

	return found
}
