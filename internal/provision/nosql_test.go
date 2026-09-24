package provision_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestProvisioningImportsNoSQLDriver keeps provisioning from ever speaking SQL: a dependency's state is
// reset by replacing its container and copying its volumes, and an SQL reset was measured not to give
// the same bytes back.
func TestProvisioningImportsNoSQLDriver(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list the package's files: %v", err)
	}

	scanned, found := 0, 0

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}

		parsed, err := parser.ParseFile(token.NewFileSet(), file, source, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}

		scanned++

		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("%s: import %s: %v", file, spec.Path.Value, err)
			}

			if speaksSQL(path) {
				found++

				t.Errorf("%s imports %s: provisioning never issues SQL", file, path)
			}
		}
	}

	t.Logf("files scanned=%d sql imports=%d", scanned, found)

	if scanned == 0 {
		t.Fatal("files scanned=0: the scan proves nothing")
	}
}

// speaksSQL reports an import that is an SQL driver or the SQL package itself.
func speaksSQL(path string) bool {
	return path == "database/sql" || strings.HasPrefix(path, "github.com/jackc/pgx") || path == "github.com/lib/pq"
}
