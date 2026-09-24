package testgate_test

import (
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/testgate"
)

// makefile is a Makefile whose build recipe is the one build of the product.
const makefile = `GO ?= go
BIN ?= bin/stutter
CGO = 0

.PHONY: test
test: ## Race tests keep cgo
	$(GO) test -race ./...

.PHONY: build
build: ## Build the CLI
	CGO_ENABLED=$(CGO) $(GO) build -trimpath -o $(BIN) ./cmd/stutter
`

func file(path, content string) testgate.SourceFile {
	return testgate.SourceFile{Path: path, Content: []byte(content)}
}

func TestOnlyTheMakefileRecipeBuildsTheProduct(t *testing.T) {
	t.Parallel()

	workflow := func(line string) testgate.SourceFile {
		return file(".github/workflows/ci.yml", "jobs:\n  smoke:\n    steps:\n      - run: "+line+"\n")
	}

	cases := []struct {
		name  string
		files []testgate.SourceFile
		found int
		pass  bool
	}{
		{name: "the recipe alone", files: []testgate.SourceFile{file("Makefile", makefile)}, found: 1, pass: true},
		{
			name: "a compile check writes nothing",
			files: []testgate.SourceFile{
				file("Makefile", makefile), workflow("go build -trimpath -o /dev/null ./..."),
				workflow("go build -o=/dev/null ./..."),
			},
			found: 1, pass: true,
		},
		{
			name:  "an external tool is not the product",
			files: []testgate.SourceFile{file("Makefile", makefile), workflow(`go install "x/y/cmd/z@v1.2.3"`)},
			found: 1, pass: true,
		},
		{
			name: "a second build in a workflow",
			files: []testgate.SourceFile{
				file("Makefile", makefile),
				workflow("go build -trimpath -o stutter ./cmd/stutter"),
			},
			found: 2,
		},
		{
			name: "an install of the product",
			files: []testgate.SourceFile{
				file("Makefile", makefile),
				file("scripts/release.sh", "go install ./cmd/stutter\n"),
			},
			found: 2,
		},
		{
			name: "a commented build is ignored",
			files: []testgate.SourceFile{
				file("Makefile", makefile+"# go build -o x ./cmd/stutter\n"),
				file("main.go", "package main\n\n// go build -o x ./cmd/stutter\n"),
			},
			found: 1, pass: true,
		},
		{
			name: "test files and binary files are not scanned",
			files: []testgate.SourceFile{
				file("Makefile", makefile), file("x_test.go", `const c = "go build -o x ."`),
				file("blob.bin", "\x00go build -o x .\n"),
			},
			found: 1, pass: true,
		},
		{name: "zero files", files: nil, found: 0},
		{
			name:  "the one build outside the Makefile",
			files: []testgate.SourceFile{workflow("go build -trimpath -o stutter ./cmd/stutter")},
			found: 1,
		},
		{
			name: "the one build in another Makefile target",
			files: []testgate.SourceFile{
				file("Makefile", "release:\n\t$(GO) build -o dist/stutter ./cmd/stutter\n"),
			},
			found: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scan := testgate.ScanBuilds(tc.files)
			if len(scan.Found) != tc.found {
				t.Errorf("found %d builds (%v), want %d", len(scan.Found), scan.Found, tc.found)
			}

			if err := scan.Err(); (err == nil) != tc.pass {
				t.Errorf("Err() = %v, want pass=%v", err, tc.pass)
			}
		})
	}
}

func TestAScannedFileIsCountedOnce(t *testing.T) {
	t.Parallel()

	scan := testgate.ScanBuilds([]testgate.SourceFile{
		file("Makefile", makefile), file("README.md", "text\n"), file("x_test.go", "package x\n"),
	})
	if scan.Files != 2 {
		t.Fatalf("files=%d, want 2: test files are not scanned", scan.Files)
	}

	match := scan.Found[0]
	if match.Path != "Makefile" || match.Line != 11 || !strings.Contains(match.Text, "-trimpath") {
		t.Fatalf("match = %+v, want Makefile:11 and the recipe's text", match)
	}
}

func TestAnExportedCgoSettingFailsTheScan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		files []testgate.SourceFile
		cgo   int
	}{
		{name: "the recipe line alone", files: []testgate.SourceFile{file("Makefile", makefile)}},
		{
			name:  "an export at column 0",
			files: []testgate.SourceFile{file("Makefile", "export CGO_ENABLED := 0\n"+makefile)},
			cgo:   1,
		},
		{
			name:  "an assignment at column 0",
			files: []testgate.SourceFile{file("Makefile", "CGO_ENABLED = 0\n"+makefile)},
			cgo:   1,
		},
		{
			name: "a workflow setting",
			files: []testgate.SourceFile{
				file("Makefile", makefile),
				file(".github/workflows/ci.yml", "    env:\n      CGO_ENABLED: \"0\"\n"),
			},
			cgo: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scan := testgate.ScanBuilds(tc.files)
			if len(scan.CgoAssignments) != tc.cgo {
				t.Errorf("cgo-assignments=%d (%v), want %d", len(scan.CgoAssignments), scan.CgoAssignments, tc.cgo)
			}

			if err := scan.Err(); (err == nil) != (tc.cgo == 0) {
				t.Errorf("Err() = %v, want pass=%v", err, tc.cgo == 0)
			}
		})
	}
}

func TestTheMakefileDefaultIsCGOZero(t *testing.T) {
	t.Parallel()

	withDefault := func(assignment string) string {
		return strings.Replace(makefile, "CGO = 0\n", assignment, 1)
	}

	cases := []struct {
		name       string
		cgoDefault string
		files      []testgate.SourceFile
		pass       bool
	}{
		{name: "a plain zero", files: []testgate.SourceFile{file("Makefile", makefile)}, cgoDefault: "0", pass: true},
		{
			name:       "a conditional default yields to the environment",
			files:      []testgate.SourceFile{file("Makefile", withDefault("CGO ?= 0\n"))},
			cgoDefault: "0",
		},
		{
			name:       "a default of one",
			files:      []testgate.SourceFile{file("Makefile", withDefault("CGO = 1\n"))},
			cgoDefault: "1",
		},
		{
			name:       "an exported default",
			files:      []testgate.SourceFile{file("Makefile", withDefault("export CGO = 0\n"))},
			cgoDefault: "0",
		},
		{name: "no default at all", files: []testgate.SourceFile{file("Makefile", withDefault(""))}},
		{
			name: "a workflow passing CGO=1 to make",
			files: []testgate.SourceFile{
				file("Makefile", makefile), file(".github/workflows/ci.yml", "      - run: make build CGO=1 BIN=x\n"),
			},
			cgoDefault: "0",
		},
		{
			name: "make told to let the environment win",
			files: []testgate.SourceFile{
				file("Makefile", makefile), file("scripts/release.sh", "make -e build\n"),
			},
			cgoDefault: "0",
		},
		{
			name: "a shell CGO in the environment is harmless",
			files: []testgate.SourceFile{
				file("Makefile", makefile), file(".github/workflows/ci.yml", "      - run: CGO=1 make build BIN=x\n"),
			},
			cgoDefault: "0", pass: true,
		},
		{
			name: "a test passing CGO=1 is not scanned",
			files: []testgate.SourceFile{
				file("Makefile", makefile), file("build_test.go", `var argv = []string{"make", "build", "CGO=1"}`),
			},
			cgoDefault: "0", pass: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scan := testgate.ScanBuilds(tc.files)
			if scan.CgoDefault != tc.cgoDefault {
				t.Errorf("cgo-default=%q, want %q", scan.CgoDefault, tc.cgoDefault)
			}

			if err := scan.Err(); (err == nil) != tc.pass {
				t.Errorf("Err() = %v, want pass=%v (cgo settings %v)", err, tc.pass, scan.CgoAssignments)
			}
		})
	}
}
