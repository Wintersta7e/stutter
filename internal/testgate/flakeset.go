package testgate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
)

// dockerHelper is the package every Docker test reaches the engine through: a test binary that
// links it runs Docker tests.
const dockerHelper = "github.com/Wintersta7e/stutter/internal/dockertest"

// databaseMarker is the variable a database test reads; a test file naming it makes its package a
// database package.
const databaseMarker = "STUTTER_TEST_" + "POSTGRES"

// errFlakeSet means the flake set could not be derived, or derived nothing.
var errFlakeSet = errors.New("flake set")

// Flake is the flake job's package set and how it was derived: every package with tests, less the
// ones that need a database and the ones that run Docker tests.
type Flake struct {
	WithTests []string
	Postgres  []string
	Docker    []string
	Derived   []string
}

// listedPackage is one entry of go list -test -json.
type listedPackage struct {
	ImportPath   string   `json:"ImportPath"`   //nolint:tagliatelle // the toolchain's own field name
	Dir          string   `json:"Dir"`          //nolint:tagliatelle // the toolchain's own field name
	ForTest      string   `json:"ForTest"`      //nolint:tagliatelle // the toolchain's own field name
	TestGoFiles  []string `json:"TestGoFiles"`  //nolint:tagliatelle // the toolchain's own field name
	XTestGoFiles []string `json:"XTestGoFiles"` //nolint:tagliatelle // the toolchain's own field name
	Deps         []string `json:"Deps"`         //nolint:tagliatelle // the toolchain's own field name
}

// FlakeSet derives the flake job's packages from go list -test -json: a package has tests when its
// plain entry lists test files; it needs a database when one of those files names the database
// variable; it runs Docker tests when its test binary links the Docker helper, or it is the helper.
// readFile reads a test file; one that cannot be read fails the derivation rather than guessing.
func FlakeSet(list io.Reader, readFile func(path string) ([]byte, error)) (Flake, error) {
	var flake Flake

	plain, linksHelper, err := readPackageList(list)
	if err != nil {
		return Flake{}, err
	}

	for _, p := range plain {
		if len(p.TestGoFiles)+len(p.XTestGoFiles) == 0 {
			continue
		}

		flake.WithTests = append(flake.WithTests, p.ImportPath)

		database, err := readsDatabase(p, readFile)
		if err != nil {
			return Flake{}, err
		}

		switch {
		case p.ImportPath == dockerHelper || linksHelper[p.ImportPath]:
			flake.Docker = append(flake.Docker, p.ImportPath)
		case database:
			flake.Postgres = append(flake.Postgres, p.ImportPath)
		default:
			flake.Derived = append(flake.Derived, p.ImportPath)
		}
	}

	for _, set := range []*[]string{&flake.WithTests, &flake.Postgres, &flake.Docker, &flake.Derived} {
		slices.Sort(*set)
	}

	if len(flake.Derived) == 0 {
		return flake, fmt.Errorf("%w: derived=0 of %d packages with tests", errFlakeSet, len(flake.WithTests))
	}

	return flake, nil
}

// readPackageList returns the plain package entries of go list -test -json, and for each package
// whether its test binary links the Docker helper.
func readPackageList(list io.Reader) ([]listedPackage, map[string]bool, error) {
	var plain []listedPackage

	linksHelper := map[string]bool{}
	decoder := json.NewDecoder(list)

	for decoder.More() {
		var p listedPackage
		if err := decoder.Decode(&p); err != nil {
			return nil, nil, fmt.Errorf("%w: reading go list -test -json: %w", errFlakeSet, err)
		}

		switch {
		case strings.HasSuffix(p.ImportPath, ".test"):
			linksHelper[strings.TrimSuffix(p.ImportPath, ".test")] = linksDockerHelper(p.Deps)
		case p.ForTest == "" && !strings.Contains(p.ImportPath, " ["):
			plain = append(plain, p)
		default:
		}
	}

	return plain, linksHelper, nil
}

// linksDockerHelper reports whether a test binary's dependencies include the Docker helper; a
// dependency compiled for a test carries a " [pkg.test]" suffix.
func linksDockerHelper(deps []string) bool {
	for _, dep := range deps {
		if name, _, _ := strings.Cut(dep, " "); name == dockerHelper {
			return true
		}
	}

	return false
}

// readsDatabase reports whether any of a package's test files names the database variable.
func readsDatabase(p listedPackage, readFile func(path string) ([]byte, error)) (bool, error) {
	for _, name := range slices.Concat(p.TestGoFiles, p.XTestGoFiles) {
		content, err := readFile(filepath.Join(p.Dir, name))
		if err != nil {
			return false, fmt.Errorf("%w: reading a test file of %s: %w", errFlakeSet, p.ImportPath, err)
		}

		if bytes.Contains(content, []byte(databaseMarker)) {
			return true, nil
		}
	}

	return false, nil
}
