package compose

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// dotEnvKey matches the key of one `.env` line. The value after it is never kept.
var dotEnvKey = regexp.MustCompile(`^(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_.-]*)[ \t]*=`)

// dotEnvKeys reads the keys the project directory's `.env` sets, and whether it has one. Each line
// is split and its value discarded as it is read. A `.env` that exists but cannot be read is an
// error, never absent.
func dotEnvKeys(dir string) ([]string, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, ".env"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, fmt.Errorf("%w: the project's .env cannot be read: %w", ErrModel, err)
	}

	var keys []string

	for line := range strings.Lines(string(data)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if match := dotEnvKey.FindStringSubmatch(line); match != nil {
			keys = append(keys, match[1])
		}
	}

	return keys, true, nil
}

// composeVars names every `COMPOSE_*` variable set in environ (`NAME=value` entries) or as a
// `.env` key: sorted, unique, names only.
func composeVars(environ, dotEnv []string) []string {
	var names []string

	for _, entry := range environ {
		if name, _, ok := strings.Cut(entry, "="); ok && strings.HasPrefix(name, "COMPOSE_") {
			names = append(names, name)
		}
	}

	for _, key := range dotEnv {
		if strings.HasPrefix(key, "COMPOSE_") {
			names = append(names, key)
		}
	}

	slices.Sort(names)

	return slices.Compact(names)
}
