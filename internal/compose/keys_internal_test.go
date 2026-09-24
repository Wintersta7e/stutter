package compose

import (
	"encoding/json"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// schemaPaths derives key paths from a compose JSON schema the way the verdict table spells them:
// dotted, list elements sharing their parent's path, `*` for a user-named map key.
type schemaPaths struct {
	defs  map[string]any
	paths map[string]bool
	stop  string
}

func (s *schemaPaths) resolve(t *testing.T, node map[string]any) map[string]any {
	t.Helper()

	for {
		ref, ok := node["$ref"].(string)
		if !ok {
			return node
		}

		next, ok := s.defs[path.Base(ref)].(map[string]any)
		if !ok {
			t.Fatalf("unresolvable $ref %q", ref)
		}

		node = next
	}
}

func (s *schemaPaths) walk(t *testing.T, node any, at string) {
	t.Helper()

	object, ok := node.(map[string]any)
	if !ok {
		return
	}

	object = s.resolve(t, object)

	for _, key := range []string{"oneOf", "anyOf", "allOf"} {
		if list, ok := object[key].([]any); ok {
			for _, sub := range list {
				s.walk(t, sub, at)
			}
		}
	}

	switch items := object["items"].(type) {
	case map[string]any:
		s.walk(t, items, at)
	case []any:
		for _, sub := range items {
			s.walk(t, sub, at)
		}
	default:
	}

	if properties, ok := object["properties"].(map[string]any); ok {
		for name, sub := range properties {
			s.add(t, joinPath(at, name), sub)
		}
	}

	if patterns, ok := object["patternProperties"].(map[string]any); ok {
		for pattern, sub := range patterns {
			if !strings.HasPrefix(pattern, "^x-") {
				s.add(t, joinPath(at, "*"), sub)
			}
		}
	}

	if extra, ok := object["additionalProperties"].(map[string]any); ok {
		s.add(t, joinPath(at, "*"), extra)
	}
}

func (s *schemaPaths) add(t *testing.T, at string, sub any) {
	t.Helper()

	s.paths[at] = true
	if at != s.stop {
		s.walk(t, sub, at)
	}
}

// deriveKeys returns a schema's paths by table: service paths and top-level paths.
func deriveKeys(t *testing.T, file string) map[string]map[string]bool {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "schema", file))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}

	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode schema %s: %v", file, err)
	}

	defs, ok := root["$defs"].(map[string]any)
	if !ok {
		defs, ok = root["definitions"].(map[string]any)
	}

	if !ok {
		t.Fatalf("schema %s has no definitions", file)
	}

	services := &schemaPaths{defs: defs, paths: map[string]bool{}}
	services.walk(t, defs["service"], "")

	top := &schemaPaths{defs: defs, paths: map[string]bool{}, stop: "services"}
	top.walk(t, root, "")

	return map[string]map[string]bool{serviceTable: services.paths, projectTable: top.paths}
}

const (
	serviceTable = "service"
	projectTable = "top-level"
)

func TestEveryComposeKeyHasOneVerdict(t *testing.T) {
	t.Parallel()

	tables := []struct {
		name  string
		rules []keyRule
	}{
		{name: serviceTable, rules: serviceKeys},
		{name: projectTable, rules: projectKeys},
	}
	seen := map[string]map[string]bool{serviceTable: {}, projectTable: {}}

	for _, schema := range []struct{ release, file string }{
		{release: "2.29.7", file: "compose-2.29.7.json"},
		{release: "5.5.1", file: "compose-5.5.1.json"},
	} {
		derived := deriveKeys(t, schema.file)
		classified, total := 0, 0

		for _, table := range tables {
			index := newKeyTable(table.rules)

			for _, key := range slices.Sorted(maps.Keys(derived[table.name])) {
				total++
				seen[table.name][key] = true

				if index.classifies(key) {
					classified++
				} else {
					t.Errorf("%s key %s has no verdict (compose %s)", table.name, key, schema.release)
				}
			}
		}

		t.Logf("keys classified: %d/%d (compose %s)", classified, total, schema.release)

		if total == 0 {
			t.Errorf("compose %s: the schema yielded no keys", schema.release)
		}
	}

	for _, table := range tables {
		rows := map[string]bool{}

		for _, rule := range table.rules {
			if rows[rule.path] {
				t.Errorf("%s table lists %s twice", table.name, rule.path)
			}

			rows[rule.path] = true

			if !seen[table.name][rule.path] {
				t.Errorf("%s table lists %s, which neither schema has", table.name, rule.path)
			}

			if rule.verdict == 0 || (rule.verdict == Refuse && rule.class == 0) {
				t.Errorf("%s table row %s has no verdict or no class", table.name, rule.path)
			}
		}

		t.Logf("%s rows: %d", table.name, len(rows))
	}
}
