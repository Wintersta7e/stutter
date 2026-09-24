package compose

import (
	"maps"
	"slices"
	"strings"
)

// walkModel refuses any key the verdict tables do not list, anywhere in the parsed model: a key
// compose adds in a later release is never silently honoured. An `x-` key at a struct level is an
// extension and is skipped; the same name at a user-named map level is a name like any other.
func walkModel(root map[string]any) error {
	rest := maps.Clone(root)
	delete(rest, "services")

	if err := (walker{table: newKeyTable(projectKeys)}).walkMap(rest, "", ""); err != nil {
		return err
	}

	services, ok := root["services"].(map[string]any)
	if !ok {
		return nil
	}

	table := newKeyTable(serviceKeys)

	for _, name := range slices.Sorted(maps.Keys(services)) {
		body, ok := services[name].(map[string]any)
		if !ok {
			continue
		}

		if err := (walker{table: table, service: name}).walkMap(body, "", ""); err != nil {
			return err
		}
	}

	return nil
}

// walker walks one verdict table over one part of the model.
type walker struct {
	table   keyTable
	service string
}

// walk classifies value found at rule path rule; shown is the same path with the user's own names,
// which is what a refusal names.
func (w walker) walk(value any, rule, shown string) error {
	if row, ok := w.table.rules[rule]; ok && row.subtree {
		return nil
	}

	switch typed := value.(type) {
	case map[string]any:
		return w.walkMap(typed, rule, shown)
	case []any:
		for _, element := range typed {
			if err := w.walk(element, rule, shown); err != nil {
				return err
			}
		}

		return nil
	case nil:
		return nil
	default:
		if _, ok := w.table.rules[rule]; !ok {
			return &Refusal{Service: w.service, Key: shown, Class: K14}
		}

		return nil
	}
}

func (w walker) walkMap(body map[string]any, rule, shown string) error {
	for _, key := range slices.Sorted(maps.Keys(body)) {
		child, known := w.table.resolve(rule, key)
		if !known {
			return &Refusal{Service: w.service, Key: joinPath(shown, key), Class: K14}
		}

		if child == "" {
			continue
		}

		if err := w.walk(body[key], child, joinPath(shown, key)); err != nil {
			return err
		}
	}

	return nil
}

// resolve names the rule path of key beneath parent: its own row, else the parent's user-named map
// level. An `x-` key at a struct level resolves to "" and known, meaning skip it.
func (k keyTable) resolve(parent, key string) (string, bool) {
	if own := joinPath(parent, key); k.known(own) {
		return own, true
	}

	if named := joinPath(parent, "*"); k.known(named) {
		return named, true
	}

	if strings.HasPrefix(key, "x-") {
		return "", true
	}

	return "", false
}

func joinPath(parent, key string) string {
	if parent == "" {
		return key
	}

	return parent + "." + key
}
