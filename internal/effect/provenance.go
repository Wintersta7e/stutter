package effect

import (
	"cmp"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
)

// Mode records how one message's values were extracted.
type Mode string

const (
	// ModeJSON means the payload parsed as JSON and every scalar was extracted with its path.
	ModeJSON Mode = "json"
	// ModeOpaque means the payload did not parse, so only the payload as a whole can be matched.
	ModeOpaque Mode = "opaque"
	// ModeNone means there was no payload, leaving type substitution as the only normalisation.
	ModeNone Mode = "none"
)

// minProvenanceLength is the shortest message value worth substituting.
//
// Substitution is a plain string replacement, so a short value matches inside unrelated tokens: the
// value "12" would rewrite part of "1234". Four characters is a compromise, not a derived constant,
// and is the first thing to revisit if a real corpus produces spurious matches.
const minProvenanceLength = 4

// Provenance maps the scalar values carried by one inbound message to stable placeholders.
//
// It answers the question that decides whether the determinism gate can ever pass: is this
// identifier in an outbound query one the handler invented, or one that arrived in the message? The
// first is noise and must be flattened; the second is signal and must be preserved.
type Provenance struct {
	mode         Mode
	replacements []replacement
}

type replacement struct {
	value       string
	placeholder string
}

// NewProvenance extracts the substitutable values from one message payload.
//
// A JSON payload yields every scalar keyed by its path. A payload that does not parse degrades to
// matching the payload as a whole, and an empty payload to nothing at all; Mode reports which
// happened so a later gate failure can be attributed to the degradation.
func NewProvenance(payload []byte) *Provenance {
	if len(payload) == 0 {
		return &Provenance{mode: ModeNone}
	}

	var root any
	if err := json.Unmarshal(payload, &root); err != nil {
		return &Provenance{
			mode:         ModeOpaque,
			replacements: []replacement{{value: string(payload), placeholder: "<msg:payload>"}},
		}
	}

	prov := &Provenance{mode: ModeJSON}
	prov.walk("", root)
	prov.sortLongestFirst()

	return prov
}

// Mode reports how the payload was parsed.
func (p *Provenance) Mode() Mode {
	return p.mode
}

// Substitute replaces every message-derived value in text with its placeholder.
//
// Values are replaced longest first, so that a value which is a substring of another does not
// consume part of it.
func (p *Provenance) Substitute(text string) string {
	for _, r := range p.replacements {
		text = strings.ReplaceAll(text, r.value, r.placeholder)
	}

	return text
}

func (p *Provenance) walk(path string, node any) {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			p.walk(joinPath(path, key), child)
		}
	case []any:
		for index, child := range value {
			p.walk(joinPath(path, strconv.Itoa(index)), child)
		}
	case string:
		p.add(path, value)
	case float64:
		p.add(path, strconv.FormatFloat(value, 'f', -1, 64))
	default:
		// Booleans and nulls carry no identity worth tracking, and substituting them would rewrite
		// every occurrence of "true" in unrelated text.
	}
}

func (p *Provenance) add(path, value string) {
	if len(value) < minProvenanceLength {
		return
	}

	p.replacements = append(p.replacements, replacement{
		value:       value,
		placeholder: "<msg:" + path + ">",
	})
}

// sortLongestFirst orders replacements so substitution is deterministic. Map iteration order is
// random, so the tie-break on value is what stops two runs over the same payload from producing
// different output.
func (p *Provenance) sortLongestFirst() {
	slices.SortFunc(p.replacements, func(a, b replacement) int {
		if diff := cmp.Compare(len(b.value), len(a.value)); diff != 0 {
			return diff
		}

		return cmp.Compare(a.value, b.value)
	})
}

func joinPath(parent, child string) string {
	if parent == "" {
		return child
	}

	return parent + "." + child
}
