package testgate

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// AuditPrefix begins every audit line.
const AuditPrefix = "STUTTER-AUDIT"

// AuditKind is the kind of an audit line; it fixes the line's keys and their order.
type AuditKind string

// The audit line kinds.
const (
	AuditRecorder AuditKind = "recorder"
	AuditTags     AuditKind = "tags"
	AuditResidue  AuditKind = "residue"
	AuditDecoys   AuditKind = "decoys"
	AuditDSNDecoy AuditKind = "dsn-decoy"
	AuditSecrets  AuditKind = "secrets"
	AuditTree     AuditKind = "tree"
	AuditStatic   AuditKind = "static"
)

// checkKey names the check an audit line is about; the tree and static lines are about no check.
const checkKey = "check"

// minAuditFields is the prefix and the kind: the least an audit line holds.
const minAuditFields = 2

// auditLinePattern finds an audit line in a test's output: at the start of the line, or right
// after the file:line prefix t.Logf adds, never quoted inside another message.
var auditLinePattern = regexp.MustCompile(`^(?:\s*[\w./-]+\.go:\d+: )?(` + AuditPrefix + `\s.*?)\s*$`)

// errAudit means an audit line broke the grammar.
var errAudit = errors.New("audit line")

// AuditKeys returns the count keys of kind, in their fixed order; nil for an unknown kind.
func AuditKeys(kind AuditKind) []string {
	return auditGrammar()[kind]
}

// auditGrammar is every kind's count keys, in order. It is the one statement of the grammar.
func auditGrammar() map[AuditKind][]string {
	return map[AuditKind][]string{
		AuditRecorder: {"calls", "mutating", "prune", "foreign-touched"},
		AuditTags:     {"preexisting", "moved"},
		AuditResidue:  {"created", "remaining"},
		AuditDecoys:   {"planted", "survived"},
		AuditDSNDecoy: {"connections"},
		AuditSecrets:  {"sentinel-hits", "scanned-files", "scanned-argv"},
		AuditTree:     {"untracked-or-modified"},
		AuditStatic: {
			"files-go", "files-workflow", "files-make", "files-script", "spawners", "verbs", "compose-sites",
			"mutators",
		},
	}
}

// aboutACheck reports whether kind's lines name a check.
func aboutACheck(kind AuditKind) bool {
	return kind != AuditTree && kind != AuditStatic
}

// Audit is one parsed audit line.
type Audit struct {
	Counts map[string]int
	Kind   AuditKind
	Check  string
}

// FormatAudit formats one audit line. It refuses an unknown kind, a check where the kind names none
// or none where it needs one, a check with a space in it, the wrong number of counts, and a negative
// count.
func FormatAudit(kind AuditKind, check string, counts ...int) (string, error) {
	keys, ok := auditGrammar()[kind]
	if !ok {
		return "", fmt.Errorf("%w: unknown kind %q", errAudit, kind)
	}

	fields := []string{AuditPrefix, string(kind)}

	switch {
	case aboutACheck(kind) && (check == "" || strings.ContainsAny(check, " \t\n=")):
		return "", fmt.Errorf("%w: %s needs a check ID with no space or '=', got %q", errAudit, kind, check)
	case !aboutACheck(kind) && check != "":
		return "", fmt.Errorf("%w: %s names no check, got %q", errAudit, kind, check)
	case len(counts) != len(keys):
		return "", fmt.Errorf("%w: %s takes %d counts (%s), got %d", errAudit, kind, len(keys),
			strings.Join(keys, " "), len(counts))
	case aboutACheck(kind):
		fields = append(fields, checkKey+"="+check)
	default:
	}

	for i, key := range keys {
		if counts[i] < 0 {
			return "", fmt.Errorf("%w: %s %s=%d is negative", errAudit, kind, key, counts[i])
		}

		fields = append(fields, key+"="+strconv.Itoa(counts[i]))
	}

	return strings.Join(fields, " "), nil
}

// ParseAudit reads an audit line out of one line of test output, and reports whether it held one
// that follows the grammar: a known kind, its check when it names one, and exactly its keys, in
// order, each a count.
func ParseAudit(line string) (Audit, bool) {
	text, found := auditIn(line)
	if !found {
		return Audit{}, false
	}

	fields := strings.Fields(text)
	if len(fields) < minAuditFields {
		return Audit{}, false
	}

	audit := Audit{Kind: AuditKind(fields[1])}

	keys, ok := auditGrammar()[audit.Kind]
	if !ok {
		return Audit{}, false
	}

	pairs := fields[minAuditFields:]
	if aboutACheck(audit.Kind) {
		if audit.Check, pairs, ok = parseCheck(pairs); !ok {
			return Audit{}, false
		}
	}

	if audit.Counts, ok = parseCounts(keys, pairs); !ok {
		return Audit{}, false
	}

	return audit, true
}

// parseCheck reads the check=<id> pair that leads pairs, and returns the pairs after it.
func parseCheck(pairs []string) (string, []string, bool) {
	if len(pairs) == 0 {
		return "", nil, false
	}

	check, found := strings.CutPrefix(pairs[0], checkKey+"=")
	if !found || check == "" {
		return "", nil, false
	}

	return check, pairs[1:], true
}

// parseCounts reads exactly keys, in order, each a non-negative count.
func parseCounts(keys, pairs []string) (map[string]int, bool) {
	if len(pairs) != len(keys) {
		return nil, false
	}

	counts := make(map[string]int, len(keys))

	for i, key := range keys {
		value, found := strings.CutPrefix(pairs[i], key+"=")

		n, err := strconv.Atoi(value)
		if !found || err != nil || n < 0 {
			return nil, false
		}

		counts[key] = n
	}

	return counts, true
}

// auditIn returns the audit text in one line of test output, and whether the line carries one at
// all, well formed or not.
func auditIn(line string) (string, bool) {
	m := auditLinePattern.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}

	return m[1], true
}
