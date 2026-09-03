package effect

import "regexp"

// Canonicaliser rewrites raw effect text into the form that is compared between runs.
//
// One Canonicaliser serves a whole run; provenance is supplied per message.
type Canonicaliser struct {
	patterns []typePattern
}

type typePattern struct {
	expr        *regexp.Regexp
	placeholder string
}

// NewCanonicaliser builds a Canonicaliser with the type substitutions Stutter applies by default.
//
// The set is deliberately small. Every type substitution is a place a real divergence can hide, so
// a pattern is added only when a determinism gate failure demands it — never pre-emptively.
func NewCanonicaliser() *Canonicaliser {
	return &Canonicaliser{patterns: []typePattern{
		{
			expr:        regexp.MustCompile(`\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b`),
			placeholder: "<uuid>",
		},
		{
			expr: regexp.MustCompile(
				`\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:?\d{2})?`,
			),
			placeholder: "<ts>",
		},
	}}
}

// Canonicalise returns the comparable form of text.
//
// Order is load-bearing. Provenance runs first so that an identifier which arrived in the message
// keeps its identity as <msg:...>; letting the type patterns see it first would flatten it to
// <uuid> and destroy the distinction between the id the message carried and one the handler
// invented. A nil provenance applies type substitution alone.
func (c *Canonicaliser) Canonicalise(text string, prov *Provenance) string {
	if prov != nil {
		text = prov.Substitute(text)
	}

	for _, pattern := range c.patterns {
		text = pattern.expr.ReplaceAllString(text, pattern.placeholder)
	}

	return text
}
