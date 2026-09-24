package compose

import (
	"maps"
	"slices"
	"strconv"
	"strings"
)

// proxyVariables are the variables that route a process's HTTP through a proxy. A surviving one
// sends every external call to a CONNECT the stub refuses, so each is removed.
//
//nolint:gochecknoglobals // a fixed list, not mutable state.
var proxyVariables = []string{
	"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy",
}

// The sources a removed or overridden variable is named with.
const (
	sourceCompose = "compose"
	sourceImage   = "image"
)

// unescape turns compose's `$$` into `$`, once. Compose escapes every literal `$` in its JSON, and a
// created container must see one.
func unescape(text string) string {
	return strings.ReplaceAll(text, "$$", "$")
}

// unescapeAll unescapes every value; nil stays nil, so an unset list stays unset.
func unescapeAll(values []string) []string {
	if values == nil {
		return nil
	}

	out := make([]string, len(values))
	for index, value := range values {
		out[index] = unescape(value)
	}

	return out
}

// composeEnvironment returns a service's `environment` as compose printed it: still escaped, a key
// compose left null absent.
func composeEnvironment(svc *composeService) map[string]string {
	out := make(map[string]string, len(svc.Environment))

	for key, value := range svc.Environment {
		if value != nil {
			out[key] = *value
		}
	}

	return out
}

// imageEnvironment reads an image's `ENV` entries.
func imageEnvironment(img Image) map[string]string {
	out := make(map[string]string, len(img.Env))

	for _, entry := range img.Env {
		key, value, _ := strings.Cut(entry, "=")
		out[key] = value
	}

	return out
}

// Environment returns the environment service runs with, before any proxy variable is removed or
// CA variable set: the image's `ENV` overlaid by compose's, unescaped, a null compose key leaving
// the image's value. A fresh map per call; its values leave this package only here.
func (m *Model) Environment(service string, img Image) map[string]string {
	svc, ok := m.typed.Services[service]
	if !ok {
		return nil
	}

	env := imageEnvironment(img)
	for key, value := range composeEnvironment(svc) {
		env[key] = unescape(value)
	}

	return env
}

// applyEnvironment sets the environment a spec passes: compose's, unescaped, without null keys and
// without proxy variables — each one the image sets is unset instead — and, with ca, the CA
// variables over any compose or image value. No other value is rewritten.
func applyEnvironment(spec *Spec, svc *composeService, img Image, ca map[string]string) {
	spec.raw = composeEnvironment(svc)
	spec.Env = make(map[string]string, len(spec.raw)+len(ca))

	for key, value := range spec.raw {
		spec.Env[key] = unescape(value)
	}

	image := imageEnvironment(img)

	for _, name := range proxyVariables {
		if _, ok := spec.Env[name]; ok {
			delete(spec.Env, name)
			spec.ProxyRemoved = append(spec.ProxyRemoved, Named{Name: name, Source: sourceCompose})
		}
	}

	for _, name := range proxyVariables {
		if _, ok := image[name]; ok {
			spec.Unset = append(spec.Unset, name)
			spec.ProxyRemoved = append(spec.ProxyRemoved, Named{Name: name, Source: sourceImage})
		}
	}

	if ca == nil {
		return
	}

	for _, name := range CAVariables() {
		if _, ok := spec.Env[name]; ok {
			spec.CAOverridden = append(spec.CAOverridden, Named{Name: name, Source: sourceCompose})
		}

		if _, ok := image[name]; ok {
			spec.CAOverridden = append(spec.CAOverridden, Named{Name: name, Source: sourceImage})
		}

		spec.Env[name] = ca[name]
	}
}

// serviceReferences finds every endpoint a service's own configuration names: its environment values,
// unescaped, and each `command` and `entrypoint` argument.
func serviceReferences(svc *composeService) []ref {
	var refs []ref

	env := composeEnvironment(svc)
	for _, key := range slices.Sorted(maps.Keys(env)) {
		refs = append(refs, references(key, unescape(env[key]))...)
	}

	for _, list := range []struct {
		values *[]string
		name   string
	}{
		{values: svc.Command, name: "command"},
		{values: svc.Entrypoint, name: "entrypoint"},
	} {
		if list.values == nil {
			continue
		}

		for index, value := range *list.values {
			refs = append(refs, references(list.name+"["+strconv.Itoa(index)+"]", unescape(value))...)
		}
	}

	return refs
}
