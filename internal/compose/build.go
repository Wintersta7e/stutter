package compose

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// errBuildRequest means BuildModel was asked for something it cannot render.
var errBuildRequest = errors.New("compose build model")

// Images returns how each of services' images is resolved. `pull_policy: never` refuses an absent
// image, `pull_policy: build` builds it, and every other policy pulls only what is missing — pulling
// over an existing tag would move the user's own tag.
func (m *Model) Images(services []string) []ImageRef {
	out := make([]ImageRef, 0, len(services))

	for _, name := range services {
		svc, ok := m.typed.Services[name]
		if !ok {
			continue
		}

		ref := ImageRef{
			Service: name, Ref: unescape(svc.Image), Platform: unescape(svc.Platform), Policy: PullMissing,
			Build: svc.Build != nil,
		}

		switch svc.PullPolicy {
		case string(PullNever):
			ref.Policy = PullNever
		case string(PullBuild):
			ref.Policy = PullBuild
		default:
		}

		out = append(out, ref)
	}

	return out
}

// BuildModel renders the model compose builds from on stdin: the parsed model verbatim, except that
// each service in tags is built under its tag instead of its own `image`, loses `build.tags` and
// `build.cache_to` (they would move the user's tags and export outside the engine), gains labels
// in `build.labels`, and builds for its own `platform` alone; the top-level `name` is dropped so
// the caller's project name is the only one. Nothing is unescaped: compose reads `$$` itself. The
// bytes are returned, never written.
func (m *Model) BuildModel(tags map[string]string, labels map[string]map[string]string) ([]byte, error) {
	if len(tags) == 0 {
		return nil, fmt.Errorf("%w: no service to build", errBuildRequest)
	}

	if !slices.Equal(slices.Sorted(maps.Keys(tags)), slices.Sorted(maps.Keys(labels))) {
		return nil, fmt.Errorf("%w: tags and labels name different services", errBuildRequest)
	}

	root, err := copyModel(m.root)
	if err != nil {
		return nil, err
	}

	delete(root, "name")

	services, ok := root["services"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: the model has no services", errBuildRequest)
	}

	for _, name := range slices.Sorted(maps.Keys(tags)) {
		service, ok := services[name].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: service %s is not in the model", errBuildRequest, name)
		}

		if err = rewriteBuild(name, service, tags[name], labels[name]); err != nil {
			return nil, err
		}
	}

	out, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("%w: render: %w", errBuildRequest, err)
	}

	return out, nil
}

// rewriteBuild applies the build rewrites to one service.
func rewriteBuild(name string, service map[string]any, tag string, labels map[string]string) error {
	build, ok := service["build"].(map[string]any)
	if !ok {
		return fmt.Errorf("%w: service %s has no build", errBuildRequest, name)
	}

	service["image"] = tag

	delete(build, "tags")
	delete(build, "cache_to")

	merged, ok := build["labels"].(map[string]any)
	if !ok {
		merged = make(map[string]any, len(labels))
	}

	for key, value := range labels {
		merged[key] = value
	}

	build["labels"] = merged

	if platform, ok := service["platform"].(string); ok && platform != "" {
		build["platforms"] = []any{platform}
	} else {
		delete(build, "platforms")
	}

	return nil
}

// copyModel deep-copies the generic model, numbers kept verbatim.
func copyModel(root map[string]any) (map[string]any, error) {
	data, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("%w: copy the model: %w", errBuildRequest, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var out map[string]any
	if err := decoder.Decode(&out); err != nil {
		return nil, fmt.Errorf("%w: copy the model: %w", errBuildRequest, err)
	}

	return out, nil
}
