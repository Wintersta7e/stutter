package compose_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// The build route hands BuildModel to the driver as a method value of exactly this type.
var _ func(map[string]string, map[string]map[string]string) ([]byte, error) = (*compose.Model)(nil).BuildModel

const (
	kindLabel = "io.stutter.kind"
	firstTag  = "t:1"
	cacheTo   = "cache_to"
	imageKey  = "image"
	// targetKind and platformsKey are a label value and a build key several cases name.
	targetKind   = "target"
	platformsKey = "platforms"
	tagsKey      = "tags"
)

func decodeJSON(t *testing.T, data []byte) map[string]any {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var out map[string]any
	if err := decoder.Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	return out
}

// child walks a decoded model by keys.
func child(t *testing.T, node map[string]any, keys ...string) map[string]any {
	t.Helper()

	for _, key := range keys {
		next, ok := node[key].(map[string]any)
		if !ok {
			t.Fatalf("no map at %s", strings.Join(keys, "."))
		}

		node = next
	}

	return node
}

func TestBuildModelRewritesOnlyWhatItMust(t *testing.T) {
	t.Parallel()

	const tag = "stutter.invalid/check/target:1"

	input := golden(t, "5.5.1")
	model := modelFrom(t, strings.Replace(string(input), `"PLAIN": "plain"`, `"PLAIN": "pa$$word"`, 1))
	labels := map[string]string{"io.stutter.check": "c1", kindLabel: targetKind}

	out, err := model.BuildModel(map[string]string{target: tag}, map[string]map[string]string{target: labels})
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}

	got := decodeJSON(t, out)
	want := decodeJSON(t, []byte(strings.Replace(string(input), `"PLAIN": "plain"`, `"PLAIN": "pa$$word"`, 1)))

	gotAPI, wantAPI := child(t, got, "services", target), child(t, want, "services", target)
	gotBuild, wantBuild := child(t, gotAPI, "build"), child(t, wantAPI, "build")

	if gotAPI[imageKey] != tag {
		t.Errorf("image = %v, want the reserved tag", gotAPI[imageKey])
	}

	for _, key := range []string{tagsKey, cacheTo, platformsKey} {
		if _, ok := gotBuild[key]; ok {
			t.Errorf("build.%s survived: %v", key, gotBuild[key])
		}
	}

	wantLabels := map[string]any{"com.example.test.kind": "app", "io.stutter.check": "c1", kindLabel: targetKind}
	if !reflect.DeepEqual(gotBuild["labels"], wantLabels) {
		t.Errorf("build.labels = %v, want %v", gotBuild["labels"], wantLabels)
	}

	if _, ok := got["name"]; ok {
		t.Errorf("top-level name survived: %v", got["name"])
	}

	if env := child(t, gotAPI, "environment"); env["PLAIN"] != "pa$$word" {
		t.Errorf("PLAIN = %v, want still escaped", env["PLAIN"])
	}

	for _, touched := range []struct {
		node map[string]any
		key  string
	}{
		{gotAPI, imageKey},
		{wantAPI, imageKey},
		{gotBuild, tagsKey},
		{wantBuild, tagsKey},
		{gotBuild, cacheTo},
		{wantBuild, cacheTo},
		{gotBuild, "labels"},
		{wantBuild, "labels"},
		{gotBuild, platformsKey},
		{wantBuild, platformsKey},
		{got, "name"},
		{want, "name"},
	} {
		delete(touched.node, touched.key)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildModel changed more than the six rewrites:\n%v\n%v", got, want)
	}
}

func TestBuildModelPinsTheServicePlatform(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {"api": {"platform": "linux/arm64",
		"build": {"context": "/project", "platforms": ["linux/amd64", "linux/arm64"]}}}}`)

	out, err := model.BuildModel(map[string]string{target: firstTag}, map[string]map[string]string{target: {}})
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}

	platforms := child(t, decodeJSON(t, out), "services", target, "build")[platformsKey]
	if !reflect.DeepEqual(platforms, []any{"linux/arm64"}) {
		t.Errorf("build.platforms = %v, want only the service's platform", platforms)
	}
}

func TestBuildModelTakesLabelsPerService(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"build": {"context": "/project/api"}},
		"migrate": {"build": {"context": "/project/api", "labels": {"keep": "yes"}}}
	}}`)

	out, err := model.BuildModel(
		map[string]string{target: firstTag, jobService: "t:2"},
		map[string]map[string]string{target: {kindLabel: targetKind}, jobService: {kindLabel: "job"}},
	)
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}

	decoded := decodeJSON(t, out)
	if labels := child(t, decoded, "services", target, "build", "labels"); labels[kindLabel] != targetKind {
		t.Errorf("api labels = %v", labels)
	}

	labels := child(t, decoded, "services", jobService, "build", "labels")
	if labels[kindLabel] != "job" || labels["keep"] != "yes" {
		t.Errorf("migrate labels = %v", labels)
	}
}

func TestBuildModelRefusesAServiceWithoutBuild(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {"api": {"build": {"context": "/p"}}, "db": {"image": "d"}}}`)

	cases := []struct {
		tags   map[string]string
		labels map[string]map[string]string
		name   string
	}{
		{name: "no build", tags: map[string]string{"db": firstTag}, labels: map[string]map[string]string{"db": {}}},
		{
			name: "unknown service", tags: map[string]string{"ghost": firstTag},
			labels: map[string]map[string]string{"ghost": {}},
		},
		{
			name: "labels for another service", tags: map[string]string{target: firstTag},
			labels: map[string]map[string]string{"db": {}},
		},
		{name: "nothing to build", tags: map[string]string{}, labels: map[string]map[string]string{}},
	}

	for _, tc := range cases {
		if _, err := model.BuildModel(tc.tags, tc.labels); err == nil {
			t.Errorf("%s: BuildModel = nil error", tc.name)
		}
	}
}

func TestImagesMapsPullPolicy(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"image": "a:1", "build": {"context": "/p"}, "pull_policy": "build"},
		"db": {"image": "d:1", "pull_policy": "never", "platform": "linux/amd64"},
		"cache": {"image": "c:1", "pull_policy": "always"},
		"plain": {"image": "p:1"}
	}}`)

	got := model.Images([]string{"plain", target, "db", cacheService, absentService})
	want := []compose.ImageRef{
		{Service: "plain", Ref: "p:1", Policy: compose.PullMissing},
		{Service: target, Ref: "a:1", Policy: compose.PullBuild, Build: true},
		{Service: "db", Ref: "d:1", Platform: "linux/amd64", Policy: compose.PullNever},
		{Service: cacheService, Ref: "c:1", Policy: compose.PullMissing},
	}

	if !slices.Equal(got, want) {
		t.Errorf("Images =\n%+v\nwant\n%+v", got, want)
	}
}
