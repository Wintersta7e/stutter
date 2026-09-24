package compose

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

// decodeModel decodes a model the way Parse does, numbers kept as json.Number.
func decodeModel(t *testing.T, text string) map[string]any {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader([]byte(text)))
	decoder.UseNumber()

	var root map[string]any
	if err := decoder.Decode(&root); err != nil {
		t.Fatalf("decode model: %v", err)
	}

	return root
}

// wantRefusal asserts err is a K14 refusal naming service and key.
func wantRefusal(t *testing.T, err error, service, key string) {
	t.Helper()

	var refusal *Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("walkModel = %v, want a refusal naming %s", err, key)
	}

	if refusal.Service != service || refusal.Key != key || refusal.Class != K14 {
		t.Errorf("refusal = %+v, want service %q key %q class K14", *refusal, service, key)
	}

	if !errors.Is(err, ErrRefused) {
		t.Errorf("refusal %v does not unwrap to ErrRefused", err)
	}
}

func TestAnUnlistedKeyIsRefusedByName(t *testing.T) {
	t.Parallel()

	cases := []struct{ name, model, key string }{
		{
			name:  "service key",
			model: `{"services": {"api": {"image": "x", "future_key": 1}}}`,
			key:   "future_key",
		},
		{
			name:  "nested under a non-subtree key",
			model: `{"services": {"api": {"volumes": [{"type": "volume", "target": "/x", "future_sub": 1}]}}}`,
			key:   "volumes.future_sub",
		},
		{
			name:  "nested two levels",
			model: `{"services": {"api": {"volumes": [{"type": "bind", "bind": {"future": true}}]}}}`,
			key:   "volumes.bind.future",
		},
		{
			name:  "under a user-named network",
			model: `{"services": {"api": {"networks": {"front": {"aliases": ["a"], "future": 1}}}}}`,
			key:   "networks.front.future",
		},
		{
			name:  "under a structural key",
			model: `{"services": {"api": {"deploy": {"resources": {"future": {}}}}}}`,
			key:   "deploy.resources.future",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			wantRefusal(t, walkModel(decodeModel(t, tc.model)), "api", tc.key)
		})
	}

	accepted := `{"services": {"api": {"build": {"context": "/project", "future_sub": 1}}}}`
	if err := walkModel(decodeModel(t, accepted)); err != nil {
		t.Errorf("an unknown key under the build subtree = %v, want accepted", err)
	}
}

func TestAnXKeyAtDepthIsIgnored(t *testing.T) {
	t.Parallel()

	model := `{
		"name": "p",
		"x-stutter": {"roles": {"db": "datastore"}},
		"x-anchor": {"anything": [1, 2]},
		"services": {
			"api": {
				"image": "x",
				"x-service-note": 1,
				"deploy": {"x-note": 1, "resources": {"x-note": 2, "limits": {"cpus": "0.5"}}},
				"volumes": [{"type": "volume", "source": "data", "target": "/x", "x-note": 1}],
				"networks": {"x-net": {"aliases": ["a"]}, "default": null},
				"environment": {"A": "1", "B": null}
			}
		},
		"volumes": {"data": {"name": "p_data"}, "x-vol": {"name": "p_x-vol"}},
		"networks": {"default": {"name": "p_default", "ipam": {}}}
	}`

	if err := walkModel(decodeModel(t, model)); err != nil {
		t.Errorf("walkModel = %v, want every x- key ignored", err)
	}
}

func TestAnUnknownTopLevelKeyIsRefused(t *testing.T) {
	t.Parallel()

	wantRefusal(t, walkModel(decodeModel(t, `{"services": {}, "future": 1}`)), "", "future")
	wantRefusal(t, walkModel(decodeModel(t, `{"volumes": {"data": {"future": 1}}}`)), "", "volumes.data.future")
	wantRefusal(t, walkModel(decodeModel(t, `{"configs": {"c": {"content": "x", "future": 1}}}`)), "",
		"configs.c.future")
}
