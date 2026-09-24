package compose_test

import (
	"slices"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

func TestEveryNameKindIsInTheTargetView(t *testing.T) {
	t.Parallel()

	const firstReplica = "shop-db-1"

	cases := []struct {
		name, model string
		want        []string
	}{
		{
			name: "service name",
			model: `{"name": "shop", "services": {"api": {"networks": {"default": null}},
				"db": {"networks": {"default": null}}}}`,
			want: []string{"db", firstReplica},
		},
		{
			name: "container_name",
			model: `{"name": "shop", "services": {"api": {"networks": {"default": null}},
				"db": {"container_name": "shop-database", "networks": {"default": null}}}}`,
			want: []string{"db", "shop-database"},
		},
		{
			name: "default container name for every replica",
			model: `{"name": "shop", "services": {"api": {"networks": {"default": null}},
				"db": {"deploy": {"replicas": 2}, "networks": {"default": null}}}}`,
			want: []string{"db", firstReplica, "shop-db-2"},
		},
		{
			name: "hostname",
			model: `{"name": "shop", "services": {"api": {"networks": {"default": null}},
				"db": {"hostname": "pghost", "networks": {"default": null}}}}`,
			want: []string{"db", "pghost", firstReplica},
		},
		{
			name: "shared-network alias",
			model: `{"name": "shop", "services": {"api": {"networks": {"front": null}},
				"db": {"networks": {"front": {"aliases": ["primary"]}}}}}`,
			want: []string{"db", "primary", firstReplica},
		},
		{
			name: "links alias",
			model: `{"name": "shop", "services": {"api": {"links": ["db:database"], "networks": {"default": null}},
				"db": {"networks": {"default": null}}}}`,
			want: []string{"database", "db", firstReplica},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := compose.NamesFrom(modelFrom(t, tc.model), "db", target)
			if !slices.Equal(got, tc.want) {
				t.Errorf("NamesFrom(db, api) = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAForeignNetworkAliasIsAbsent(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"networks": {"front": null}},
		"db": {"networks": {"front": {"aliases": ["primary"]}, "back": {"aliases": ["hidden"]}}},
		"batch": {"networks": {"back": null}},
		"island": {"networks": {"elsewhere": null}},
		"host": {"network_mode": "host"}
	}}`)

	if got := compose.NamesFrom(model, "db", target); slices.Contains(got, "hidden") {
		t.Errorf("NamesFrom(db, api) = %v carries an alias on a network api does not join", got)
	}

	if got := compose.NamesFrom(model, "db", "batch"); !slices.Contains(got, "hidden") ||
		slices.Contains(got, "primary") {
		t.Errorf("NamesFrom(db, batch) = %v, want hidden and not primary", got)
	}

	for _, dep := range []string{"island", "host", "ghost"} {
		if got := compose.NamesFrom(model, dep, target); len(got) != 0 {
			t.Errorf("NamesFrom(%s, api) = %v, want none: no shared network", dep, got)
		}
	}

	if got := compose.NamesFrom(model, "db", "host"); len(got) != 0 {
		t.Errorf("NamesFrom(db, host) = %v, want none: a network_mode viewer joins no network", got)
	}
}

func TestHostnameDotDomainnameIsNotAName(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {"api": {"networks": {"default": null}},
		"db": {"hostname": "pg", "domainname": "corp.test", "networks": {"default": null}}}}`)

	got := compose.NamesFrom(model, "db", target)
	if slices.Contains(got, "pg.corp.test") || !slices.Contains(got, "pg") {
		t.Errorf("NamesFrom(db, api) = %v, want pg and not pg.corp.test", got)
	}
}
