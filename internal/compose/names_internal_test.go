package compose

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// namesModel has two services that both answer to `db` on the target's network, and two bus
// services that share `bus`.
const namesModel = `{"name": "shop", "services": {
	"api": {"networks": {"default": {"aliases": ["self"]}}},
	"db": {"networks": {"default": null}},
	"replica": {"networks": {"default": {"aliases": ["db"]}}},
	"bus-a": {"networks": {"default": {"aliases": ["bus"]}}},
	"bus-b": {"networks": {"default": {"aliases": ["bus"]}}},
	"shadow": {"networks": {"default": {"aliases": ["self"]}}},
	"cache": {"networks": {"back": {"aliases": ["db"]}}},
	"job": {"networks": {"default": null, "back": null}},
	"worker": {"networks": {"default": null, "back": null}},
	"legacy": {"external_links": ["old_db_1:olddb", "plain_1"], "networks": {"default": null}}
}}`

func wantCollision(t *testing.T, err error, names ...string) {
	t.Helper()

	if !errors.Is(err, ErrModel) {
		t.Fatalf("collision check = %v, want ErrModel", err)
	}

	for _, name := range names {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
}

func TestANameClaimedTwiceIsRefused(t *testing.T) {
	t.Parallel()

	model := parseIn(t, t.TempDir(), namesModel, "")
	self := model.selfAliases()

	if want := []string{testTarget, "self", "shop-api-1"}; !slices.Equal(self, want) {
		t.Errorf("self-aliases = %v, want %v", self, want)
	}

	views := model.targetViews([]string{"db", "replica"})
	wantCollision(t, viewCollision(testTarget, views, self, nil), `"db"`, "db", "replica")

	views = model.targetViews([]string{"db", "shadow"})
	wantCollision(t, viewCollision(testTarget, views, self, nil), `"self"`, "shadow", "api")

	bus := map[string]bool{"bus-a": true, "bus-b": true}
	if err := viewCollision(testTarget, model.targetViews([]string{"bus-a", "bus-b", "db"}), self, bus); err != nil {
		t.Errorf("two bus services sharing a name = %v, want one bus", err)
	}

	// cache's `db` alias is on a network the target does not join: no collision in its view.
	if err := viewCollision(testTarget, model.targetViews([]string{"db", cacheService}), self, nil); err != nil {
		t.Errorf("an alias on a foreign network = %v, want no collision", err)
	}

	// job sees db on the default network and cache as `db` on the back network.
	sets := map[string][]string{
		"db":         model.dependencyNames("db", []string{"job", "worker", "db"}),
		cacheService: model.dependencyNames(cacheService, []string{"job", "worker", cacheService}),
	}
	wantCollision(t, networkCollision(sets), `"db"`, cacheService, "db")

	if got := externalLinkNames(model.typed.Services["legacy"]); !slices.Equal(got, []string{"olddb", "plain_1"}) {
		t.Errorf("external link names = %v", got)
	}
}
