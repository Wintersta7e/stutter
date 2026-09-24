package dockertest

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
)

// Spellings the call tables share.
const (
	post             = http.MethodPost
	get              = http.MethodGet
	del              = http.MethodDelete
	cid              = "abc"
	imagesCreate     = "/images/create"
	containersCreate = "/containers/create"
	tagKey           = "tag"
)

func TestClassifyEngineCalls(t *testing.T) {
	t.Parallel()

	q := func(pairs ...string) url.Values {
		v := url.Values{}
		for i := 0; i+1 < len(pairs); i += 2 {
			v.Add(pairs[i], pairs[i+1])
		}

		return v
	}

	cases := []struct {
		method, path string
		query        url.Values
		targets      []string
		mutating     bool
		prune        bool
		build        bool
	}{
		{method: get, path: "/v1.51/containers/json"},
		{method: http.MethodHead, path: "/_ping"},
		{method: post, path: "/v1.51/containers/" + cid + "/wait"},
		{method: post, path: "/containers/" + cid + "/attach"},
		{method: post, path: "/v1.47/containers/" + cid + "/resize"},
		{method: post, path: "/grpc", build: true},
		{method: post, path: "/v1.51/session", build: true},
		{method: post, path: "/v1.51/containers/prune", mutating: true, prune: true},
		{method: post, path: "/images/prune", mutating: true, prune: true},
		{method: post, path: "/v1.51/volumes/prune", mutating: true, prune: true},
		{method: post, path: "/networks/prune", mutating: true, prune: true},
		{method: post, path: "/v1.51/build/prune", mutating: true, prune: true},
		{
			method:   "DELETE",
			path:     "/v1.51/images/sha256:ab",
			query:    q("noprune", "1"),
			mutating: true,
			targets:  []string{"sha256:ab"},
		},
		{method: post, path: "/v1.51/containers/create", query: q("name", "x"), mutating: true},
		{method: post, path: "/networks/create", mutating: true},
		{method: post, path: "/volumes/create", mutating: true},
		{method: post, path: "/v1.51/containers/" + cid + "/start", mutating: true, targets: []string{cid}},
		{method: post, path: "/containers/" + cid + "/kill", mutating: true, targets: []string{cid}},
		{method: del, path: "/v1.51/containers/" + cid, mutating: true, targets: []string{cid}},
		{
			method:   "PUT",
			path:     "/containers/" + cid + "/archive",
			query:    q("path", "/d"),
			mutating: true,
			targets:  []string{"abc"},
		},
		{method: post, path: "/networks/n1/connect", mutating: true, targets: []string{"n1"}},
		{method: del, path: "/networks/n1", mutating: true, targets: []string{"n1"}},
		{method: del, path: "/volumes/v1", mutating: true, targets: []string{"v1"}},
		{
			method: post, path: "/images/stutter.invalid/c/app:1/tag", query: q("repo", "other/x", tagKey, "2"),
			mutating: true, targets: []string{"stutter.invalid/c/app:1", "other/x:2"},
		},
		{
			method: post, path: "/commit", query: q("container", cid, "repo", "stutter.invalid/c/s", tagKey, "1"),
			mutating: true, targets: []string{cid, "stutter.invalid/c/s:1"},
		},
		{
			method: post, path: imagesCreate, query: q("fromSrc", "-", "repo", "stutter.invalid/c/t", tagKey, "1"),
			mutating: true, targets: []string{"stutter.invalid/c/t:1"},
		},
		{method: post, path: "/v1.51/exec/" + cid + "/start", mutating: true},
		{method: post, path: "/images/load", mutating: true},
	}

	for _, tc := range cases {
		c := classify(tc.method, tc.path, tc.query)
		if c.mutating != tc.mutating || c.prune != tc.prune || c.buildChannel != tc.build ||
			!slices.Equal(c.targets, tc.targets) {
			t.Errorf("%s %s?%s = %+v; want mutating=%v prune=%v build=%v targets=%q", tc.method, tc.path,
				tc.query.Encode(), c, tc.mutating, tc.prune, tc.build, tc.targets)
		}
	}
}

func TestOwnershipFollowsWhatTheRecorderSaw(t *testing.T) {
	t.Parallel()

	const check = "c0ffee"

	own := func(calls ...Call) []Call {
		for i := range calls {
			calls[i].Seq = i + 1
		}

		return calls
	}
	created := Call{Method: post, Path: containersCreate, Status: 201, Created: strings.Repeat("a", 64)}
	named := Call{
		Method: post, Path: containersCreate, Query: url.Values{"name": {"stutter-x"}}, Status: 201,
		Created: strings.Repeat("b", 64),
	}
	slashed := Call{
		Method: post, Path: containersCreate, Query: url.Values{"name": {"/stutter-y"}}, Status: 201,
		Created: strings.Repeat("d", 64), Name: "/stutter-y",
	}
	start := func(target string) Call {
		return Call{Method: post, Path: "/containers/" + target + "/start", Status: 204}
	}
	inspect404 := Call{Method: get, Path: "/images/postgres:18-alpine/json", Status: 404}
	pull := Call{
		Method: post, Path: imagesCreate, Query: url.Values{"fromImage": {"postgres"}, tagKey: {"18-alpine"}},
		Status: 200,
	}
	importAt := func(repo string) Call {
		return Call{
			Method: post, Path: imagesCreate, Query: url.Values{"fromSrc": {"-"}, "repo": {repo}, tagKey: {"1"}},
			Status: 200, Created: "sha256:" + strings.Repeat("c", 64),
		}
	}
	commitOf := func(container string) Call {
		return Call{
			Method: post, Path: "/commit",
			Query:  url.Values{"container": {container}, "repo": {"stutter.invalid/" + check + "/s"}, tagKey: {"1"}},
			Status: 201,
		}
	}

	cases := []struct {
		name    string
		calls   []Call
		foreign int
	}{
		{name: "a start of what it created", calls: own(created, start(created.Created))},
		{name: "a start by name", calls: own(named, start("stutter-x"))},
		{name: "a name created with the slash", calls: own(slashed, start("stutter-y"))},
		{name: "a foreign start", calls: own(start(strings.Repeat("f", 64))), foreign: 1},
		{name: "no prefix match", calls: own(created, start(strings.Repeat("a", 12))), foreign: 1},
		{name: "a pull after a 404", calls: own(inspect404, pull)},
		{name: "a pull with no 404", calls: own(pull), foreign: 1},
		{name: "an import under the check's scheme", calls: own(importAt("stutter.invalid/" + check + "/t"))},
		{name: "an import under another check", calls: own(importAt("stutter.invalid/other/t")), foreign: 1},
		{name: "an import outside the scheme", calls: own(importAt("decoy.invalid/t")), foreign: 1},
		{name: "a commit of its own container", calls: own(created, commitOf(created.Created))},
		{name: "a commit of a foreign container", calls: own(commitOf("stranger")), foreign: 1},
		{name: "a prune", calls: own(Call{Method: post, Path: "/containers/prune", Status: 200}), foreign: 1},
	}

	for _, tc := range cases {
		s := summarize(tc.calls, nil, census{}, check)
		if s.ForeignTouched != tc.foreign || len(s.Foreign) != tc.foreign {
			t.Errorf("%s: foreign-touched=%d (%v), want %d", tc.name, s.ForeignTouched, s.Foreign, tc.foreign)
		}
	}
}
