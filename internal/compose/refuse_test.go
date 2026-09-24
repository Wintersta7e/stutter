package compose_test

import (
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

const (
	labelNS     = "io.stutter"
	networkMode = "network_mode"
	firstType   = "volumes[0].type"
	firstTarget = "volumes[0].target"
)

// refusalOf parses model and runs the refusals over started, fingerprinting their bind sources
// first as the check does.
func refusalOf(t *testing.T, model string, started ...string) error {
	t.Helper()

	parsed := modelFrom(t, model)

	if len(started) == 0 {
		started = []string{target}
	}

	// A source that does not resolve is refused by the fingerprint too; Refusals must say so itself.
	prints, err := compose.Fingerprint(t.Context(), parsed.BindSources(started))
	if err != nil && !errors.Is(err, compose.ErrRefused) {
		t.Fatalf("Fingerprint: %v", err)
	}

	return parsed.Refusals(started, labelNS, prints)
}

// service wraps one target service body, and optional top-level keys, into a model.
func service(body string, topLevel ...string) string {
	keys := append([]string{`"name": "shop"`, `"services": {"api": ` + body + `, "db": {"image": "d"}}`}, topLevel...)

	return "{" + strings.Join(keys, ", ") + "}"
}

func TestEachRefusedClassIsRefusedByItsKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	source := strconv.Quote(dir)
	missing := filepath.Join(t.TempDir(), "absent")

	cases := []struct {
		model string
		key   string
		class compose.Class
	}{
		{class: compose.K1, model: service(`{"privileged": true}`), key: "privileged"},
		{class: compose.K2, model: service(`{"cap_add": ["CHOWN", "cap_sys_admin"]}`), key: "cap_add"},
		{class: compose.K3, model: service(`{"network_mode": "host"}`), key: networkMode},
		{class: compose.K3, model: service(`{"network_mode": "service:db"}`), key: networkMode},
		{class: compose.K3, model: service(`{"network_mode": "none"}`), key: networkMode},
		{class: compose.K4, model: service(`{"pid": "host"}`), key: pidKey},
		{class: compose.K4, model: service(`{"ipc": "host"}`), key: ipcKey},
		{class: compose.K4, model: service(`{"uts": "host"}`), key: utsKey},
		{class: compose.K4, model: service(`{"userns_mode": "host"}`), key: "userns_mode"},
		{class: compose.K4, model: service(`{"cgroup": "host"}`), key: "cgroup"},
		{
			class: compose.K5, model: service(`{"devices": [{"source": "/dev/fuse", "target": "/dev/fuse"}]}`),
			key: "devices",
		},
		{class: compose.K5, model: service(`{"device_cgroup_rules": ["c 1:3 mr"]}`), key: "device_cgroup_rules"},
		{class: compose.K5, model: service(`{"gpus": [{"count": -1}]}`), key: "gpus"},
		{
			class: compose.K5, key: "deploy.resources.reservations.devices",
			model: service(`{"deploy": {"resources": {"reservations": {"devices": [{"capabilities": ["gpu"]}]}}}}`),
		},
		{
			class: compose.K5, key: "deploy.resources.reservations.generic_resources",
			model: service(`{"deploy": {"resources": {"reservations": {"generic_resources":
				[{"discrete_resource_spec": {"kind": "gpu", "value": 1}}]}}}}`),
		},
		{class: compose.K6, model: service(`{"runtime": "runsc"}`), key: "runtime"},
		{class: compose.K6, model: service(`{"isolation": "hyperv"}`), key: "isolation"},
		{class: compose.K6, model: service(`{"credential_spec": {"file": "spec.json"}}`), key: "credential_spec"},
		{class: compose.K7, model: service(`{"volumes_from": ["db"]}`), key: "volumes_from"},
		{class: compose.K7, model: service(`{"use_api_socket": true}`), key: "use_api_socket"},
		{class: compose.K7, model: service(`{"provider": {"type": "cloud"}}`), key: "provider"},
		{class: compose.K7, model: service(`{"models": {"llm": {}}}`), key: "models"},
		{class: compose.K8, model: service(`{"pre_start": [{"command": ["x"]}]}`), key: "pre_start"},
		{class: compose.K8, model: service(`{"post_start": [{"command": ["x"]}]}`), key: "post_start"},
		{class: compose.K8, model: service(`{"pre_stop": [{"command": ["x"]}]}`), key: "pre_stop"},
		{
			class: compose.K9, model: service(`{"volumes": [{"type": "npipe", "source": "p", "target": "/p"}]}`),
			key: firstType,
		},
		{
			class: compose.K9, model: service(`{"volumes": [{"type": "cluster", "source": "c", "target": "/c"}]}`),
			key: firstType,
		},
		{
			class: compose.K9, model: service(`{"volumes": [{"type": "image", "source": "i", "target": "/i"}]}`),
			key: firstType,
		},
		{
			class: compose.K9, key: "volumes[0].volume.subpath",
			model: service(`{"volumes": [{"type": "volume", "source": "data", "target": "/d",
				"volume": {"subpath": "s"}}]}`, `"volumes": {"data": {"name": "shop_data"}}`),
		},
		{
			class: compose.K9, key: "volumes.ext.external",
			model: service(`{"volumes": [{"type": "volume", "source": "ext", "target": "/e"}]}`,
				`"volumes": {"ext": {"name": "ext", "external": true}}`),
		},
		{
			class: compose.K9, key: "volumes[0].bind.selinux",
			model: service(`{"volumes": [{"type": "bind", "source": ` + source + `, "target": "/b",
				"bind": {"selinux": "z"}}]}`),
		},
		{
			class: compose.K10, key: missing,
			model: service(`{"volumes": [{"type": "bind", "source": ` + strconv.Quote(missing) + `, "target": "/b"}]}`),
		},
		{
			class: compose.K10, key: missing,
			model: service(`{"configs": [{"source": "c", "target": "/c"}]}`,
				`"configs": {"c": {"file": `+strconv.Quote(missing)+`}}`),
		},
		{
			class: compose.K11, key: "configs.c with read_only",
			model: service(`{"read_only": true, "configs": [{"source": "c", "target": "/etc/c"}]}`,
				`"configs": {"c": {"content": "x"}}`),
		},
		{
			class: compose.K11, key: "secrets.tok.target under tmpfs[0]",
			model: service(`{"tmpfs": ["/run/secrets:size=1m"],
				"secrets": [{"source": "tok", "target": "/run/secrets/tok"}]}`,
				`"secrets": {"tok": {"environment": "TOKEN"}}`),
		},
		{
			class: compose.K11, key: "configs.c.target under volumes[0]",
			model: service(`{"volumes": [{"type": "bind", "source": `+source+`, "target": "/etc/app"}],
				"configs": [{"source": "c", "target": "/etc/app/c.conf"}]}`, `"configs": {"c": {"content": "x"}}`),
		},
		{
			class: compose.K11, key: "configs.c.external",
			model: service(`{"configs": [{"source": "c"}]}`, `"configs": {"c": {"external": true, "name": "c"}}`),
		},
		{
			class: compose.K11, key: "configs.c.template_driver",
			model: service(`{"configs": [{"source": "c"}]}`,
				`"configs": {"c": {"content": "x", "template_driver": "t"}}`),
		},
		{
			class: compose.K11, key: "secrets.s.driver",
			model: service(`{"secrets": [{"source": "s"}]}`, `"secrets": {"s": {"environment": "S", "driver": "d"}}`),
		},
		{class: compose.K12, model: service(`{"labels": {"io.stutter.check": "x"}}`), key: "labels.io.stutter.check"},
		{
			class: compose.K12, key: "build.labels.io.stutter.kind",
			model: service(`{"build": {"context": "/project", "labels": {"io.stutter.kind": "target"}}}`),
		},
		{class: compose.K13, model: service(`{"tmpfs": ["/etc"]}`), key: "tmpfs[0]"},
	}

	classes := map[compose.Class]int{}

	for _, tc := range cases {
		err := refusalOf(t, tc.model)

		refusal, ok := errors.AsType[*compose.Refusal](err)
		if !ok || refusal.Class != tc.class || refusal.Key != tc.key || refusal.Service != target {
			t.Errorf("%s %s: Refusals = %v, want %s refusing %s", tc.class, tc.key, err, tc.class, tc.key)

			continue
		}

		classes[tc.class]++
	}

	// K14 applies at parse, to any unlisted key.
	run := &fakeRun{whole: func([]string) ([]byte, int, error) {
		return []byte(service(`{"future_key": 1}`)), 0, nil
	}}

	files := []string{project(t, "c.yaml")}

	_, err := compose.Parse(t.Context(), run.run, compose.Inputs{Service: target, Files: files})
	if refusal, ok := errors.AsType[*compose.Refusal](err); ok && refusal.Class == compose.K14 &&
		refusal.Key == "future_key" {
		classes[compose.K14]++
	} else {
		t.Errorf("K14: Parse = %v, want a K14 refusal of future_key", err)
	}

	t.Logf("classes=%d cases=%d", len(classes), len(cases)+1)

	for class := compose.K1; class <= compose.K14; class++ {
		if classes[class] == 0 {
			t.Errorf("class %s has no passing case", class)
		}
	}
}

func TestAMountAtOrAboveTheCAPathIsRefused(t *testing.T) {
	t.Parallel()

	source := strconv.Quote(t.TempDir())
	bind := func(target string) string {
		return service(`{"volumes": [{"type": "bind", "source": ` + source + `, "target": "` + target + `"}]}`)
	}

	cases := []struct{ name, model, key string }{
		{name: "bind at /etc/stutter", model: bind("/etc/stutter"), key: firstTarget},
		{
			name: "volume at /etc", key: firstTarget,
			model: service(`{"volumes": [{"type": "volume", "target": "/etc"}]}`),
		},
		{
			name: "volume at /etc/", key: firstTarget,
			model: service(`{"volumes": [{"type": "volume", "target": "/etc/"}]}`),
		},
		{name: "tmpfs key at /etc/stutter", model: service(`{"tmpfs": ["/etc/stutter"]}`), key: "tmpfs[0]"},
		{
			name: "long tmpfs at /etc/stutter", key: firstTarget,
			model: service(`{"volumes": [{"type": "tmpfs", "target": "/etc/stutter"}]}`),
		},
		{
			name: "config at the CA path", key: "configs.c.target",
			model: service(`{"configs": [{"source": "c", "target": "`+compose.CAMountPath+`"}]}`,
				`"configs": {"c": {"content": "x"}}`),
		},
	}

	for _, tc := range cases {
		var refusal *compose.Refusal
		if err := refusalOf(t, tc.model); !errors.As(err, &refusal) || refusal.Class != compose.K13 ||
			refusal.Key != tc.key {
			t.Errorf("%s: Refusals = %v, want K13 refusing %s", tc.name, err, tc.key)
		}
	}

	if err := refusalOf(t, bind("/etc/stutter-other")); err != nil {
		t.Errorf("a bind at /etc/stutter-other = %v, want accepted", err)
	}
}

func TestARefusalOnlyCoversStartedServices(t *testing.T) {
	t.Parallel()

	model := `{"name": "shop", "services": {"api": {"image": "a"}, "sibling": {"image": "s", "privileged": true}}}`

	if err := refusalOf(t, model); err != nil {
		t.Errorf("a privileged service that is not started = %v, want nil", err)
	}

	var refusal *compose.Refusal
	if err := refusalOf(t, model, target, "sibling"); !errors.As(err, &refusal) || refusal.Service != "sibling" {
		t.Errorf("a privileged started service = %v, want refused", err)
	}
}

func TestBindSourcesAreEveryBindAndFileSource(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, string(golden(t, "5.5.1")))
	got := model.BindSources([]string{target, "db", absentService})
	want := []string{"/project/conf", "/project/conf/file.conf", "/project/data"}

	if !slices.Equal(got, want) {
		t.Errorf("BindSources = %v, want %v", got, want)
	}

	if got := model.BindSources([]string{"db"}); len(got) != 0 {
		t.Errorf("BindSources(db) = %v, want none", got)
	}
}
