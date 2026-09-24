package compose_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

const (
	imageID     = "sha256:1"
	httpProxy   = "HTTP_PROXY"
	lowerHTTPS  = "https_proxy"
	fromCompose = "compose"
	fromImage   = "image"
)

// specOf parses model and builds the target's spec on img, with the CA environment.
func specOf(t *testing.T, model string, img compose.Image) compose.Spec {
	t.Helper()

	spec, err := modelFrom(t, model).Spec(target, img, compose.CAEnvironment())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}

	return spec
}

func TestUnescapeAppliesOnce(t *testing.T) {
	t.Parallel()

	spec := specOf(t, `{"name": "shop", "services": {"api": {
		"environment": {"PASS": "pa$$word", "FOUR": "$$$$", "PLAIN": "x"},
		"entrypoint": ["sh", "-c", "echo $$HOME"],
		"command": ["run", "$$1"],
		"working_dir": "/srv/$$x",
		"hostname": "h$$"
	}}}`, compose.Image{ID: imageID})

	checks := []struct{ name, got, want string }{
		{name: "env PASS", got: spec.Env["PASS"], want: "pa$word"},
		{name: "env FOUR", got: spec.Env["FOUR"], want: "$$"},
		{name: "entrypoint", got: spec.Entrypoint[2], want: "echo $HOME"},
		{name: "command", got: spec.Cmd[1], want: "$1"},
		{name: "working_dir", got: spec.WorkingDir, want: "/srv/$x"},
		{name: "hostname", got: spec.Hostname, want: "h$"},
	}

	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %q, want %q", check.name, check.got, check.want)
		}
	}
}

func TestProxyVariablesAreRemovedAndUnset(t *testing.T) {
	t.Parallel()

	spec := specOf(t, `{"name": "shop", "services": {"api": {
		"environment": {"HTTPS_PROXY": "http://proxy.test:3128", "no_proxy": "x", "KEEP": "1"}
	}}}`, compose.Image{
		ID: imageID, Env: []string{httpProxy + "=http://proxy.test:3128", lowerHTTPS + "=", "PATH=/bin"},
	})

	for _, name := range []string{"HTTPS_PROXY", "no_proxy", httpProxy, lowerHTTPS} {
		if _, ok := spec.Env[name]; ok {
			t.Errorf("Env carries %s", name)
		}
	}

	if spec.Env["KEEP"] != "1" {
		t.Errorf("Env = %v, want KEEP kept", slices.Sorted(maps.Keys(spec.Env)))
	}

	if want := []string{httpProxy, lowerHTTPS}; !slices.Equal(spec.Unset, want) {
		t.Errorf("Unset = %v, want %v", spec.Unset, want)
	}

	want := []compose.Named{
		{Name: "HTTPS_PROXY", Source: fromCompose},
		{Name: "no_proxy", Source: fromCompose},
		{Name: httpProxy, Source: fromImage},
		{Name: lowerHTTPS, Source: fromImage},
	}
	if !slices.Equal(spec.ProxyRemoved, want) {
		t.Errorf("ProxyRemoved = %+v, want %+v", spec.ProxyRemoved, want)
	}
}

func TestCAVariablesWinOnTheTargetOnly(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"environment": {"SSL_CERT_FILE": "/compose/ca.pem"}},
		"migrate": {"environment": {"SSL_CERT_FILE": "/compose/ca.pem"}}
	}}`)
	img := compose.Image{ID: imageID, Env: []string{"NODE_EXTRA_CA_CERTS=/image/ca.pem"}}

	spec, err := model.Spec(target, img, compose.CAEnvironment())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}

	for _, name := range compose.CAVariables() {
		if spec.Env[name] != compose.CAMountPath {
			t.Errorf("target Env[%s] = %q, want %s", name, spec.Env[name], compose.CAMountPath)
		}
	}

	want := []compose.Named{
		{Name: "SSL_CERT_FILE", Source: fromCompose},
		{Name: "NODE_EXTRA_CA_CERTS", Source: fromImage},
	}
	if !slices.Equal(spec.CAOverridden, want) {
		t.Errorf("CAOverridden = %+v, want %+v", spec.CAOverridden, want)
	}

	job, err := model.Spec("migrate", img, nil)
	if err != nil {
		t.Fatalf("Spec(job): %v", err)
	}

	if job.Env["SSL_CERT_FILE"] != "/compose/ca.pem" || len(job.CAOverridden) != 0 {
		t.Errorf("job Env = %v, CAOverridden %v: want compose's value and no override", job.Env, job.CAOverridden)
	}

	names := compose.CAVariables()
	for _, name := range names[1:] {
		if _, ok := job.Env[name]; ok {
			t.Errorf("job Env carries %s", name)
		}
	}

	if _, err := model.Spec(target, img, map[string]string{"SSL_CERT_FILE": "/elsewhere"}); err == nil {
		t.Error("Spec with a CA environment other than CAEnvironment() = nil error")
	}
}

func TestEntrypointWithoutCommandClearsTheImageCmd(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"entrypoint": ["/bin/app"], "command": null},
		"both": {"entrypoint": [], "command": ["serve"]},
		"neither": {"entrypoint": null, "command": null}
	}}`)
	img := compose.Image{ID: imageID}

	cases := []struct {
		service                 string
		entrypoint, cmd         []string
		entrypointSet, cmdIsSet bool
	}{
		{service: target, entrypoint: []string{"/bin/app"}, entrypointSet: true},
		{service: "both", entrypoint: []string{}, cmd: []string{"serve"}, entrypointSet: true, cmdIsSet: true},
		{service: "neither"},
	}

	for _, tc := range cases {
		spec, err := model.Spec(tc.service, img, nil)
		if err != nil {
			t.Fatalf("Spec(%s): %v", tc.service, err)
		}

		if spec.EntrypointSet != tc.entrypointSet || spec.CmdSet != tc.cmdIsSet ||
			!slices.Equal(spec.Entrypoint, tc.entrypoint) || !slices.Equal(spec.Cmd, tc.cmd) {
			t.Errorf("%s: entrypoint %v (set %v), cmd %v (set %v)", tc.service, spec.Entrypoint, spec.EntrypointSet,
				spec.Cmd, spec.CmdSet)
		}
	}
}

func TestUnsetHostnameIsAPerCheckConstant(t *testing.T) {
	t.Parallel()

	hashed := func(name string) string {
		sum := sha256.Sum256([]byte(name))

		return hex.EncodeToString(sum[:])[:12]
	}

	cases := []struct{ name, model, want string }{
		{
			name: "the service name", want: target,
			model: `{"name": "shop", "services": {"api": {"networks": {"default": null}},
				"db": {"networks": {"default": null}}}}`,
		},
		{
			name: "a dependency aliased like the target", want: hashed(target),
			model: `{"name": "shop", "services": {"api": {"networks": {"default": null}},
				"db": {"networks": {"default": {"aliases": ["api"]}}}}}`,
		},
		{
			name: "a bus host named like the target", want: hashed(target),
			model: `{"name": "shop", "services": {"api": {"environment": {"NATS_URL": "nats://api:4222"}}}}`,
		},
		{
			name: "compose's own hostname", want: "box",
			model: `{"name": "shop", "services": {"api": {"hostname": "box"}}}`,
		},
	}

	for _, tc := range cases {
		spec := specOf(t, tc.model, compose.Image{ID: imageID})
		if spec.Hostname != tc.want {
			t.Errorf("%s: hostname %q, want %q", tc.name, spec.Hostname, tc.want)
		}
	}

	model := modelFrom(t, `{"name": "shop", "services": {"api": {}, "my_worker": {}}}`)

	spec, err := model.Spec("my_worker", compose.Image{ID: imageID}, nil)
	if err != nil || spec.Hostname != hashed("my_worker") {
		t.Errorf("a service name that is not a host label: hostname %q, %v; want %q", spec.Hostname, err,
			hashed("my_worker"))
	}
}

func TestSpecIsIdenticalForEveryStart(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, string(golden(t, "5.5.1")))
	img := compose.Image{ID: imageID, Env: []string{"HTTP_PROXY=x", "PATH=/bin"}, Volumes: []string{"/data"}}

	first, err := model.Spec(target, img, compose.CAEnvironment())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}

	second, err := model.Spec(target, img, compose.CAEnvironment())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}

	if !reflect.DeepEqual(first, second) || fmt.Sprintf("%v", first) != fmt.Sprintf("%v", second) {
		t.Errorf("two starts built different specs:\n%v\n%v", first, second)
	}

	if first.Image != img.ID || first.Service != target || first.Resources.MemLimit != 536870912 {
		t.Errorf("spec identity = %v", first)
	}
}

func TestEnvironmentIsImageOverlaidByCompose(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {"api": {"environment": {
		"NATS_URL": "tls://bus:4222", "P": "pa$$word", "KEPT": null
	}}}}`)
	img := compose.Image{ID: imageID, Env: []string{"NATS_URL=nats://a:4222", "KEPT=image", "ONLY=image"}}

	env := model.Environment(target, img)
	want := map[string]string{"NATS_URL": "tls://bus:4222", "P": "pa$word", "KEPT": "image", "ONLY": "image"}

	if !maps.Equal(env, want) {
		t.Errorf("Environment = %v, want %v", env, want)
	}

	env["NATS_URL"] = "changed"

	if again := model.Environment(target, img); again["NATS_URL"] != "tls://bus:4222" {
		t.Errorf("a second Environment saw the first's mutation: %v", again)
	}
}
