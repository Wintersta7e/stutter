package compose_test

import (
	"cmp"
	"crypto/rand"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// faithful builds what a correct create of s on img yields when inspected: the image's environment
// overlaid by the passed one, each unset name bare, the argv split as the driver splits it, every
// mount as asked, and Stutter's fixed settings.
func faithful(s compose.Spec, img compose.Image) compose.Inspected {
	// A target container is the one whose spec carries the CA variables.
	target := s.Env[sslCertFile] == compose.CAMountPath
	env := map[string]string{}

	for _, entry := range img.Env {
		key, value, _ := strings.Cut(entry, "=")
		env[key] = value
	}

	maps.Copy(env, s.Env)

	for _, name := range s.Unset {
		delete(env, name)
	}

	entries := make([]string, 0, len(env)+len(s.Unset))
	for _, key := range slices.Sorted(maps.Keys(env)) {
		entries = append(entries, key+"="+env[key])
	}

	entries = append(entries, s.Unset...)

	argv := slices.Clone(img.Entrypoint)
	if s.EntrypointSet {
		argv = slices.Clone(s.Entrypoint)
	}

	switch {
	case s.CmdSet:
		argv = append(argv, s.Cmd...)
	case !s.EntrypointSet:
		argv = append(argv, img.Cmd...)
	default:
	}

	got := compose.Inspected{
		Labels:         maps.Clone(img.Labels),
		Image:          img.ID,
		User:           cmp.Or(s.User, img.User),
		WorkingDir:     cmp.Or(s.WorkingDir, img.WorkingDir),
		Hostname:       s.Hostname,
		Domainname:     s.Domainname,
		RestartPolicy:  "no",
		DefaultRuntime: "runc",
		LogDriver:      "local",
		Env:            entries,
		Healthcheck:    []string{"NONE"},
		CapAdd:         slices.Clone(s.CapAdd),
		IpcMode:        privateNamespace,
		CgroupnsMode:   privateNamespace,
	}

	if len(argv) > 0 {
		got.Entrypoint, got.Cmd = argv[:1], argv[1:]
	}

	if got.Labels == nil {
		got.Labels = map[string]string{}
	}

	got.Labels["io.stutter.check"] = "c1"

	for _, mount := range s.Mounts {
		switch mount.Kind {
		case compose.MountBind:
			got.Mounts = append(got.Mounts, compose.InspectedMount{
				Type: "bind", Source: mount.Source,
				Destination: mount.Target, ReadOnlyRecursive: true,
			})
		case compose.MountFresh:
			got.Mounts = append(got.Mounts, compose.InspectedMount{
				Type: "volume", Name: "v" + mount.Target,
				Destination: mount.Target, RW: !mount.ReadOnly,
			})
		case compose.MountTmpfs:
			got.Mounts = append(got.Mounts, compose.InspectedMount{Type: "tmpfs", Destination: mount.Target, RW: true})
		default:
		}
	}

	if target {
		got.Mounts = append(got.Mounts, compose.InspectedMount{Type: "bind", Destination: compose.CAMountPath})
	}

	return got
}

// auditModel carries a secret in PASS, an escaped value, an image proxy, a CA variable, and every
// mount kind.
const auditModel = `{"name": "shop", "services": {
	"api": {
		"entrypoint": ["/bin/app"],
		"environment": {"PASS": "pa$$word", "SSL_CERT_FILE": "/compose/ca.pem", "SECRET": "SENTINEL"},
		"volumes": [
			{"type": "bind", "source": "/project/conf", "target": "/etc/app"},
			{"type": "volume", "source": "data", "target": "/data"},
			{"type": "tmpfs", "target": "/run/tmp"}
		],
		"cap_add": ["NET_BIND_SERVICE"]
	},
	"migrate": {"command": ["migrate"]}
}, "volumes": {"data": {"name": "shop_data"}}}`

// audited is a model with a random secret, the image, and the target's spec.
type audited struct {
	model    *compose.Model
	sentinel string
	img      compose.Image
	spec     compose.Spec
}

func auditFixture(t *testing.T) audited {
	t.Helper()

	sentinel := rand.Text()
	model := modelFrom(t, strings.Replace(auditModel, "SENTINEL", sentinel, 1))
	img := compose.Image{
		ID:      "sha256:audited",
		Env:     []string{pathEntry, httpProxy + "=http://image-proxy.test:3128", "NODE_EXTRA_CA_CERTS=/image/ca.pem"},
		Cmd:     []string{"--default"},
		Volumes: []string{"/cache"},
	}

	spec, err := model.Spec(target, img, compose.CAEnvironment())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}

	return audited{model: model, sentinel: sentinel, img: img, spec: spec}
}

func TestAuditPassesAFaithfulContainer(t *testing.T) {
	t.Parallel()

	fixture := auditFixture(t)
	img, model, spec := fixture.img, fixture.model, fixture.spec

	checked, differ, err := compose.Audit(spec, img, faithful(spec, img))
	if err != nil || differ != 0 {
		t.Fatalf("Audit of a faithful target = %d differ, %v", differ, err)
	}

	t.Logf("checked=%d", checked)

	if checked == 0 {
		t.Fatal("checked=0")
	}

	job, err := model.Spec(jobService, img, nil)
	if err != nil {
		t.Fatalf("Spec(job): %v", err)
	}

	if _, differ, err := compose.Audit(job, img, faithful(job, img)); err != nil || differ != 0 {
		t.Errorf("Audit of a faithful job = %d differ, %v", differ, err)
	}
}

func TestAuditCatchesEachBreak(t *testing.T) {
	t.Parallel()

	fixture := auditFixture(t)
	sentinel, img, spec := fixture.sentinel, fixture.img, fixture.spec

	withEnv := func(entry string) func(*compose.Inspected) {
		return func(got *compose.Inspected) {
			name, _, _ := strings.Cut(entry, "=")
			got.Env = slices.DeleteFunc(
				got.Env,
				func(e string) bool { return e == name || strings.HasPrefix(e, name+"=") },
			)
			got.Env = append(got.Env, entry)
		}
	}

	cases := []struct {
		breakage func(*compose.Inspected)
		name     string
		names    string
	}{
		{name: "user-mode-create", names: httpProxy, breakage: withEnv(httpProxy + "=http://injected.test:3128")},
		{name: "unset-by-empty-value", names: httpProxy, breakage: withEnv(httpProxy + "=")},
		{
			name:     "unset-by-env-file-line",
			names:    "HTTP_PROXY",
			breakage: withEnv(httpProxy + "=http://image-proxy.test:3128"),
		},
		{name: "compose-ca-wins", names: sslCertFile, breakage: withEnv(sslCertFile + "=/compose/ca.pem")},
		{name: "extra variable", names: "EXTRA", breakage: withEnv("EXTRA=1")},
		{name: "rw bind", names: "/etc/app", breakage: func(got *compose.Inspected) { got.Mounts[0].RW = true }},
		{name: "missing volume", names: dataTarget, breakage: func(got *compose.Inspected) {
			got.Mounts = slices.Delete(got.Mounts, 1, 2)
		}},
		{name: "extra mount", names: "/extra", breakage: func(got *compose.Inspected) {
			got.Mounts = append(got.Mounts, compose.InspectedMount{Type: "volume", Destination: "/extra", RW: true})
		}},
		{name: "no CA mount", names: compose.CAMountPath, breakage: func(got *compose.Inspected) {
			got.Mounts = got.Mounts[:len(got.Mounts)-1]
		}},
		{name: "restart", breakage: func(got *compose.Inspected) { got.RestartPolicy = "always" }},
		{name: "healthcheck", breakage: func(got *compose.Inspected) {
			got.Healthcheck = []string{"CMD", "true"}
		}},
		{name: "ports", breakage: func(got *compose.Inspected) { got.PortBindings = 1 }},
		{name: "publish all", names: "publish", breakage: func(got *compose.Inspected) { got.PublishAllPorts = true }},
		{name: "extra hosts", names: "extra_hosts", breakage: func(got *compose.Inspected) {
			got.ExtraHosts = []string{"db:10.0.0.1"}
		}},
		{name: "privileged", breakage: func(got *compose.Inspected) { got.Privileged = true }},
		{name: "refused capability", names: "cap_add", breakage: func(got *compose.Inspected) {
			got.CapAdd = append(got.CapAdd, "SYS_ADMIN")
		}},
		{name: "pid host", names: pidKey, breakage: func(got *compose.Inspected) { got.PidMode = "host" }},
		{
			name:     "device",
			breakage: func(got *compose.Inspected) { got.Devices = []string{"/dev/fuse"} },
		},
		{name: "device request", names: "devices", breakage: func(got *compose.Inspected) { got.DeviceRequests = 1 }},
		{name: "runtime", breakage: func(got *compose.Inspected) { got.Runtime = "runsc" }},
		{name: "volumes from", names: "volumes_from", breakage: func(got *compose.Inspected) {
			got.VolumesFrom = []string{"db"}
		}},
		{name: "log driver", names: "log", breakage: func(got *compose.Inspected) { got.LogDriver = "json-file" }},
		{name: "compose label", names: "com.docker.compose.project", breakage: func(got *compose.Inspected) {
			got.Labels["com.docker.compose.project"] = "p1"
		}},
		{name: imageKey, breakage: func(got *compose.Inspected) { got.Image = "sha256:other" }},
		{name: hostnameKey, breakage: func(got *compose.Inspected) { got.Hostname = "abcdef012345" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := faithful(spec, img)
			tc.breakage(&got)

			names := tc.names
			if names == "" {
				names = tc.name
			}

			assertCaught(t, spec, img, got, names, sentinel)
		})
	}

	t.Run("unescape-dropped", func(t *testing.T) {
		t.Parallel()

		// What Spec yields with the unescape call removed: PASS still escaped, the raw environment
		// intact.
		broken := spec
		broken.Env = maps.Clone(spec.Env)
		broken.Env["PASS"] = "pa$$word"

		assertCaught(t, broken, img, faithful(broken, img), "PASS", sentinel)
	})
}

func assertCaught(t *testing.T, spec compose.Spec, img compose.Image, got compose.Inspected, names, sentinel string) {
	t.Helper()

	checked, differ, err := compose.Audit(spec, img, got)
	if !errors.Is(err, compose.ErrAudit) || differ == 0 || checked == 0 {
		t.Fatalf("Audit = checked %d, differ %d, %v; want the difference caught", checked, differ, err)
	}

	if !strings.Contains(err.Error(), names) {
		t.Errorf("error %q does not name %s", err, names)
	}

	if strings.Contains(err.Error(), sentinel) || strings.Contains(err.Error(), "image-proxy") {
		t.Errorf("error %q carries a value", err)
	}
}

func TestInheritedComposeLabelsPass(t *testing.T) {
	t.Parallel()

	fixture := auditFixture(t)
	img, spec := fixture.img, fixture.spec
	img.Labels = map[string]string{"com.docker.compose.project": "shop", "com.docker.compose.service": target}

	got := faithful(spec, img)
	if _, differ, err := compose.Audit(spec, img, got); err != nil || differ != 0 {
		t.Errorf("Audit with labels inherited from the image = %d differ, %v", differ, err)
	}

	got.Labels["com.docker.compose.service"] = "other"
	if _, _, err := compose.Audit(spec, img, got); !errors.Is(err, compose.ErrAudit) {
		t.Errorf("Audit with a compose label changed from the image's = %v, want ErrAudit", err)
	}
}

func TestAuditComparesTheWholeArgv(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, `{"name": "shop", "services": {
		"api": {"entrypoint": ["sh", "-c"], "command": ["run"]},
		"bare": {"entrypoint": ["/bin/app"], "command": null}
	}}`)
	img := compose.Image{ID: "sha256:argv", Cmd: []string{"--image-default"}}

	spec, err := model.Spec(target, img, compose.CAEnvironment())
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}

	split := faithful(spec, img)
	split.Entrypoint, split.Cmd = []string{"sh"}, []string{"-c", "run"}

	if _, differ, auditErr := compose.Audit(spec, img, split); auditErr != nil || differ != 0 {
		t.Errorf("the argv split across entrypoint and cmd = %d differ, %v; want it accepted", differ, auditErr)
	}

	dropped := faithful(spec, img)
	dropped.Cmd = nil

	if _, _, auditErr := compose.Audit(spec, img, dropped); !errors.Is(auditErr, compose.ErrAudit) {
		t.Errorf("an argv element dropped = %v, want ErrAudit", auditErr)
	}

	bare, err := model.Spec("bare", img, nil)
	if err != nil {
		t.Fatalf("Spec(bare): %v", err)
	}

	leaked := faithful(bare, img)
	leaked.Cmd = append(leaked.Cmd, img.Cmd...)

	if _, _, err := compose.Audit(bare, img, leaked); !errors.Is(err, compose.ErrAudit) {
		t.Errorf("the image CMD leaking past a compose entrypoint = %v, want ErrAudit", err)
	}
}

// TestAuditReadsTheImageDefaultsComposeLeftUnset expects the image's user and working directory on a
// container whose compose service sets neither — what the engine applies — and compose's own when set.
func TestAuditReadsTheImageDefaultsComposeLeftUnset(t *testing.T) {
	t.Parallel()

	img := compose.Image{ID: "sha256:defaults", User: "1234:1234", WorkingDir: "/srv", Cmd: []string{"/app"}}

	job, err := modelFrom(t, `{"name": "shop", "services": {"api": {}}}`).Spec(target, img, nil)
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}

	got := faithful(job, img)
	if got.User != img.User || got.WorkingDir != img.WorkingDir {
		t.Fatalf("the fixture made user %q and working_dir %q, want the image's", got.User, got.WorkingDir)
	}

	if _, differ, err := compose.Audit(job, img, got); err != nil || differ != 0 {
		t.Errorf("Audit of a container that inherited the image's defaults = %d differ, %v", differ, err)
	}

	got.WorkingDir, got.User = "/", ""

	if _, _, err := compose.Audit(job, img, got); !errors.Is(err, compose.ErrAudit) ||
		!strings.Contains(err.Error(), "user") || !strings.Contains(err.Error(), "working_dir") {
		t.Errorf("Audit of a container that lost the image's defaults = %v, want user and working_dir named", err)
	}
}
