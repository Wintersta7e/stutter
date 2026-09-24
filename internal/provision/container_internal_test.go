//go:build linux

package provision

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"maps"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

const (
	// proxyName is a variable a user's client configuration injects.
	proxyName = "HTTP_PROXY"
	// hostPath is a path a writable bind would expose.
	hostPath = "/host"
	// pathVar is the variable every constructed environment carries.
	pathVar = "PATH"
)

// containerFixture opens an engine over a fake with a pinned image declaring a VOLUME and one
// network, and returns a target spec that creates cleanly.
func containerFixture(t *testing.T) (*Engine, *fakeEngine, ContainerSpec) {
	t.Helper()

	engine, fake := openFakeEngine(t)
	fake.add(ResourceImage, &fakeObject{
		tags: []string{"app:1"}, labels: map[string]string{},
		volumes: map[string]any{"/var/lib/postgresql": map[string]any{}},
	})

	image, err := engine.ResolveImage(t.Context(), "app:1", "")
	if err != nil {
		t.Fatal(err)
	}

	network, err := engine.CreateNetwork(t.Context(), "service", true, netip.MustParsePrefix("10.231.20.0/24"))
	if err != nil {
		t.Fatal(err)
	}

	return engine, fake, ContainerSpec{
		Kind: rules.KindTarget, Service: testService,
		Spec:     compose.Spec{Image: image.ID, Env: map[string]string{"MODE": "test"}},
		Networks: []NetworkAttach{{Network: network, Aliases: []string{testService}}},
	}
}

// createCalls returns every create the fake answered.
func createCalls(fake *fakeEngine) []fakeCall {
	var out []fakeCall

	for _, c := range fake.calls {
		if c.verb == "create" {
			out = append(out, c)
		}
	}

	return out
}

// Environment values never reach argv: single-line ones travel on stdin, a multi-line one only in
// the child's environment.
func TestNoEnvironmentValueIsEverInArgv(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	single := map[string]string{"A": "sentinel-7f3a", "B": "two words 9c1e", "EMPTY": ""}
	multi := "line-one-4d2b\nline-two-8e5f"

	spec.Spec.Env = map[string]string{"ML": multi}
	maps.Copy(spec.Spec.Env, single)

	if _, err := engine.CreateContainer(t.Context(), spec); err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	creates := createCalls(fake)
	if len(creates) != 1 {
		t.Fatalf("creates = %d", len(creates))
	}

	argv := strings.Join(creates[0].argv, "\x00")

	for _, value := range []string{"sentinel-7f3a", "two words 9c1e", "line-one-4d2b", "line-two-8e5f"} {
		if strings.Contains(argv, value) {
			t.Errorf("a value reached argv: %q", value)
		}
	}

	for key, value := range single {
		if !strings.Contains(creates[0].stdin, key+"="+value+"\n") {
			t.Errorf("%s did not travel on stdin", key)
		}
	}

	if strings.Contains(creates[0].stdin, "line-one") || !slices.Contains(creates[0].env, "ML="+multi) {
		t.Errorf("the multi-line value is not in the child environment only: stdin %q env %q", creates[0].stdin,
			creates[0].env)
	}

	t.Logf("argv tokens=%d sentinels=4 found=0", len(creates[0].argv))
}

// A constructed-mode call carries exactly PATH, the pinned endpoint and the private client
// configuration, plus a create's multi-line variables: nothing of the user's environment, so a
// client config's proxies never reach a container.
func TestConstructedModeCarriesOnlyItsOwnVariables(t *testing.T) {
	t.Parallel()

	env := shimEnv(t, "env\n", proxyName+"=http://proxy.invalid:3128", "https_proxy=http://proxy.invalid:3128",
		"DOCKER_CONTEXT=elsewhere", "HOME=/nonexistent")
	runner := newRunner(env, nil)
	runner.pin = "unix:///pinned.sock"
	runner.attach("/private/docker-config", nil, true)

	res, err := runner.call(t.Context(), request{verb: verbCreate, extraEnv: []string{"ML=a\nb"}})
	if err != nil {
		t.Fatalf("call: %v", err)
	}

	var got []string

	for line := range strings.Lines(string(res.out)) {
		if name, _, ok := strings.Cut(line, "="); ok {
			got = append(got, name)
		}
	}

	slices.Sort(got)

	want := []string{"DOCKER_CONFIG", "DOCKER_HOST", "ML", pathVar}
	if !slices.Equal(got, want) {
		t.Errorf("the constructed environment holds %v, want exactly %v", got, want)
	}
}

// Unsetting is `-e NAME` with the name absent from the child's environment — never `NAME=`, which
// sets it empty, and never a bare env-file line, which the CLI drops.
func TestAnUnsetNameIsPassedBare(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	spec.Spec.Unset = []string{proxyName, "no_proxy"}

	if _, err := engine.CreateContainer(t.Context(), spec); err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	create := createCalls(fake)[0]

	for _, name := range spec.Spec.Unset {
		if !slices.Contains(flagValues(create.argv, "-e"), name) {
			t.Errorf("%s is not passed as a bare -e", name)
		}

		setSomewhere := slices.Contains(create.argv, name+"=") || strings.Contains(create.stdin, name) ||
			slices.ContainsFunc(create.env, func(v string) bool { return strings.HasPrefix(v, name+"=") })
		if setSomewhere {
			t.Errorf("%s is set somewhere it must not be", name)
		}
	}
}

// A multi-line key travels in the child's environment, so it must not be one of the names the
// constructed environment itself carries, nor steer the CLI.
func TestAMultiLineKeyThatCollidesIsRefused(t *testing.T) {
	t.Parallel()

	for _, key := range []string{pathVar, "DOCKER_HOST", "DOCKER_CONFIG", "DOCKER_TLS_VERIFY"} {
		engine, fake, spec := containerFixture(t)
		spec.Spec.Env = map[string]string{key: "a\nb"}

		_, err := engine.CreateContainer(t.Context(), spec)
		if !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "a\nb") {
			t.Errorf("%s: CreateContainer = %v, want ErrRefused naming the key only", key, err)
		}

		if len(createCalls(fake)) != 0 {
			t.Errorf("%s: a create was issued", key)
		}
	}

	engine, _, spec := containerFixture(t)
	spec.Spec.Env, spec.Spec.Unset = map[string]string{proxyName: "x"}, []string{proxyName}

	if _, err := engine.CreateContainer(t.Context(), spec); !errors.Is(err, ErrRefused) {
		t.Errorf("a name both set and unset = %v, want ErrRefused", err)
	}
}

// A missing bind source would be created by the engine as a root-owned directory on the user's
// disk: it is refused before any create, and is still absent afterwards.
func TestAMissingBindSourceIsRefusedBeforeCreate(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	missing := filepath.Join(t.TempDir(), "absent")
	spec.Spec.Mounts = []compose.Mount{{Kind: compose.MountBind, Source: missing, Target: "/src"}}

	if _, err := engine.CreateContainer(t.Context(), spec); !errors.Is(err, ErrRefused) {
		t.Errorf("CreateContainer = %v, want ErrRefused", err)
	}

	if creates := createCalls(fake); len(creates) != 0 {
		t.Errorf("a create was issued for a missing bind source: %v", creates[0].argv)
	}

	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the missing bind source now exists: %v", err)
	}
}

// A directory holding a socket reaches whatever serves it — the engine's own API among them — even
// bound read-only: refused before any create.
func TestASocketDirectoryBindIsRefusedBeforeCreate(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	dir := t.TempDir()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", filepath.Join(dir, "api.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeListener(t, listener)

	spec.Spec.Mounts = []compose.Mount{{Kind: compose.MountBind, Source: dir, Target: "/src"}}

	if _, err := engine.CreateContainer(t.Context(), spec); !errors.Is(err, ErrRefused) {
		t.Errorf("CreateContainer = %v, want ErrRefused", err)
	}

	if len(createCalls(fake)) != 0 {
		t.Error("a create was issued for a directory holding a socket")
	}
}

func closeListener(t *testing.T, listener net.Listener) {
	t.Helper()

	if err := listener.Close(); err != nil {
		t.Log(err)
	}
}

// Every image VOLUME no mount covers gets an explicit anonymous volume carrying this check's labels,
// so no container of the check ever holds an unlabelled volume.
func TestEveryImageVolumeIsCoveredWithALabelledVolume(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)

	container, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}

	mounts := strings.Join(flagValues(createCalls(fake)[0].argv, "--mount"), " ")
	for _, want := range []string{
		"type=volume", "dst=/var/lib/postgresql", "volume-label=" + rules.LabelCheck + "=" + engine.id,
		"volume-label=" + rules.LabelKind + "=target",
	} {
		if !strings.Contains(mounts, want) {
			t.Errorf("the image VOLUME is not covered with %q: %s", want, mounts)
		}
	}

	if children := engine.book.children(container.seq); len(children) != 1 {
		t.Errorf("anonymous volumes ledgered against the container = %d, want 1", len(children))
	}
}

// flagValues returns every value given to flag in argv.
func flagValues(argv []string, flag string) []string {
	var out []string

	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == flag {
			out = append(out, argv[i+1])
		}
	}

	return out
}

// A container found holding a volume without this check's labels is a class defect: it is removed
// before it ever starts, and the failure names the path.
func TestAContainerHoldingAnUnlabelledVolumeIsRemovedUnstarted(t *testing.T) {
	t.Parallel()

	const unlabelled = "unlabelled"

	engine, fake, spec := containerFixture(t)
	fake.add(ResourceVolume, &fakeObject{id: unlabelled, name: unlabelled, labels: map[string]string{}})
	fake.inspectHook = func(r *containerReport) {
		r.Mounts = append(r.Mounts, mountReport{
			Type: mountVolume, Name: unlabelled, Destination: "/uncovered", RW: true,
		})
	}

	_, err := engine.CreateContainer(t.Context(), spec)
	if !errors.Is(err, ErrUnlabelledVolume) || !strings.Contains(err.Error(), "/uncovered") {
		t.Fatalf("CreateContainer = %v, want ErrUnlabelledVolume naming the path", err)
	}

	if removes := fake.verbCalls(removeVerb); len(removes) != 1 {
		t.Errorf("rm calls = %v, want the container removed", removes)
	}

	if len(fake.objects[ResourceContainer]) != 0 {
		t.Error("the container remains")
	}

	if starts := fake.verbCalls("start"); len(starts) != 0 {
		t.Errorf("the container was started: %v", starts)
	}
}

// Every property the post-create verification reads is checked: a container whose labels are not
// this check's is never removed; one that fails any other property is removed and named.
func TestPostCreateVerificationChecksEveryProperty(t *testing.T) {
	t.Parallel()

	cases := []struct {
		edit    func(r *containerReport)
		name    string
		want    string
		removed bool
	}{
		{name: "1 labels", want: "lacks", edit: func(r *containerReport) {
			r.Labels = map[string]string{"owner": "someone"}
		}},
		{name: "2 restart", removed: true, want: "restart", edit: func(r *containerReport) { r.Restart = "always" }},
		{name: "2 log driver", removed: true, want: "log driver", edit: func(r *containerReport) {
			r.LogDriver = "json-file"
		}},
		{name: "3 privileged", removed: true, want: "privileged", edit: func(r *containerReport) {
			r.Privileged = true
		}},
		{name: "3 devices", removed: true, want: "device", edit: func(r *containerReport) {
			r.Devices = []string{"/dev/kvm"}
		}},
		{name: "3 cap_add", removed: true, want: "SYS_ADMIN", edit: func(r *containerReport) {
			r.CapAdd = []string{"SYS_ADMIN"}
		}},
		{name: "3 pid", removed: true, want: "pid", edit: func(r *containerReport) { r.PidMode = "host" }},
		{name: "4 writable bind", removed: true, want: hostPath, edit: func(r *containerReport) {
			r.Mounts = append(r.Mounts, mountReport{Type: mountBind, Source: hostPath, Destination: hostPath, RW: true})
		}},
		{name: "4 other type", removed: true, want: "npipe", edit: func(r *containerReport) {
			r.Mounts = append(r.Mounts, mountReport{Type: "npipe", Destination: "/pipe"})
		}},
		{name: "5 foreign network", removed: true, want: "bridge", edit: func(r *containerReport) {
			r.Networks = append(r.Networks, endpointReport{Name: "bridge"})
		}},
		{name: "6 public port", removed: true, want: "0.0.0.0", edit: func(r *containerReport) {
			r.Bindings = []portReport{{Port: "80/tcp", HostIP: "0.0.0.0"}}
		}},
		{name: "6 publish all", removed: true, want: "publish", edit: func(r *containerReport) { r.PublishAll = true }},
		{name: "4 read-only volume written", removed: true, want: "read-only", edit: func(r *containerReport) {
			for i := range r.Mounts {
				if r.Mounts[i].Destination == "/seed" {
					r.Mounts[i].RW = true
				}
			}
		}},
	}

	for _, tc := range cases {
		engine, fake, spec := containerFixture(t)

		volume, err := engine.CreateVolume(t.Context(), rules.KindTemplateVolume, "db")
		if err != nil {
			t.Fatal(err)
		}

		spec.Volumes = []VolumeMount{{Volume: volume, Target: "/seed", ReadOnly: true}}
		fake.inspectHook = tc.edit

		_, createErr := engine.CreateContainer(t.Context(), spec)
		if createErr == nil || !strings.Contains(createErr.Error(), tc.want) {
			t.Errorf("%s: CreateContainer = %v, want a failure naming %q", tc.name, createErr, tc.want)
		}

		if removed := len(fake.verbCalls(removeVerb)) > 0; removed != tc.removed {
			t.Errorf("%s: removed = %v, want %v", tc.name, removed, tc.removed)
		}
	}

	t.Logf("properties=%d", len(cases))
}

// Only a volume-copy helper may run with no network; every other container joins at least one of
// the check's networks, so none lands on the engine's default bridge.
func TestOnlyAHelperMayHaveNoNetwork(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	spec.Networks, spec.NoNetwork = nil, true

	if _, err := engine.CreateContainer(t.Context(), spec); !errors.Is(err, ErrRefused) {
		t.Errorf("a target with no network = %v, want ErrRefused", err)
	}

	spec.NoNetwork = false
	if _, err := engine.CreateContainer(t.Context(), spec); !errors.Is(err, ErrRefused) {
		t.Errorf("a target with no network at all = %v, want ErrRefused", err)
	}

	spec.Kind, spec.NoNetwork = rules.KindHelper, true

	if _, err := engine.CreateContainer(t.Context(), spec); err != nil {
		t.Errorf("a helper with no network = %v", err)
	}

	if networks := flagValues(createCalls(fake)[0].argv, "--network"); len(networks) != 1 || networks[0] != "none" {
		t.Errorf("the helper's networks = %v, want only none", networks)
	}
}

// A container is created only from an image this check pinned: a tag moved mid-check can never
// change what runs.
func TestAnImageThisEngineDidNotPinIsRefused(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	spec.Spec.Image = "sha256:" + strings.Repeat("9", 64)

	if _, err := engine.CreateContainer(t.Context(), spec); !errors.Is(err, ErrRefused) {
		t.Errorf("CreateContainer = %v, want ErrRefused", err)
	}

	if len(createCalls(fake)) != 0 {
		t.Error("a create was issued from an unpinned image")
	}
}

// Every resource key compose honours reaches create as its flag: a field added to the compose model
// without one fails here, naming it.
func TestEveryResourceFieldHasAFlag(t *testing.T) {
	t.Parallel()

	typ := reflect.TypeFor[compose.Resources]()
	checked := 0

	for field := range typ.Fields() {
		flag, known := resourceFlag(field.Name)
		if !known {
			t.Errorf("compose.Resources.%s has no flag", field.Name)

			continue
		}

		if flag == "" {
			continue
		}

		var resources compose.Resources

		setNonZero(t, reflect.ValueOf(&resources).Elem().FieldByIndex(field.Index))

		args := argsOf(resourceArgs(resources))
		if !slices.Contains(args, flag) {
			t.Errorf("compose.Resources.%s set renders %v, not %s", field.Name, args, flag)
		}

		checked++
	}

	t.Logf("fields=%d flags=%d", typ.NumField(), checked)
}

func argsOf(args []arg) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		out = append(out, a.val)
	}

	return out
}

// setNonZero gives a field of any resource type a non-zero value.
func setNonZero(t *testing.T, v reflect.Value) {
	t.Helper()

	scalars := map[reflect.Kind]func(){
		reflect.String:  func() { v.SetString("1") },
		reflect.Int64:   func() { v.SetInt(7) },
		reflect.Int:     func() { v.SetInt(7) },
		reflect.Uint16:  func() { v.SetUint(7) },
		reflect.Float64: func() { v.SetFloat(1.5) },
		reflect.Bool:    func() { v.SetBool(true) },
		reflect.Map: func() {
			v.Set(reflect.MakeMap(v.Type()))
			v.SetMapIndex(reflect.ValueOf("nofile"), reflect.Zero(v.Type().Elem()))
		},
	}

	if set, ok := scalars[v.Kind()]; ok {
		set()

		return
	}

	if v.Kind() == reflect.Pointer {
		v.Set(reflect.New(v.Type().Elem()))
		setNonZero(t, v.Elem())

		return
	}

	if v.Kind() == reflect.Slice {
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		setNonZero(t, v.Index(0))

		return
	}

	if v.Kind() != reflect.Struct {
		t.Fatalf("no non-zero value for %s", v.Kind())
	}

	for _, field := range v.Fields() {
		setNonZero(t, field)
	}
}

// The CLI cannot set a multi-element entrypoint: its first element becomes --entrypoint and the rest
// precede the command as arguments, giving the process the same argv.
func TestAMultiElementEntrypointKeepsItsArgv(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	spec.Spec.Entrypoint, spec.Spec.EntrypointSet = []string{"/bin/sh", "-c"}, true
	spec.Spec.Cmd, spec.Spec.CmdSet = []string{"exit 7"}, true

	if _, err := engine.CreateContainer(t.Context(), spec); err != nil {
		t.Fatal(err)
	}

	argv := createCalls(fake)[0].argv
	if got := flagValues(argv, "--entrypoint"); !slices.Equal(got, []string{"/bin/sh"}) {
		t.Errorf("--entrypoint = %v, want /bin/sh", got)
	}

	if tail := argv[len(argv)-3:]; !slices.Equal(tail, []string{spec.Spec.Image, "-c", "exit 7"}) {
		t.Errorf("argv ends %v, want the image then -c, exit 7", tail)
	}
}

// An exec-form healthcheck is expressible only as a shell command: its arguments are quoted so the
// shell hands the process the same argv.
func TestAnExecFormHealthcheckRunsUnderTheShell(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	spec.Healthcheck = &Healthcheck{
		Test: []string{"CMD", "pg_isready", "-U", "it's me"}, Interval: 200 * time.Millisecond, Retries: 3,
	}

	if _, err := engine.CreateContainer(t.Context(), spec); err != nil {
		t.Fatal(err)
	}

	argv := createCalls(fake)[0].argv
	if got := flagValues(argv, "--health-cmd"); !slices.Equal(got, []string{`'pg_isready' '-U' 'it'\''s me'`}) {
		t.Errorf("--health-cmd = %q", got)
	}

	if got := flagValues(argv, "--health-interval"); !slices.Equal(got, []string{"200ms"}) {
		t.Errorf("--health-interval = %v", got)
	}

	spec.Healthcheck = nil

	if _, err := engine.CreateContainer(t.Context(), spec); err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(createCalls(fake)[1].argv, "--no-healthcheck") {
		t.Error("a nil healthcheck did not disable the image's")
	}
}

// A copy into a container's filesystem is a tar of regular files only, owned by root unless given,
// and never lands at or under a bind destination.
func TestACopyInIsFilesOnly(t *testing.T) {
	t.Parallel()

	engine, fake, spec := containerFixture(t)
	source := t.TempDir()
	spec.Spec.Mounts = []compose.Mount{{Kind: compose.MountBind, Source: source, Target: "/etc/app"}}

	container, err := engine.CreateContainer(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}

	files := []File{
		{Path: "ssl/ca.pem", Data: []byte("pem"), Mode: 0o644},
		{Path: "token", Data: []byte("secret"), Mode: 0o400, UID: 1000, GID: 1000},
	}

	if err := engine.CopyIn(t.Context(), container, "/etc", files); err != nil {
		t.Fatalf("CopyIn: %v", err)
	}

	if headers := tarHeaders(t, fake.find(ResourceContainer, container.ID()).copied); headers != len(files) {
		t.Errorf("the tar holds %d entries, want %d", headers, len(files))
	}

	for _, dir := range []string{"/etc/app", "/etc/app/conf.d"} {
		if err := engine.CopyIn(t.Context(), container, dir, files[:1]); !errors.Is(err, ErrRefused) {
			t.Errorf("CopyIn under the bind %s = %v, want ErrRefused", dir, err)
		}
	}

	if err := engine.CopyIn(t.Context(), container, "/etc", []File{{Path: "app/x"}}); !errors.Is(err, ErrRefused) {
		t.Errorf("a file landing under a bind destination = %v, want ErrRefused", err)
	}
}

// tarHeaders checks every entry of a copy-in tar is a regular file, a root-owned one unless the test
// gave it an owner, and counts them.
func tarHeaders(t *testing.T, data []byte) int {
	t.Helper()

	reader := tar.NewReader(bytes.NewReader(data))
	headers := 0

	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return headers
		}

		if err != nil {
			t.Fatal(err)
		}

		headers++

		if header.Typeflag != tar.TypeReg {
			t.Errorf("the tar holds a %c entry %s", header.Typeflag, header.Name)
		}

		if header.Name == "ssl/ca.pem" && (header.Uid != 0 || header.Gid != 0) {
			t.Errorf("%s is owned %d:%d, want 0:0", header.Name, header.Uid, header.Gid)
		}
	}
}
