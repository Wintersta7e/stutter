package compose_test

import (
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// cache is a mount target several cases use.
const cache = "/cache"

func mountsOf(spec compose.Spec, kind compose.MountKind) []compose.Mount {
	var out []compose.Mount

	for _, mount := range spec.Mounts {
		if mount.Kind == kind {
			out = append(out, mount)
		}
	}

	return out
}

func TestEveryBindIsReadOnly(t *testing.T) {
	t.Parallel()

	spec := specOf(t, `{"name": "shop", "services": {"api": {
		"volumes": [
			{"type": "bind", "source": "/project/conf", "target": "/etc/app", "read_only": true,
				"bind": {"create_host_path": true}},
			{"type": "bind", "source": "/project/data", "target": "/data"},
			{"type": "bind", "source": "/project/src", "target": "/src$$", "read_only": true,
				"bind": {"recursive": "disabled"}}
		],
		"configs": [{"source": "c_file", "target": "/etc/app.conf"}],
		"secrets": [{"source": "s_file"}]
	}},
	"configs": {"c_file": {"file": "/project/c.conf"}},
	"secrets": {"s_file": {"file": "/project/s.txt"}}}`, compose.Image{ID: imageID})

	want := []compose.Mount{
		{Kind: compose.MountBind, Source: "/project/conf", Target: "/etc/app", ReadOnly: true},
		{Kind: compose.MountBind, Source: "/project/data", Target: "/data", ReadOnly: true},
		{Kind: compose.MountBind, Source: "/project/src", Target: "/src$", ReadOnly: true},
		{Kind: compose.MountBind, Source: "/project/c.conf", Target: "/etc/app.conf", ReadOnly: true},
		{Kind: compose.MountBind, Source: "/project/s.txt", Target: "/run/secrets/s_file", ReadOnly: true},
	}
	if got := mountsOf(spec, compose.MountBind); !slices.Equal(got, want) {
		t.Errorf("binds =\n%+v\nwant\n%+v", got, want)
	}

	for _, key := range []string{"volumes[1]", "volumes[2].bind.recursive"} {
		if !slices.ContainsFunc(spec.Replaced, func(r compose.Replaced) bool { return r.Key == key }) {
			t.Errorf("Replaced = %+v, want %s named", spec.Replaced, key)
		}
	}

	if slices.ContainsFunc(spec.Replaced, func(r compose.Replaced) bool { return r.Key == "volumes[0]" }) {
		t.Errorf("Replaced = %+v names a bind compose already mounts read-only", spec.Replaced)
	}
}

func TestNamedAndAnonymousVolumesAreFresh(t *testing.T) {
	t.Parallel()

	spec := specOf(t, `{"name": "shop", "services": {"api": {
		"volumes": [
			{"type": "volume", "source": "data", "target": "/var/lib/data", "volume": {"nocopy": true}},
			{"type": "volume", "target": "/scratch", "read_only": true},
			{"type": "tmpfs", "target": "/run/tmp", "tmpfs": {"size": "1048576", "mode": 1023}}
		],
		"tmpfs": ["/tmp", "/cache:size=64m,mode=0700"]
	}}, "volumes": {"data": {"name": "shop_data"}}}`, compose.Image{ID: imageID})

	fresh := []compose.Mount{
		{Kind: compose.MountFresh, Target: "/var/lib/data", Volume: "data", NoCopy: true},
		{Kind: compose.MountFresh, Target: "/scratch", ReadOnly: true},
	}
	if got := mountsOf(spec, compose.MountFresh); !slices.Equal(got, fresh) {
		t.Errorf("fresh volumes =\n%+v\nwant\n%+v", got, fresh)
	}

	tmpfs := []compose.Mount{
		{Kind: compose.MountTmpfs, Target: "/run/tmp", Size: 1048576, Mode: 0o1777},
		{Kind: compose.MountTmpfs, Target: "/tmp"},
		{Kind: compose.MountTmpfs, Target: cache, Size: 64 << 20, Mode: 0o700},
	}
	if got := mountsOf(spec, compose.MountTmpfs); !slices.Equal(got, tmpfs) {
		t.Errorf("tmpfs =\n%+v\nwant\n%+v", got, tmpfs)
	}

	for _, mount := range spec.Mounts {
		if strings.Contains(mount.Source, "shop_data") || strings.Contains(mount.Volume, "shop_data") {
			t.Errorf("mount %+v names the user's own volume", mount)
		}
	}

	for _, key := range []string{"volumes[0]", "volumes[1]"} {
		if !slices.ContainsFunc(spec.Replaced, func(r compose.Replaced) bool { return r.Key == key }) {
			t.Errorf("Replaced = %+v, want the substituted %s named", spec.Replaced, key)
		}
	}
}

func TestEveryImageVolumeIsCovered(t *testing.T) {
	t.Parallel()

	img := compose.Image{ID: imageID, Volumes: []string{"/var/lib/data/", "/cache", "/logs"}}
	spec := specOf(t, `{"name": "shop", "services": {"api": {
		"volumes": [{"type": "volume", "source": "data", "target": "/var/lib/data"}],
		"tmpfs": ["/logs"]
	}}, "volumes": {"data": {"name": "shop_data"}}}`, img)

	want := []compose.Mount{
		{Kind: compose.MountFresh, Target: "/var/lib/data", Volume: "data"},
		{Kind: compose.MountFresh, Target: cache},
	}
	if got := mountsOf(spec, compose.MountFresh); !slices.Equal(got, want) {
		t.Errorf("fresh volumes =\n%+v\nwant\n%+v", got, want)
	}
}

func TestCopyInCarriesComposeOwnership(t *testing.T) {
	t.Parallel()

	for _, release := range releases {
		spec := specOf(t, string(golden(t, release)), compose.Image{ID: imageID})

		want := []struct {
			target, data string
			uid, gid     int
			mode         fs.FileMode
		}{
			{target: "/etc/app/content.conf", data: "line one $HOME\n", mode: 0o444},
			{target: "/etc/app/env.conf", data: "config-value", uid: 1000, gid: 1000, mode: 0o400},
			{target: "/run/secrets/token", data: "secret-value", uid: 1000, mode: 0o400},
		}

		if len(spec.CopyIn) != len(want) {
			t.Fatalf("compose %s: CopyIn = %v, want %d files", release, spec.CopyIn, len(want))
		}

		for index, file := range spec.CopyIn {
			w := want[index]
			if file.Target != w.target || string(file.Data()) != w.data || file.UID != w.uid || file.GID != w.gid ||
				file.Mode != w.mode {
				t.Errorf("compose %s: CopyIn[%d] = %v, want %+v", release, index, file, w)
			}
		}

		bind := compose.Mount{
			Kind: compose.MountBind, Source: "/project/conf/file.conf", Target: "/etc/app/file.conf", ReadOnly: true,
		}
		if !slices.Contains(spec.Mounts, bind) {
			t.Errorf("compose %s: mounts = %v, want the file: config bound read-only", release, spec.Mounts)
		}
	}
}

func TestAnAbsentSourcedVariableFailsOnlyTheSpecThatCopiesIt(t *testing.T) {
	t.Parallel()

	run := &fakeRun{whole: func([]string) ([]byte, int, error) { return golden(t, "5.5.1"), 0, nil }}

	model, err := compose.Parse(t.Context(), run.run, compose.Inputs{
		Service: target, Files: []string{declaring(t, t.TempDir(), "compose.yaml")},
	})
	if err != nil {
		t.Fatalf("Parse with the variables unset = %v, want the parse to succeed", err)
	}

	_, err = model.Spec(target, compose.Image{ID: imageID}, compose.CAEnvironment())
	if !errors.Is(err, compose.ErrModel) || !strings.Contains(err.Error(), "configs.c_env") ||
		!strings.Contains(err.Error(), "CONFIG_VALUE") {
		t.Errorf("Spec = %v, want ErrModel naming configs.c_env and CONFIG_VALUE", err)
	}

	if _, err := model.Spec("db", compose.Image{ID: imageID}, nil); err != nil {
		t.Errorf("Spec(db) = %v: a sibling's unset variable must not fail it", err)
	}
}
