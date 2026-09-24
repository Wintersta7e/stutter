package compose_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// execForm opens an exec-form healthcheck test.
const execForm = "CMD"

// escapedService is a service as `docker compose config --format json` prints it: every literal `$`
// doubled, as compose itself escapes its output. Every accessor must hand a container one `$`.
const escapedService = `{"name": "shop", "services": {"api": {
	"image": "example.test/a$$b:1",
	"platform": "linux/$$p",
	"user": "u$$",
	"domainname": "d$$",
	"mac_address": "m$$",
	"ipc": "i$$",
	"cgroup": "c$$",
	"cgroup_parent": "p$$",
	"cpuset": "0$$",
	"sysctls": {"net.core.somaxconn": "v$$"},
	"security_opt": ["label=type:$$t"],
	"cap_add": ["C$$"],
	"cap_drop": ["D$$"],
	"group_add": ["g$$"],
	"storage_opt": {"size": "1$$"},
	"blkio_config": {"weight_device": [{"path": "/dev/w$$", "weight": 10}],
		"device_read_bps": [{"path": "/dev/r$$", "rate": 1}]},
	"stop_signal": "SIG$$X",
	"healthcheck": {"test": ["CMD", "test", "-f", "/tmp/it's a \"b c\" $$HOME", "$$$$"]}
}, "db": {"image": "postgres",
	"healthcheck": {"test": ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER}"]}
}}}`

// TestAHealthcheckIsUnescapedOnce hands the engine the test compose meant: a `$` the shell expands in
// the container, never `$$`, which the shell reads as its own process ID.
func TestAHealthcheckIsUnescapedOnce(t *testing.T) {
	t.Parallel()

	model := modelFrom(t, escapedService)

	for service, want := range map[string][]string{
		target: {execForm, "test", "-f", `/tmp/it's a "b c" $HOME`, "$$"},
		"db":   {"CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"},
	} {
		got, _, err := model.Healthcheck(service)
		if err != nil {
			t.Fatalf("Healthcheck(%s): %v", service, err)
		}

		if !slices.Equal(got.Test, want) {
			t.Errorf("Healthcheck(%s).Test = %q, want %q", service, got.Test, want)
		}
	}
}

// TestAStopSignalIsUnescapedOnce reads stop_signal as a container sees it.
func TestAStopSignalIsUnescapedOnce(t *testing.T) {
	t.Parallel()

	stop, err := modelFrom(t, escapedService).Stop(target)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if stop.Signal != "SIG$X" {
		t.Errorf("Stop().Signal = %q, want %q", stop.Signal, "SIG$X")
	}
}

// TestEverySpecStringIsUnescapedOnce covers each string a spec takes from the model that the
// environment, argv, working directory and hostname cases leave out.
func TestEverySpecStringIsUnescapedOnce(t *testing.T) {
	t.Parallel()

	spec := specOf(t, escapedService, compose.Image{ID: imageID})
	blkio := spec.Resources.Blkio

	type check struct{ name, got, want string }

	checks := make([]check, 0, 16)
	checks = append(checks, []check{
		{name: "platform", got: spec.Platform, want: "linux/$p"},
		{name: "user", got: spec.User, want: "u$"},
		{name: "domainname", got: spec.Domainname, want: "d$"},
		{name: "mac_address", got: spec.MacAddress, want: "m$"},
		{name: "ipc", got: spec.IPC, want: "i$"},
		{name: "spec.Cgroup", got: spec.Cgroup, want: "c$"},
		{name: "cgroup_parent", got: spec.Resources.CgroupParent, want: "p$"},
		{name: "cpuset", got: spec.Resources.Cpuset, want: "0$"},
		{name: "sysctls", got: spec.Sysctls["net.core.somaxconn"], want: "v$"},
		{name: "storage_opt", got: spec.Resources.StorageOpt["size"], want: "1$"},
		{name: "blkio weight_device", got: blkio.WeightDevice[0].Path, want: "/dev/w$"},
		{name: "blkio device_read_bps", got: blkio.DeviceReadBps[0].Path, want: "/dev/r$"},
	}...)

	for _, list := range []struct {
		name string
		want string
		got  []string
	}{
		{name: "security_opt", got: spec.SecurityOpt, want: "label=type:$t"},
		{name: "spec.CapAdd", got: spec.CapAdd, want: "C$"},
		{name: "cap_drop", got: spec.CapDrop, want: "D$"},
		{name: "group_add", got: spec.GroupAdd, want: "g$"},
	} {
		checks = append(checks, check{name: list.name, got: firstOf(list.got), want: list.want})
	}

	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %q, want %q", check.name, check.got, check.want)
		}
	}
}

// firstOf is a list's first element, or "" for an empty one.
func firstOf(values []string) string {
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

// TestAnImageReferenceIsUnescapedOnce resolves the reference compose meant.
func TestAnImageReferenceIsUnescapedOnce(t *testing.T) {
	t.Parallel()

	refs := modelFrom(t, escapedService).Images([]string{target})
	if len(refs) != 1 || refs[0].Ref != "example.test/a$b:1" || refs[0].Platform != "linux/$p" {
		t.Errorf("Images() = %+v, want the reference example.test/a$b:1 on linux/$p", refs)
	}
}

// TestTheEnvironmentIsUnescapedOnce reads the merged environment as a container sees it.
func TestTheEnvironmentIsUnescapedOnce(t *testing.T) {
	t.Parallel()

	env := modelFrom(t, `{"name": "shop", "services": {"api": {"environment": {"A": "x$$y", "B": "$$$$"}}}}`).
		Environment(target, compose.Image{ID: imageID})

	if want := map[string]string{"A": "x$y", "B": "$$"}; !maps.Equal(env, want) {
		t.Errorf("Environment() = %v, want %v", env, want)
	}
}
