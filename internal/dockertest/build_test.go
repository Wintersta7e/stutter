package dockertest_test

import (
	"debug/buildinfo"
	"debug/elf"
	"testing"

	"github.com/Wintersta7e/stutter/internal/dockertest"
)

// machines maps the engine's architecture, in Go's naming, to the ELF machine a binary for it has.
var machines = map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}

func TestTheBuildHelperMakesAStaticBinary(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)
	path := engine.Binary(t, "./cmd/stutter")

	f, err := elf.Open(path)
	if err != nil {
		t.Fatalf("the built binary is not ELF: %v", err)
	}

	defer func() { _ = f.Close() }()

	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			t.Errorf("%s names an ELF interpreter: it is dynamically linked", path)
		}
	}

	if want, ok := machines[engine.Arch()]; !ok || f.Machine != want {
		t.Errorf("%s is for %s; the engine runs %s", path, f.Machine, engine.Arch())
	}

	info, err := buildinfo.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the build info: %v", err)
	}

	settings := map[string]string{}
	for _, s := range info.Settings {
		settings[s.Key] = s.Value
	}

	if settings["CGO_ENABLED"] != "0" || settings["-trimpath"] != "true" {
		t.Errorf("built with CGO_ENABLED=%q -trimpath=%q; want 0 and true", settings["CGO_ENABLED"],
			settings["-trimpath"])
	}

	if again := engine.Binary(t, "./cmd/stutter"); again != path {
		t.Errorf("a second call built again: %s then %s", path, again)
	}
}
