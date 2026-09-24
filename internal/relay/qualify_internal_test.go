package relay

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The engine the table's rows are checked against, and the system a relay runs on.
const (
	amd64 = "amd64"
	linux = "linux"
)

// syntheticELF writes a minimal ELF64 executable: a header and one program header per entry of progs.
func syntheticELF(t *testing.T, machine elf.Machine, progs ...elf.ProgType) string {
	t.Helper()

	const (
		headerSize = 64
		progSize   = 56
	)

	header := elf.Header64{
		Type:      uint16(elf.ET_EXEC),
		Machine:   uint16(machine),
		Version:   uint32(elf.EV_CURRENT),
		Phoff:     headerSize,
		Ehsize:    headerSize,
		Phentsize: progSize,
		Phnum:     uint16(len(progs)), //nolint:gosec // a handful of program headers.
		Shentsize: 64,
	}
	copy(header.Ident[:], elf.ELFMAG)
	header.Ident[elf.EI_CLASS] = byte(elf.ELFCLASS64)
	header.Ident[elf.EI_DATA] = byte(elf.ELFDATA2LSB)
	header.Ident[elf.EI_VERSION] = byte(elf.EV_CURRENT)

	var file bytes.Buffer

	if err := binary.Write(&file, binary.LittleEndian, header); err != nil {
		t.Fatalf("write the ELF header: %v", err)
	}

	for _, prog := range progs {
		progType := uint32(prog) //nolint:gosec // program header types are small constants.
		if err := binary.Write(&file, binary.LittleEndian, elf.Prog64{Type: progType, Align: 1}); err != nil {
			t.Fatalf("write a program header: %v", err)
		}
	}

	path := filepath.Join(t.TempDir(), "stutter")
	if err := os.WriteFile(path, file.Bytes(), 0o600); err != nil {
		t.Fatalf("write the ELF file: %v", err)
	}

	return path
}

// TestQualifyNamesEachCause is the static-self check's whole contract: every reason a binary cannot
// run in a relay container is refused before any resource exists, each with its own error naming what
// to change.
func TestQualifyNamesEachCause(t *testing.T) {
	t.Parallel()

	static := syntheticELF(t, elf.EM_X86_64, elf.PT_LOAD)
	dynamic := syntheticELF(t, elf.EM_X86_64, elf.PT_LOAD, elf.PT_INTERP)
	arm := syntheticELF(t, elf.EM_AARCH64, elf.PT_LOAD)

	notELF := filepath.Join(t.TempDir(), "script")
	if err := os.WriteFile(notELF, []byte("#!/bin/sh\n"), 0o600); err != nil {
		t.Fatalf("write the script: %v", err)
	}

	cases := []struct {
		want     error
		name     string
		goos     string
		file     string
		engineOS string
		arch     string
		mentions []string
	}{
		{name: "a static ELF", goos: linux, file: static, engineOS: linux, arch: amd64},
		{
			name: "non-linux build", goos: "darwin", file: static, engineOS: linux, arch: amd64,
			want: ErrNotLinux, mentions: []string{"Linux build", "WSL2", "darwin"},
		},
		{
			name: "not ELF", goos: linux, file: notELF, engineOS: linux, arch: amd64,
			want: ErrNotELF, mentions: []string{notELF},
		},
		{
			name: "dynamic ELF", goos: linux, file: dynamic, engineOS: linux, arch: amd64,
			want: ErrDynamic, mentions: []string{"CGO_ENABLED=0"},
		},
		{
			name: "wrong machine", goos: linux, file: arm, engineOS: linux, arch: amd64,
			want: ErrArch, mentions: []string{"arm64", amd64},
		},
		{
			name: "engine OS", goos: linux, file: static, engineOS: "windows", arch: amd64,
			want: ErrEngineOS, mentions: []string{"windows", "Linux containers"},
		},
	}

	for _, testCase := range cases {
		err := qualifyFor(testCase.goos, testCase.file, testCase.engineOS, testCase.arch)
		if !errors.Is(err, testCase.want) || (testCase.want == nil) != (err == nil) {
			t.Errorf("%s: err = %v, want %v", testCase.name, err, testCase.want)

			continue
		}

		for _, mention := range testCase.mentions {
			if !strings.Contains(err.Error(), mention) {
				t.Errorf("%s: %q does not name %q", testCase.name, err, mention)
			}
		}
	}

	t.Logf("static-self causes checked: %d", len(cases))
}

// TestQualifyRefusesARealDynamicBinary runs the check on a real dynamic executable the host already
// has, so the ELF reading is proven against a file no test wrote.
func TestQualifyRefusesARealDynamicBinary(t *testing.T) {
	t.Parallel()

	const shell = "/bin/sh"

	file, err := elf.Open(shell)
	if err != nil {
		t.Fatalf("open %s as ELF: %v", shell, err)
	}

	interpreted := false

	for _, prog := range file.Progs {
		interpreted = interpreted || prog.Type == elf.PT_INTERP
	}

	_ = file.Close()

	if !interpreted {
		t.Fatalf("%s has no PT_INTERP header, so it proves nothing here: the host needs a dynamic %s", shell, shell)
	}

	err = Qualify(shell, linux, runtime.GOARCH)
	if !errors.Is(err, ErrDynamic) || !strings.Contains(err.Error(), "CGO_ENABLED=0") {
		t.Errorf("Qualify(%s) = %v, want ErrDynamic naming CGO_ENABLED=0", shell, err)
	}
}
