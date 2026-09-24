package relay

import (
	"debug/elf"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
)

// The static-self check's causes, each its own error so the caller can say which one stopped it.
var (
	// ErrNotLinux means stutter was built for another system: the relay runs this same binary inside a
	// Linux container.
	ErrNotLinux = errors.New("--compose needs a Linux build of stutter; on Windows, run it inside WSL2")
	// ErrNotELF means the running executable could not be read as an ELF file.
	ErrNotELF = errors.New("the running executable is not a readable ELF file")
	// ErrDynamic means the running executable needs a dynamic loader, which a relay container, holding
	// nothing but the binary, does not have.
	ErrDynamic = errors.New("the running executable is dynamically linked; build stutter with CGO_ENABLED=0")
	// ErrArch means the executable's architecture differs from the engine's, so the engine cannot run it.
	ErrArch = errors.New("the running executable's architecture differs from the engine's")
	// ErrEngineOS means the engine runs containers of another system.
	ErrEngineOS = errors.New("the engine must run Linux containers")
)

// goArch names an ELF file's machine as Go names architectures, for the machines a Docker engine runs
// on; PPC64 is spelled by byte order, as Go spells it. An unlisted machine keeps its ELF name, which
// matches no engine and so is refused naming it.
func goArch(file *elf.File) string {
	if file.Machine == elf.EM_PPC64 && file.ByteOrder == binary.LittleEndian {
		return "ppc64le"
	}

	arches := map[elf.Machine]string{
		elf.EM_X86_64: "amd64", elf.EM_AARCH64: "arm64", elf.EM_386: "386", elf.EM_ARM: "arm",
		elf.EM_RISCV: "riscv64", elf.EM_S390: "s390x", elf.EM_LOONGARCH: "loong64", elf.EM_PPC64: "ppc64",
	}

	if arch, known := arches[file.Machine]; known {
		return arch
	}

	return file.Machine.String()
}

// Qualify decides whether executable can run as a relay on an engine of engineOS and engineArch: a
// Linux build, a static ELF, the engine's architecture, an engine of Linux containers. It reads the
// file's own bytes, so the caller passes /proc/self/exe — never a path a rebuild could replace.
func Qualify(executable, engineOS, engineArch string) error {
	return qualifyFor(runtime.GOOS, executable, engineOS, engineArch)
}

// qualifyFor is Qualify with the build's system as a parameter, so a test can reach the non-Linux
// cause.
func qualifyFor(goos, executable, engineOS, engineArch string) error {
	if goos != "linux" {
		return fmt.Errorf("%w (this build is for %s)", ErrNotLinux, goos)
	}

	if engineOS != "linux" {
		return fmt.Errorf("%w; this one runs %q containers", ErrEngineOS, engineOS)
	}

	file, err := elf.Open(executable)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrNotELF, executable, err)
	}

	defer func() { _ = file.Close() }()

	for _, prog := range file.Progs {
		if prog.Type == elf.PT_INTERP {
			return fmt.Errorf("%w (%s names an ELF interpreter)", ErrDynamic, executable)
		}
	}

	if arch := goArch(file); arch != engineArch {
		return fmt.Errorf("%w: %s is %s, the engine is %s", ErrArch, executable, arch, engineArch)
	}

	return nil
}
