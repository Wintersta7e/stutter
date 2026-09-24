//go:build linux

package compose

import "syscall"

// Filesystem magic numbers of the kernel pseudo-filesystems a bind source must never lie on
// (linux/magic.h). devtmpfs reports tmpfs's number, so a device tree is caught by its device files
// instead.
const (
	procMagic       = 0x9fa0
	sysfsMagic      = 0x62656572
	devptsMagic     = 0x1cd1
	cgroupMagic     = 0x27e0eb
	cgroup2Magic    = 0x63677270
	debugfsMagic    = 0x64626720
	tracefsMagic    = 0x74726163
	securityfsMagic = 0x73636673
	bpfMagic        = 0xcafe4a11
)

// pseudoFilesystem reports a path on a kernel pseudo-filesystem.
func pseudoFilesystem(path string) bool {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return false
	}

	switch stat.Type {
	case procMagic, sysfsMagic, devptsMagic, cgroupMagic, cgroup2Magic, debugfsMagic, tracefsMagic,
		securityfsMagic, bpfMagic:
		return true
	default:
		return false
	}
}
