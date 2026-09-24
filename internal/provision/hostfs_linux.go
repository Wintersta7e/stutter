//go:build linux

package provision

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// startTimeField is the position of a process's start time in /proc/<pid>/stat, counted from 1.
const startTimeField = 22

// errShortStat means a /proc stat line ended before the start time.
var errShortStat = errors.New("the process's stat line has no start time")

// defaultHostFS is the real host.
func defaultHostFS() hostFS {
	return hostFS{
		statfs:    fsType,
		tryLock:   tryLock,
		sync:      (*os.File).Sync,
		lstat:     os.Lstat,
		owner:     ownerOf,
		startTime: startTime,
		euid:      os.Geteuid(),
	}
}

// fsType returns the filesystem type magic of path.
func fsType(path string) (int64, error) {
	var stat syscall.Statfs_t

	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}

	//nolint:unconvert // Statfs_t.Type is not int64 on every Linux architecture.
	return int64(stat.Type), nil
}

// tryLock takes a non-blocking exclusive lock on file's open file description, reporting false when
// another description holds it. The lock is released when the description is closed.
func tryLock(file *os.File) (bool, error) {
	conn, err := file.SyscallConn()
	if err != nil {
		return false, fmt.Errorf("lock %s: %w", file.Name(), err)
	}

	var lockErr error

	if err := conn.Control(func(fd uintptr) {
		lockErr = syscall.Flock(int(fd), syscall.LOCK_EX|syscall.LOCK_NB)
	}); err != nil {
		return false, fmt.Errorf("lock %s: %w", file.Name(), err)
	}

	switch {
	case lockErr == nil:
		return true, nil
	case errors.Is(lockErr, syscall.EWOULDBLOCK):
		return false, nil
	default:
		return false, fmt.Errorf("lock %s: %w", file.Name(), lockErr)
	}
}

// ownerOf returns the UID owning a file.
func ownerOf(info fs.FileInfo) (int, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return -1, false
	}

	return int(stat.Uid), true
}

// bootID identifies this kernel boot. WSL distros share one kernel, hence one boot ID.
func bootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read the boot ID: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

// pidNamespace identifies this process's PID namespace: a PID means something only inside it.
func pidNamespace() (string, error) {
	link, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return "", fmt.Errorf("read the PID namespace: %w", err)
	}

	return link, nil
}

// startTime returns field 22 of /proc/<pid>/stat: when the process started, in clock ticks since
// boot. With the PID it names one process for the life of the boot.
func startTime(pid int) (uint64, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, fmt.Errorf("read the start time of process %d: %w", pid, err)
	}

	// The command name in parentheses may hold spaces and parentheses; the fields after the last
	// closing one start at field 3.
	text := string(data)
	fields := strings.Fields(text[strings.LastIndexByte(text, ')')+1:])

	const first = 3
	if len(fields) <= startTimeField-first {
		return 0, fmt.Errorf("process %d: %w: %d fields", pid, errShortStat, len(fields)+first-1)
	}

	value, err := strconv.ParseUint(fields[startTimeField-first], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("process %d start time: %w", pid, err)
	}

	return value, nil
}
