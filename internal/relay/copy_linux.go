//go:build linux

package relay

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"unsafe"
)

// keptMode is every mode bit the copy carries over.
const keptMode = fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

// A new entry is private to the copy until its own attributes are set.
const (
	newDirMode  = 0o700
	newFileMode = 0o600
)

// Linux's utimensat arguments, which package syscall does not export.
const (
	atFDCWD           = -0x64
	atSymlinkNoFollow = 0x100
)

// inode names one file on one filesystem: what two hard links share.
type inode struct {
	dev uint64
	ino uint64
}

// treeCopy is one copy in progress.
type treeCopy struct {
	// linked maps an inode with more than one name to the destination its first name was copied to.
	linked map[inode]string
}

// copyTree copies the tree under src into dst, an existing directory, as errCopy describes.
func copyTree(src, dst string) error {
	root, err := os.Lstat(src)
	if err != nil {
		return fmt.Errorf("%w: %w", errCopy, err)
	}

	into, err := os.Lstat(dst)
	if err != nil {
		return fmt.Errorf("%w: %w", errCopy, err)
	}

	if !root.IsDir() || !into.IsDir() {
		return fmt.Errorf("%w: %s and %s must both be a directory, and one is not a directory", errCopy, src, dst)
	}

	c := &treeCopy{linked: map[inode]string{}}

	if err := c.children(src, dst, ""); err != nil {
		return err
	}

	return c.attributes(dst, root, ".")
}

// children copies every entry of one source directory, in name order.
func (c *treeCopy) children(src, dst, rel string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return failed(rel, err)
	}

	for _, found := range entries {
		name := found.Name()
		if err := c.entry(filepath.Join(src, name), filepath.Join(dst, name), path.Join(rel, name)); err != nil {
			return err
		}
	}

	return nil
}

// entry copies one entry, then gives it its attributes. A directory's come after its children, and a
// second name of an inode already copied carries the attributes the two names share.
func (c *treeCopy) entry(src, dst, rel string) error {
	info, err := os.Lstat(src)
	if err != nil {
		return failed(rel, err)
	}

	done, err := c.create(src, dst, rel, info)
	if done || err != nil {
		return err
	}

	return c.attributes(dst, info, rel)
}

// create makes the entry itself. It reports done when the entry needs no attributes of its own.
func (c *treeCopy) create(src, dst, rel string, info fs.FileInfo) (bool, error) {
	mode := info.Mode()

	switch {
	case mode.IsDir():
		if err := os.Mkdir(dst, newDirMode); err != nil {
			return false, failed(rel, err)
		}

		return false, c.children(src, dst, rel)
	case mode.IsRegular():
		linked, err := c.link(info, dst)
		if linked || err != nil {
			return true, failed(rel, err)
		}

		return false, failed(rel, copyFile(src, dst))
	case mode&fs.ModeSymlink != 0:
		return false, failed(rel, copyLink(src, dst))
	default:
		return false, fmt.Errorf("%w: %s is %s, which cannot be copied", errCopy, rel, kindOf(mode))
	}
}

// link makes dst a hard link to an earlier copy of the same inode, when there is one.
func (c *treeCopy) link(info fs.FileInfo, dst string) (bool, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink < 2 {
		return false, nil
	}

	key := inode{dev: stat.Dev, ino: stat.Ino}

	first, seen := c.linked[key]
	if !seen {
		c.linked[key] = dst

		return false, nil
	}

	if err := os.Link(first, dst); err != nil {
		return true, fmt.Errorf("link: %w", err)
	}

	return true, nil
}

// attributes gives dst the source's owner, mode and times. The owner goes first because a change of
// owner clears setuid and setgid; a symlink has no mode of its own, and its times are its own.
func (*treeCopy) attributes(dst string, info fs.FileInfo, rel string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%w: %s: the filesystem reports no owner", errCopy, rel)
	}

	if err := os.Lchown(dst, int(stat.Uid), int(stat.Gid)); err != nil {
		return failed(rel, err)
	}

	flags := 0

	if info.Mode()&fs.ModeSymlink != 0 {
		flags = atSymlinkNoFollow
	} else if err := os.Chmod(dst, info.Mode()&keptMode); err != nil {
		return failed(rel, err)
	}

	return failed(rel, setTimes(dst, [2]syscall.Timespec{stat.Atim, stat.Mtim}, flags))
}

// copyFile copies a regular file's content into a new file; nothing at either end is followed.
func copyFile(src, dst string) error {
	in, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, newFileMode)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}

	if _, err := io.Copy(out, in); err != nil {
		return errors.Join(fmt.Errorf("write: %w", err), out.Close())
	}

	if err := out.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	return nil
}

// copyLink copies a symlink as the link itself, whatever it points at.
func copyLink(src, dst string) error {
	target, err := os.Readlink(src)
	if err != nil {
		return fmt.Errorf("read the link: %w", err)
	}

	if err := os.Symlink(target, dst); err != nil {
		return fmt.Errorf("make the link: %w", err)
	}

	return nil
}

// setTimes sets a path's access and modification times through utimensat, the one call that can set a
// symlink's own times (flags atSymlinkNoFollow) rather than its target's.
func setTimes(name string, times [2]syscall.Timespec, flags int) error {
	pathname, err := syscall.BytePtrFromString(name)
	if err != nil {
		return fmt.Errorf("utimensat %s: %w", name, err)
	}

	dir := atFDCWD

	_, _, errno := syscall.Syscall6(syscall.SYS_UTIMENSAT,
		uintptr(dir),
		uintptr(unsafe.Pointer(pathname)), uintptr(unsafe.Pointer(&times)), uintptr(flags), 0, 0)
	if errno != 0 {
		return &os.PathError{Op: "utimensat", Path: name, Err: errno}
	}

	return nil
}

// failed names the entry a step failed on; nil stays nil.
func failed(rel string, err error) error {
	if err == nil || errors.Is(err, errCopy) {
		return err
	}

	if rel == "" {
		rel = "."
	}

	return fmt.Errorf("%w: %s: %w", errCopy, rel, err)
}

// kindOf names an entry the copy refuses.
func kindOf(mode fs.FileMode) string {
	switch {
	case mode&fs.ModeNamedPipe != 0:
		return "a named pipe"
	case mode&fs.ModeSocket != 0:
		return "a socket"
	case mode&fs.ModeDevice != 0:
		return "a device"
	default:
		return "an irregular file"
	}
}
