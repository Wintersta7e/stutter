package relay

import "errors"

// errCopy means the copy mode could not reproduce the tree. The copy is how a dependency's volume is
// seeded and restored, so a partial copy is a failure, never a smaller tree.
//
// What the copy keeps: every entry under the source root lands at its relative path with its type
// (regular file, directory, symlink), content, permission bits including setuid, setgid and sticky,
// owner and group, and access and modification times — a directory's set after its children, which
// would otherwise move them. A hard link within the tree stays a link. A symlink is copied as the
// link and never followed. The destination root takes the source root's mode, owner and times.
// Extended attributes and ACLs are not copied. A socket, device or named pipe fails the copy, named:
// none of them is data a snapshot can hold.
var errCopy = errors.New("cannot copy the tree")
