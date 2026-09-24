package provision

import "errors"

var (
	// ErrPrecondition means the host or the engine cannot run a compose check: not Linux, inside a
	// container, no docker CLI, a remote, non-Docker or rootless engine, or no usable compose plugin.
	ErrPrecondition = errors.New("engine precondition not met")
	// ErrEngine means a call to the engine failed. A *CallError unwraps to it.
	ErrEngine = errors.New("engine call failed")
	// ErrDeadline means a call to the engine ran past its deadline and was killed.
	ErrDeadline = errors.New("engine call deadline exceeded")
	// ErrStateDir means the directory that holds the check ledgers cannot be trusted: not local,
	// not owned or not private, or the ledger cannot be written or locked there.
	ErrStateDir = errors.New("state directory unusable")
	// ErrPrivateDir means the check's private directory cannot be trusted: it already existed, or is
	// not a local directory this user alone owns with mode 0700.
	ErrPrivateDir = errors.New("check-private directory unusable")
)
