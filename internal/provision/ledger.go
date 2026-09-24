package provision

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/version"
)

// ledgerFormat is the ledger's format version, written in every header.
const ledgerFormat = 1

const (
	// privateMode is the only mode a directory Stutter keeps state or secrets in may have.
	privateMode fs.FileMode = 0o700
	// fileMode is the mode of every file Stutter writes into such a directory.
	fileMode fs.FileMode = 0o600
)

// Reasons a directory or a lock is refused; each is wrapped with the path and what was found.
var (
	errRemoteFS  = errors.New("its modes and locks cannot be trusted")
	errSymlink   = errors.New("it is a symlink")
	errNotDir    = errors.New("it is not a directory")
	errMode      = errors.New("its mode is not 0700")
	errOwner     = errors.New("it is owned by another user")
	errLockTaken = errors.New("another process holds its lock")
)

// ResourceType is what kind of engine or host resource a ledger entry or a listing names.
type ResourceType string

// The resource types a ledger records.
const (
	// ResourceContainer is a container.
	ResourceContainer ResourceType = "container"
	// ResourceNetwork is a network.
	ResourceNetwork ResourceType = "network"
	// ResourceVolume is a volume; one recorded with a parent is its container's anonymous volume.
	ResourceVolume ResourceType = "volume"
	// ResourceImage is an image, named by its reference.
	ResourceImage ResourceType = "image"
	// ResourceHostPath is a path on the host.
	ResourceHostPath ResourceType = "hostpath"
)

// op is what a ledger entry records.
type op string

const (
	// opIntent is written before the create it announces.
	opIntent op = "intent"
	// opCreated records the ID the engine returned.
	opCreated op = "created"
	// opVerified records that an inspect proved the resource carries this check's labels.
	opVerified op = "verified"
	// opRemoved records a removal verified gone.
	opRemoved op = "removed"
	// opRemoveFailed records a removal the engine refused, or one that could not be verified.
	opRemoveFailed op = "remove-failed"
	// opAbsent records that recovery found nothing under an intent's name.
	opAbsent op = "absent"
	// opKept records that the check kept its resources.
	opKept op = "kept"
	// opRetained records that the engine resources are done and log files remain on the host.
	opRetained op = "retained"
)

// checkWide reports an op that concerns the whole check rather than one resource.
func (o op) checkWide() bool {
	return o == opKept || o == opRetained
}

// known reports an op the ledger defines.
func (o op) known() bool {
	switch o {
	case opIntent, opCreated, opVerified, opRemoved, opRemoveFailed, opAbsent, opKept, opRetained:
		return true
	default:
		return false
	}
}

// header is a ledger's first line: who owns the check, on which engine and host.
type header struct {
	Check      string `json:"check"`
	EngineID   string `json:"engine_id"`
	Endpoint   string `json:"endpoint"`
	Context    string `json:"context"`
	Host       string `json:"host"`
	BootID     string `json:"boot_id"`
	PIDNS      string `json:"pid_ns"`
	Build      string `json:"build"`
	Started    string `json:"started"`
	PrivateDir string `json:"private_dir"`
	Project    string `json:"project"`
	Format     int    `json:"format"`
	PID        int    `json:"pid"`
	StartTime  uint64 `json:"start_time"`
}

// entry is one line after the header. Seq names the resource: the intent allocates it and every
// later line about that resource repeats it.
type entry struct {
	Op      op           `json:"op"`
	Type    ResourceType `json:"type,omitempty"`
	Kind    rules.Kind   `json:"kind,omitempty"`
	Name    string       `json:"name,omitempty"`
	ID      string       `json:"id,omitempty"`
	Service string       `json:"service,omitempty"`
	Error   string       `json:"error,omitempty"`
	Seq     int          `json:"seq"`
	Parent  int          `json:"parent,omitempty"`
}

// valid reports an entry a writer could have written.
func (e entry) valid() bool {
	if !e.Op.known() || e.Seq < 0 {
		return false
	}

	if e.Op.checkWide() {
		return true
	}

	switch e.Type {
	case ResourceContainer, ResourceNetwork, ResourceVolume, ResourceImage, ResourceHostPath:
		return e.Seq > 0
	default:
		return false
	}
}

// hostFS is the host-filesystem and process facts the ledger relies on, as fields so tests can
// stand in for the filesystem type and the lock.
type hostFS struct {
	statfs  func(path string) (int64, error)
	tryLock func(file *os.File) (bool, error)
	sync    func(file *os.File) error
	lstat   func(path string) (fs.FileInfo, error)
	owner   func(info fs.FileInfo) (int, bool)
	euid    int
}

// remoteFilesystems are the filesystem types whose modes and locks cannot be trusted: 9p reports
// every file 0777 and locks only within one namespace; the others are remote or user-space.
func remoteFilesystems() map[int64]string {
	return map[int64]string{
		0x01021997: "v9fs",
		0x6969:     "nfs",
		0x517B:     "smb",
		0xFF534D42: "cifs",
		0xFE534D42: "smb2",
		0x65735546: "fuse",
	}
}

// local refuses a path on a filesystem remoteFilesystems names.
func (h hostFS) local(path string) error {
	magic, err := h.statfs(path)
	if err != nil {
		return fmt.Errorf("its filesystem cannot be read: %w", err)
	}

	if name, remote := remoteFilesystems()[magic&0xFFFFFFFF]; remote {
		return fmt.Errorf("it is on a %s filesystem: %w", name, errRemoteFS)
	}

	return nil
}

// private refuses a path that is not a directory this user alone owns and reaches, on a local
// filesystem: a symlink, another owner, any mode but 0700.
func (h hostFS) private(path string) error {
	info, err := h.lstat(path)
	if err != nil {
		return fmt.Errorf("it cannot be read: %w", err)
	}

	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		return errSymlink
	case !info.IsDir():
		return errNotDir
	case info.Mode().Perm() != privateMode:
		return fmt.Errorf("%w: it is %04o", errMode, info.Mode().Perm())
	}

	if uid, ok := h.owner(info); !ok || uid != h.euid {
		return fmt.Errorf("%w: uid %d, not %d", errOwner, uid, h.euid)
	}

	return h.local(path)
}

// stateDir returns the directory that holds the check ledgers, created 0700 and verified private
// and local. given overrides the default: $XDG_STATE_HOME/stutter/checks when that is absolute,
// else $HOME/.local/state/stutter/checks.
func stateDir(given string, env []string, host hostFS) (string, error) {
	dir := given

	if dir == "" {
		switch xdg, home := lookupEnv(env, "XDG_STATE_HOME"), lookupEnv(env, "HOME"); {
		case filepath.IsAbs(xdg):
			dir = filepath.Join(xdg, "stutter", "checks")
		case filepath.IsAbs(home):
			dir = filepath.Join(home, ".local", "state", "stutter", "checks")
		default:
			return "", fmt.Errorf("%w: neither XDG_STATE_HOME nor HOME is an absolute path", ErrStateDir)
		}
	}

	if err := os.MkdirAll(dir, privateMode); err != nil {
		return "", fmt.Errorf("%w: %s cannot be created: %w", ErrStateDir, dir, err)
	}

	if err := host.private(dir); err != nil {
		return "", fmt.Errorf("%w: %s: %w", ErrStateDir, dir, err)
	}

	return dir, nil
}

// ledger is a check's own ledger file, locked for the life of the process.
type ledger struct {
	file *os.File
	host hostFS
	path string
	mu   sync.Mutex
}

// createLedger writes the check's ledger into dir: as a temporary file, locked, its header synced,
// then renamed into place and the directory synced — so a sweep never sees an unlocked live
// ledger. The host facts of the header are filled here.
func createLedger(dir string, host hostFS, hdr header) (*ledger, error) {
	if err := fillHost(&hdr); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStateDir, err)
	}

	final := filepath.Join(dir, hdr.Check+".ledger")
	temp := final + ".tmp"

	file, err := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return nil, fmt.Errorf("%w: create the ledger for check %s: %w", ErrStateDir, hdr.Check, err)
	}

	led := &ledger{file: file, host: host, path: temp}

	if err := led.place(hdr, final); err != nil {
		// Removed before the lock is released, wherever the failure left it.
		return nil, errors.Join(fmt.Errorf("%w: ledger for check %s: %w", ErrStateDir, hdr.Check, err),
			os.Remove(led.path), file.Close())
	}

	return led, nil
}

// place locks the temporary ledger, writes its header and renames it to final.
func (l *ledger) place(hdr header, final string) error {
	locked, lockErr := l.host.tryLock(l.file)
	if lockErr != nil {
		return fmt.Errorf("lock it: %w", lockErr)
	}

	if !locked {
		return errLockTaken
	}

	if err := l.writeLine(hdr); err != nil {
		return err
	}

	if err := os.Rename(l.path, final); err != nil {
		return fmt.Errorf("rename it into place: %w", err)
	}

	l.path = final

	dir, openErr := os.Open(filepath.Dir(final))
	if openErr != nil {
		return fmt.Errorf("open its directory: %w", openErr)
	}

	return errors.Join(l.host.sync(dir), dir.Close())
}

// fillHost records who owns the check: host, boot, process and PID namespace, and the build.
func fillHost(hdr *header) error {
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("read the host name: %w", err)
	}

	boot, err := bootID()
	if err != nil {
		return err
	}

	namespace, err := pidNamespace()
	if err != nil {
		return err
	}

	start, err := startTime(os.Getpid())
	if err != nil {
		return err
	}

	hdr.Format, hdr.Host, hdr.BootID, hdr.PIDNS = ledgerFormat, host, boot, namespace
	hdr.PID, hdr.StartTime, hdr.Build = os.Getpid(), start, version.String()
	hdr.Started = time.Now().UTC().Format(time.RFC3339)

	return nil
}

// append writes one entry with one write and syncs it, before the engine call it precedes.
func (l *ledger) append(e entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.writeLine(e)
}

// writeLine writes v as one newline-terminated JSON line and syncs the file.
func (l *ledger) writeLine(v any) error {
	line, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode a ledger line: %w", err)
	}

	if _, err := l.file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write the ledger: %w", err)
	}

	if err := l.host.sync(l.file); err != nil {
		return fmt.Errorf("sync the ledger: %w", err)
	}

	return nil
}

// selfTest reports whether locks are enforced here: a second lock through a second open file
// description must be refused. When it is granted, sweeping is unsafe.
func (l *ledger) selfTest() (bool, error) {
	second, err := os.Open(l.path)
	if err != nil {
		return false, fmt.Errorf("%w: reopen the ledger: %w", ErrStateDir, err)
	}

	granted, lockErr := l.host.tryLock(second)

	// Closing the only descriptor of that open file description releases a granted lock.
	if err := errors.Join(lockErr, second.Close()); err != nil {
		return false, fmt.Errorf("%w: lock self-test: %w", ErrStateDir, err)
	}

	return !granted, nil
}

// close releases the ledger's lock, leaving the file for a later sweep.
func (l *ledger) close() error {
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("close the ledger: %w", err)
	}

	return nil
}

// loaded is a ledger as read back.
type loaded struct {
	entries []entry
	path    string
	header  header
	// corrupt means a line other than the last could not be read: never acted on automatically.
	corrupt bool
}

// loadLedger reads a ledger. A truncated or unreadable final line is a kill mid-write and is
// discarded; any other malformed line makes the ledger corrupt.
func loadLedger(path string) (loaded, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return loaded{}, fmt.Errorf("read ledger %s: %w", path, err)
	}

	out := loaded{path: path}
	lines := bytes.Split(data, []byte{'\n'})
	// The piece after the last newline is empty, or a partial line a kill left: discarded either way.
	lines = lines[:len(lines)-1]

	if len(lines) == 0 || !decodeHeader(lines[0], &out.header) {
		out.corrupt = true

		return out, nil
	}

	for index, line := range lines[1:] {
		var e entry
		if decodeStrict(line, &e) && e.valid() {
			out.entries = append(out.entries, e)

			continue
		}

		if index != len(lines)-2 {
			out.corrupt = true
		}
	}

	return out, nil
}

// decodeHeader decodes and checks a header line.
func decodeHeader(line []byte, into *header) bool {
	return decodeStrict(line, into) && into.Format == ledgerFormat && isCheckID(into.Check)
}

// decodeStrict decodes one JSON object, refusing unknown keys and trailing data.
func decodeStrict(line []byte, into any) bool {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(into); err != nil {
		return false
	}

	_, err := decoder.Token()

	return errors.Is(err, io.EOF)
}
