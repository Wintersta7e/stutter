package provision

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The check-private directory's own entries.
const (
	invocationLog   = "invocation.log"
	logsDir         = "logs"
	dockerConfigDir = "docker-config"
	// hostDir and hostFile are what a host-path line records.
	hostDir  = "dir"
	hostFile = "file"
)

// errOutsidePrivate means a host path named for the log is not under the check-private directory.
var errOutsidePrivate = errors.New("the path is not under the check-private directory")

// invLog is the invocation log: one JSON line per call, per hold and per host path created. It is
// append-only and never reaches stdout or stderr.
type invLog struct {
	file *os.File
	err  error
	mu   sync.Mutex
}

// openInvLog creates the log; it must not exist.
func openInvLog(path string) (*invLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return nil, fmt.Errorf("%w: create %s: %w", ErrPrivateDir, path, err)
	}

	return &invLog{file: file}, nil
}

// write appends v as one line, keeping any failure for close to report too.
func (l *invLog) write(v any) error {
	var line bytes.Buffer

	// A reader greps the log for `<model>`; HTML escaping would hide it.
	encoder := json.NewEncoder(&line)
	encoder.SetEscapeHTML(false)

	err := encoder.Encode(v)

	l.mu.Lock()
	defer l.mu.Unlock()

	if err == nil {
		_, err = l.file.Write(line.Bytes())
	}

	if err != nil {
		err = fmt.Errorf("write the invocation log: %w", err)
		l.err = errors.Join(l.err, err)
	}

	return err
}

// call is the runner's hook. A call never fails for its log line: write keeps the failure, and
// close reports it.
func (l *invLog) call(line callLine) {
	if err := l.write(line); err != nil {
		return
	}
}

// close closes the log, reporting any line that could not be written.
func (l *invLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := errors.Join(l.err, l.file.Close()); err != nil {
		return fmt.Errorf("invocation log: %w", err)
	}

	return nil
}

// Hold is how long the bus held one run's deliveries back, as the invocation log records it.
type Hold struct {
	// Term names the hold's term.
	Term string
	// Length is how long the hold lasted, on the monotonic clock.
	Length time.Duration
	// Bound is the longest it was allowed to last.
	Bound time.Duration
	// Bytes is what the hold kept back.
	Bytes int64
	// Messages is how many messages the hold kept back.
	Messages int
}

type holdLine struct {
	Kind     string `json:"kind"`
	Term     string `json:"term"`
	MS       int64  `json:"ms"`
	BoundMS  int64  `json:"bound_ms"`
	Bytes    int64  `json:"bytes"`
	Messages int    `json:"messages"`
}

type hostPathLine struct {
	Kind string `json:"kind"`
	Op   string `json:"op"`
	Type string `json:"type"`
	Path string `json:"path"`
}

// LogHold records one hold in the invocation log.
func (e *Engine) LogHold(h Hold) error {
	return e.log.write(holdLine{
		Kind: "hold", Term: h.Term, MS: h.Length.Milliseconds(), BoundMS: h.Bound.Milliseconds(),
		Bytes: h.Bytes, Messages: h.Messages,
	})
}

// LogHostPath records a file or directory the caller has just created under a path HostPath
// handed out. A path outside the check-private directory is refused.
func (e *Engine) LogHostPath(path string) error {
	clean := filepath.Clean(path)

	rel, err := filepath.Rel(e.private, clean)
	if !filepath.IsAbs(path) || err != nil || rel == "." || rel == ".." ||
		strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("log host path %s: %w", path, errOutsidePrivate)
	}

	info, err := os.Lstat(clean)
	if err != nil {
		return fmt.Errorf("log host path %s: %w", path, err)
	}

	kind := hostFile
	if info.IsDir() {
		kind = hostDir
	}

	return e.recordHostPath(kind, clean)
}

func (e *Engine) recordHostPath(kind, path string) error {
	return e.log.write(hostPathLine{Kind: "hostpath", Op: "create", Type: kind, Path: path})
}
