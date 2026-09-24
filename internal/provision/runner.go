package provision

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"
)

// verb is one row of the closed table of calls the runner makes. Argv always starts from a row's
// fixed tokens; a free string never starts one.
type verb uint8

const (
	verbContext verb = iota
	verbVersion
	verbInfo
	verbWSLInfo
	verbComposeConfig
	verbInspect
	verbImageInspect
	verbNetworkInspect
	verbVolumeInspect
	verbNetworkList
	verbNetworkCreate
	verbVolumeCreate
	verbRemove
	verbNetworkRemove
	verbVolumeRemove
	verbImageRemove
	verbContainerList
	verbVolumeList
	verbImageList
	// verbCount is the number of rows; it is not a verb.
	verbCount
)

// program is what a row spawns.
type program uint8

const (
	// programDocker is the docker CLI, resolved once from PATH and used by absolute path.
	programDocker program = iota
	// programWSLInfo is the host's wslinfo.
	programWSLInfo
	// programCompose is the docker CLI's compose plugin, run beneath groupKiller: the plugin is a
	// grandchild the parent-death signal never reaches, and measured, it outlived a killed docker
	// CLI by seconds.
	programCompose
)

// mode is the environment a row runs in.
type mode uint8

const (
	// modeUser is the user's environment with the endpoint pinned and no context selection: for
	// the rows that need the user's credentials, plugins and build settings.
	modeUser mode = iota
	// modeConstructed is exactly PATH, the pinned DOCKER_HOST and an empty private DOCKER_CONFIG,
	// plus a create's multi-line variables. A client config's proxy entries never reach a
	// container created in it.
	modeConstructed
)

// deadline is a row's deadline class.
type deadline uint8

const (
	deadlineShort deadline = iota
	deadlineCopy
	deadlineLong
)

// stderrPolicy is what a row keeps of its stderr. Control flow never reads it either way.
type stderrPolicy uint8

const (
	// stderrFirstLine keeps the first line for errors and the invocation log.
	stderrFirstLine stderrPolicy = iota
	// stderrCount keeps only the number of lines: compose echoes interpolated values there.
	stderrCount
)

const (
	// shortLimit, copyLimit and longLimit are the deadlines of the classes: every call not listed
	// below; commit, import, cp and logs; pull and compose build.
	shortLimit = 30 * time.Second
	copyLimit  = 5 * time.Minute
	longLimit  = 60 * time.Minute
	// stderrCap bounds what a call's stderr can hold in memory.
	stderrCap = 64 << 10
	// waitDelay bounds how long a killed call may hold its output pipes open.
	waitDelay = 2 * time.Second
	// pendingCap bounds the call lines kept for a log not yet attached.
	pendingCap = 16
	// shellPath runs groupKiller.
	shellPath = "/bin/sh"
	// groupKiller runs its arguments as a background child with stdin passed through, and on
	// SIGTERM — the parent-death signal it is spawned with — kills its whole process group, which
	// it leads. Its exit status is the child's.
	groupKiller = `exec 3<&0; trap "kill -s KILL 0" TERM; "$@" <&3 3<&- & wait $!; exit $?`
)

// verbSpec is one row of the verb table. A call's argv is prefix, its typed arguments, suffix —
// or alt instead, for a row with two fixed forms — then its typed tail.
type verbSpec struct {
	name     string
	prefix   []string
	suffix   []string
	alt      []string
	program  program
	mode     mode
	deadline deadline
	stderr   stderrPolicy
	mutates  bool
	hold     bool
}

// verbs returns the verb table: every call the runner can make.
//
//nolint:goconst,revive // one literal, every fixed token spelled out: it reads, and is audited, as the table.
func verbs() [verbCount]verbSpec {
	return [verbCount]verbSpec{
		verbContext: {
			name: "context", program: programDocker, prefix: []string{"context", "inspect", "--format"},
			mode: modeUser, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbVersion: {
			name: "version", program: programDocker, prefix: []string{"version", "--format"},
			mode: modeUser, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbInfo: {
			name: "info", program: programDocker, prefix: []string{"info", "--format"},
			mode: modeUser, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbWSLInfo: {
			name: "wslinfo", program: programWSLInfo, prefix: []string{"--networking-mode"},
			mode: modeUser, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbComposeConfig: {
			name: "composeConfig", program: programCompose, prefix: []string{"compose"},
			suffix: []string{"config", "--format", "json"}, alt: []string{"config", "--environment"},
			mode: modeUser, deadline: deadlineShort, stderr: stderrCount,
		},
		verbInspect: {
			name: "inspect", program: programDocker, prefix: []string{"inspect", "--type", "container", "--format"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbImageInspect: {
			name: "imageInspect", program: programDocker, prefix: []string{"image", "inspect", "--format"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbNetworkInspect: {
			name: "networkInspect", program: programDocker, prefix: []string{"network", "inspect", "--format"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbVolumeInspect: {
			name: "volumeInspect", program: programDocker, prefix: []string{"volume", "inspect", "--format"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbNetworkList: {
			name: "networkList", program: programDocker, prefix: []string{"network", "ls", "--no-trunc", "--format"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbNetworkCreate: {
			name: "networkCreate", program: programDocker, prefix: []string{"network", "create"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine, mutates: true, hold: true,
		},
		verbVolumeCreate: {
			name: "volumeCreate", program: programDocker, prefix: []string{"volume", "create"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine, mutates: true, hold: true,
		},
		verbRemove: {
			name: "remove", program: programDocker, prefix: []string{"rm", "-f", "-v"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine, mutates: true, hold: true,
		},
		verbNetworkRemove: {
			name: "networkRemove", program: programDocker, prefix: []string{"network", "rm"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine, mutates: true, hold: true,
		},
		verbVolumeRemove: {
			name: "volumeRemove", program: programDocker, prefix: []string{"volume", "rm"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine, mutates: true, hold: true,
		},
		verbImageRemove: {
			name: "imageRemove", program: programDocker, prefix: []string{"rmi", "--no-prune"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine, mutates: true, hold: true,
		},
		verbContainerList: {
			name: "containerList", program: programDocker, prefix: []string{"ps", "-a", "--no-trunc", "--format"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbVolumeList: {
			name: "volumeList", program: programDocker, prefix: []string{"volume", "ls", "--format"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine,
		},
		verbImageList: {
			name: "imageList", program: programDocker, prefix: []string{"images", "--no-trunc", "--format"},
			mode: modeConstructed, deadline: deadlineShort, stderr: stderrFirstLine,
		},
	}
}

// defaultLimits are the deadlines of each class.
func defaultLimits() map[deadline]time.Duration {
	return map[deadline]time.Duration{
		deadlineShort: shortLimit,
		deadlineCopy:  copyLimit,
		deadlineLong:  longLimit,
	}
}

// arg is one typed argument of a call. A model token comes from the compose model's process
// fields and is written to the invocation log as `<model>`.
type arg struct {
	val   string
	model bool
}

// request is one call: a verb, its typed arguments and tail, and which fixed form it takes.
type request struct {
	stdin    io.Reader
	stdout   io.Writer
	dir      string
	args     []arg
	tail     []arg
	extraEnv []string
	verb     verb
	alt      bool
}

// tokens returns a call's whole argv after the program, as typed arguments: the row's fixed tokens
// are never model tokens.
func (s verbSpec) tokens(req request) ([]arg, error) {
	suffix := s.suffix
	if req.alt {
		if s.alt == nil {
			return nil, fmt.Errorf("%w: the %s row has no second form", ErrEngine, s.name)
		}

		suffix = s.alt
	}

	out := make([]arg, 0, len(s.prefix)+len(req.args)+len(suffix)+len(req.tail))
	for _, token := range s.prefix {
		out = append(out, arg{val: token})
	}

	out = append(out, req.args...)
	for _, token := range suffix {
		out = append(out, arg{val: token})
	}

	return append(out, req.tail...), nil
}

// result is what a call returned. elapsed is measured on the monotonic clock.
type result struct {
	stderrFirst string
	out         []byte
	elapsed     time.Duration
	exit        int
	stderrLines int
}

// caller makes calls. The runner is its production implementation; tests script a fake.
type caller interface {
	call(ctx context.Context, r request) (result, error)
}

// callLine is one call's line in the invocation log.
type callLine struct {
	Kind        string   `json:"kind"`
	Verb        string   `json:"verb"`
	Program     string   `json:"program"`
	Mode        string   `json:"mode"`
	Stderr      *string  `json:"stderr,omitempty"`
	StderrLines *int     `json:"stderr_lines,omitempty"`
	Argv        []string `json:"argv"`
	Exit        int      `json:"exit"`
	MS          int64    `json:"ms"`
}

// CallError is a call that ran and exited non-zero. It unwraps to ErrEngine.
type CallError struct {
	// Verb is the row that was called.
	Verb string
	// Stderr is the first line of its stderr; empty for a row whose stderr is never quoted.
	Stderr string
	// Code is its exit code; -1 when a signal ended it.
	Code int
}

// Error names the verb and the exit code, and quotes the first stderr line when the row keeps one.
func (e *CallError) Error() string {
	if e.Stderr == "" {
		return fmt.Sprintf("%s call exited %d", e.Verb, e.Code)
	}

	return fmt.Sprintf("%s call exited %d: %s", e.Verb, e.Code, e.Stderr)
}

// ExitCode returns the call's exit code.
func (e *CallError) ExitCode() int {
	return e.Code
}

// Unwrap returns ErrEngine.
func (*CallError) Unwrap() error {
	return ErrEngine
}

// execRunner spawns the verb table's programs. It is the only place anything is spawned.
//
// A runner starts read-only: it refuses every mutating row until the check that owns it has
// passed its own checks and says otherwise, so reading preconditions can never create anything.
type execRunner struct {
	logCall   func(callLine)
	limits    map[deadline]time.Duration
	pending   []callLine
	docker    string
	wslinfo   string
	pin       string
	configDir string
	env       []string
	mutable   bool
}

// newRunner resolves each program once from env's PATH. limits overrides deadline classes; nil
// keeps the defaults.
func newRunner(env []string, limits map[deadline]time.Duration) *execRunner {
	runner := &execRunner{env: env, limits: defaultLimits()}
	maps.Copy(runner.limits, limits)

	path := lookupEnv(env, "PATH")
	runner.docker = lookPath("docker", path)
	runner.wslinfo = lookPath("wslinfo", path)

	return runner
}

// lookupEnv returns the value of key in env, or "".
func lookupEnv(env []string, key string) string {
	for _, pair := range env {
		if name, value, ok := strings.Cut(pair, "="); ok && name == key {
			return value
		}
	}

	return ""
}

// lookPath finds an executable regular file named name in an absolute directory of path. A
// relative or empty entry is skipped: the CLI is never resolved against the working directory.
func lookPath(name, path string) string {
	for _, dir := range filepath.SplitList(path) {
		if dir == "" || !filepath.IsAbs(dir) {
			continue
		}

		candidate := filepath.Join(dir, name)

		info, err := os.Stat(candidate)
		if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 {
			return candidate
		}
	}

	return ""
}

// call makes one call from the verb table.
func (r *execRunner) call(ctx context.Context, req request) (result, error) {
	if req.verb >= verbCount {
		return result{}, fmt.Errorf("%w: verb %d is not in the table", ErrEngine, req.verb)
	}

	return r.run(ctx, verbs()[req.verb], req)
}

// verbSpec's call context: a held call ignores its caller's cancellation, and every call with a
// deadline class ends at it.
func (s verbSpec) bound(ctx context.Context, limit time.Duration) (context.Context, context.CancelFunc) {
	if s.hold {
		ctx = context.WithoutCancel(ctx)
	}

	if limit <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeoutCause(ctx, limit, errPastDeadline)
}

// run makes one call under spec's mode, deadline and cancellation.
func (r *execRunner) run(ctx context.Context, spec verbSpec, req request) (result, error) {
	if spec.mutates && !r.mutable {
		return result{}, fmt.Errorf("%w: the %s call mutates the engine, and this runner is read-only",
			ErrEngine, spec.name)
	}

	typed, err := spec.tokens(req)
	if err != nil {
		return result{}, err
	}

	path, argv, err := r.command(spec, typed)
	if err != nil {
		return result{}, err
	}

	env, err := r.environ(spec.mode, req.extraEnv)
	if err != nil {
		return result{}, err
	}

	limit := r.limits[spec.deadline]
	callCtx, cancel := spec.bound(ctx, limit)

	defer cancel()

	res, sink, err := spawn(callCtx, path, argv, env, spec, req)
	r.record(spec, typed, res, sink)

	return res, outcome(ctx, callCtx, spec, limit, res, err)
}

// outcome names how a call ended: its own deadline, its caller's cancellation, a failure to run,
// or a non-zero exit.
func outcome(ctx, callCtx context.Context, spec verbSpec, limit time.Duration, res result, err error) error {
	switch {
	case err == nil && res.exit == 0:
		return nil
	case errors.Is(context.Cause(callCtx), errPastDeadline):
		return fmt.Errorf("%w: %s call exceeded its %s deadline", ErrDeadline, spec.name, limit)
	case !spec.hold && ctx.Err() != nil:
		return fmt.Errorf("%s call cancelled: %w", spec.name, context.Cause(ctx))
	case err != nil:
		return err
	default:
		return &CallError{Verb: spec.name, Code: res.exit, Stderr: res.stderrFirst}
	}
}

// errPastDeadline is the cause a call's own deadline records.
var errPastDeadline = errors.New("past the call's deadline")

// command returns the program to spawn and its argv.
func (r *execRunner) command(spec verbSpec, typed []arg) (string, []string, error) {
	tokens := make([]string, 0, len(typed))
	for _, a := range typed {
		tokens = append(tokens, a.val)
	}

	switch spec.program {
	case programWSLInfo:
		if r.wslinfo == "" {
			return "", nil, fmt.Errorf("%w: wslinfo not found on PATH", ErrEngine)
		}

		return r.wslinfo, tokens, nil
	case programCompose:
		if r.docker == "" {
			return "", nil, fmt.Errorf("%w: docker CLI not found on PATH", ErrEngine)
		}

		return shellPath, append([]string{"-c", groupKiller, "sh", r.docker}, tokens...), nil
	case programDocker:
		if r.docker == "" {
			return "", nil, fmt.Errorf("%w: docker CLI not found on PATH", ErrEngine)
		}

		return r.docker, tokens, nil
	}

	return "", nil, fmt.Errorf("%w: program %d is not in the table", ErrEngine, spec.program)
}

// environ returns a call's environment. Before the endpoint is pinned, user mode is the
// environment as given, so the endpoint read resolves what the user's own CLI would use.
func (r *execRunner) environ(m mode, extra []string) ([]string, error) {
	if m == modeConstructed {
		if r.pin == "" || r.configDir == "" {
			return nil, fmt.Errorf("%w: a constructed call before the endpoint is pinned", ErrEngine)
		}

		env := []string{"PATH=" + lookupEnv(r.env, "PATH"), "DOCKER_HOST=" + r.pin, "DOCKER_CONFIG=" + r.configDir}

		return slices.Concat(env, extra), nil
	}

	env := make([]string, 0, len(r.env)+len(extra)+1)

	for _, pair := range r.env {
		name, _, _ := strings.Cut(pair, "=")
		if r.pin != "" && (name == "DOCKER_CONTEXT" || name == "DOCKER_HOST") {
			continue
		}

		env = append(env, pair)
	}

	if r.pin != "" {
		env = append(env, "DOCKER_HOST="+r.pin)
	}

	return append(env, extra...), nil
}

// deathSignal is what the kernel sends a row's child when Stutter dies: groupKiller turns SIGTERM
// into a kill of its whole group; every other child is killed outright.
func deathSignal(p program) syscall.Signal {
	if p == programCompose {
		return syscall.SIGTERM
	}

	return syscall.SIGKILL
}

// spawn runs one child to completion. The spawning goroutine holds its OS thread from start to
// reap: the parent-death signal fires when the creating thread exits, not the process.
func spawn(
	ctx context.Context, path string, argv, env []string, spec verbSpec, req request,
) (result, *stderrSink, error) {
	var out bytes.Buffer

	sink := &stderrSink{keep: spec.stderr == stderrFirstLine}

	cmd := exec.CommandContext(ctx, path, argv...)
	cmd.Env = env
	cmd.Dir = req.dir
	cmd.Stdin = req.stdin
	cmd.Stdout = &out
	cmd.Stderr = sink
	cmd.SysProcAttr = sysProcAttr(deathSignal(spec.program))
	cmd.Cancel = func() error { return killGroup(cmd.Process) }
	cmd.WaitDelay = waitDelay

	if req.stdout != nil {
		cmd.Stdout = req.stdout
	}

	runtime.LockOSThread()

	defer runtime.UnlockOSThread()

	start := time.Now()

	if err := cmd.Start(); err != nil {
		return result{exit: -1}, sink, fmt.Errorf("%w: start the %s call: %w", ErrEngine, spec.name, err)
	}

	err := cmd.Wait()
	res := result{out: out.Bytes(), elapsed: time.Since(start), exit: -1, stderrLines: sink.count()}

	if cmd.ProcessState != nil {
		res.exit = cmd.ProcessState.ExitCode()
	}

	if sink.keep {
		res.stderrFirst = sink.firstLine()
	}

	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		return res, sink, fmt.Errorf("%w: the %s call: %w", ErrEngine, spec.name, err)
	}

	return res, sink, nil
}

// record hands the call's line to the invocation log. Before a log is attached, the first few lines
// wait for it: the precondition reads happen before the check has a directory to log into.
func (r *execRunner) record(spec verbSpec, typed []arg, res result, sink *stderrSink) {
	line := callLine{
		Kind: "call", Verb: spec.name, Program: r.docker, Mode: "user",
		Exit: res.exit, MS: res.elapsed.Milliseconds(),
	}

	if spec.program == programWSLInfo {
		line.Program = r.wslinfo
	}

	if spec.mode == modeConstructed {
		line.Mode = "constructed"
	}

	for _, a := range typed {
		if a.model {
			line.Argv = append(line.Argv, "<model>")
		} else {
			line.Argv = append(line.Argv, a.val)
		}
	}

	if sink.keep {
		first := res.stderrFirst
		line.Stderr = &first
	} else {
		lines := sink.count()
		line.StderrLines = &lines
	}

	if r.logCall != nil {
		r.logCall(line)
	} else if len(r.pending) < pendingCap {
		r.pending = append(r.pending, line)
	}
}

// attach hands the runner the check's private client configuration and invocation log, flushes the
// lines that waited for the log, and lets the runner mutate the engine from now on.
func (r *execRunner) attach(configDir string, logCall func(callLine)) {
	r.configDir, r.logCall, r.mutable = configDir, logCall, true

	for _, line := range r.pending {
		logCall(line)
	}

	r.pending = nil
}

// stderrSink counts a call's stderr lines and, for a first-line row, keeps up to stderrCap bytes.
type stderrSink struct {
	buf     []byte
	lines   int
	keep    bool
	partial bool
}

// Write never fails: stderr is never allowed to stall a call.
func (s *stderrSink) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	s.lines += bytes.Count(p, []byte{'\n'})
	s.partial = p[len(p)-1] != '\n'

	if s.keep && len(s.buf) < stderrCap {
		s.buf = append(s.buf, p[:min(len(p), stderrCap-len(s.buf))]...)
	}

	return len(p), nil
}

// count returns the number of lines written, a final unterminated one included.
func (s *stderrSink) count() int {
	if s.partial {
		return s.lines + 1
	}

	return s.lines
}

// firstLine returns the first line kept, trimmed.
func (s *stderrSink) firstLine() string {
	line, _, _ := bytes.Cut(s.buf, []byte{'\n'})

	return strings.TrimSpace(string(line))
}
