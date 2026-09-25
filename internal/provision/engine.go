package provision

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// Options configure one check's use of the engine.
type Options struct {
	// StateDir holds the check ledgers; empty is $XDG_STATE_HOME/stutter/checks, else
	// $HOME/.local/state/stutter/checks.
	StateDir string
	// TempDir holds the check-private directory; empty is os.TempDir().
	TempDir string
	// Keep keeps the check's resources at Close instead of removing them.
	Keep bool
}

// engineCaller is a caller that can be handed the check's private client configuration and its
// invocation log once they exist, and told whether it may change the engine from then on.
type engineCaller interface {
	caller
	attach(configDir string, logCall func(callLine), mutable bool)
}

// openDeps is what Open reads from the host, as fields so tests can stand in for the engine.
type openDeps struct {
	host  hostFS
	admit func(ctx context.Context) (Identity, engineCaller, error)
	env   []string
}

// Engine is one check's hold on the user's engine: its identity, its ledger and its private
// directory. Everything the check creates on the engine is created, verified and removed through it.
type Engine struct {
	run          engineCaller
	book         *book
	pins         map[string]compose.Image
	fingerprints map[string]compose.Prints
	// exited holds, per started container's seq, the channel its wait closes.
	exited map[int]chan struct{}
	// held maps a one-name container kept stopped under Keep to its seq, until its successor.
	held map[string]int
	// hostPorts holds, per created container's seq, the host ports reserved for it until its removal.
	hostPorts map[int][]uint16
	// closing is closed when the check closes, ending every wait.
	closing    chan struct{}
	interfaces func() ([]localAddr, error)
	log        *invLog
	identity   Identity
	id         string
	private    string
	// wslDistro is the WSL distribution this process runs in, empty outside WSL: Docker Desktop reports a
	// bind's source under it.
	wslDistro  string
	host       hostFS
	down       Teardown
	swept      SweepResult
	privateSeq int
	// teardownBound overrides teardown's bound when set; tests shorten it.
	teardownBound time.Duration
	closeOnce     sync.Once
	mu            sync.Mutex
	keep          bool
	locks         bool
}

// Open accepts the engine, then gives the check its ledger and its private directory, in that
// order: a refusal writes nothing, and a failure after the ledger exists removes what Open made and
// touches no engine resource.
func Open(ctx context.Context, opts Options) (*Engine, error) {
	return openWith(ctx, opts, openDeps{
		admit: func(ctx context.Context) (Identity, engineCaller, error) {
			identity, runner, err := preconditionsIn(ctx, os.Environ(), string(filepath.Separator))
			if err != nil {
				return Identity{}, nil, err
			}

			return identity, runner, nil
		},
		env:  os.Environ(),
		host: defaultHostFS(),
	})
}

func openWith(ctx context.Context, opts Options, deps openDeps) (*Engine, error) {
	identity, run, err := deps.admit(ctx)
	if err != nil {
		return nil, err
	}

	state, err := stateDir(opts.StateDir, deps.env, deps.host)
	if err != nil {
		return nil, err
	}

	id, err := newCheckID()
	if err != nil {
		return nil, err
	}

	temp := opts.TempDir
	if temp == "" {
		temp = os.TempDir()
	}

	e := &Engine{
		run: run, host: deps.host, identity: identity, id: id, keep: opts.Keep,
		private: filepath.Join(temp, "stutter-"+id), interfaces: localAddrs,
		exited: map[int]chan struct{}{}, held: map[string]int{}, closing: make(chan struct{}),
		wslDistro: lookupEnv(deps.env, "WSL_DISTRO_NAME"),
	}

	led, err := createLedger(state, deps.host, header{
		Check: id, EngineID: identity.EngineID, Endpoint: identity.Endpoint, Context: identity.Context,
		PrivateDir: e.private, Project: project(id),
	})
	if err != nil {
		return nil, err
	}

	e.book = newBook(led, id)

	if err := e.prepare(); err != nil {
		return nil, errors.Join(err, e.discard())
	}

	e.swept = e.sweepAll(ctx)

	return e, nil
}

// CheckID returns the check's ID.
func (e *Engine) CheckID() string {
	return e.id
}

// PrivateDir returns the check-private directory's absolute path.
func (e *Engine) PrivateDir() string {
	return e.private
}

// Project returns the compose project name the check's builds use.
func (e *Engine) Project() string {
	return project(e.id)
}

// Identity returns the engine the check runs against.
func (e *Engine) Identity() Identity {
	return e.identity
}

// ComposeConfig runs one `docker compose <args> config` read in dir, in the user's environment. It
// returns compose's stdout, which is never logged or written, and the number of lines compose
// printed on stderr, which is never quoted either: both can carry interpolated secrets.
func (e *Engine) ComposeConfig(
	ctx context.Context, dir string, args []string, read compose.ConfigRead,
) ([]byte, int, error) {
	req := request{verb: verbComposeConfig, dir: dir, alt: read.Environment}

	for _, a := range args {
		req.args = append(req.args, arg{val: a})
	}

	if read.Service != "" && !read.Environment {
		req.tail = []arg{{val: read.Service}}
	}

	res, err := e.run.call(ctx, req)
	if err != nil {
		return nil, res.stderrLines, err
	}

	return res.out, res.stderrLines, nil
}

// prepare runs the lock self-test and makes the private directory and its invocation log.
func (e *Engine) prepare() error {
	enforced, err := e.book.led.selfTest()
	if err != nil {
		return err
	}

	e.locks = enforced

	seq, err := makePrivate(e.private, e.book, e.host)
	if err != nil {
		return err
	}

	e.privateSeq = seq

	if e.log, err = openInvLog(filepath.Join(e.private, invocationLog)); err != nil {
		return err
	}

	e.run.attach(filepath.Join(e.private, dockerConfigDir), e.log.call, true)

	return nil
}

// discard removes what a refused Open made: the invocation log's handle, the private directory if
// it was made — privateSeq is set only once it was, and a directory makePrivate refused it removed
// itself — and the ledger.
func (e *Engine) discard() error {
	var errs []error

	if e.log != nil {
		errs = append(errs, e.log.close())
	}

	if e.privateSeq != 0 {
		errs = append(errs, removeMade(e.private))
	}

	return errors.Join(append(errs, e.book.led.remove())...)
}
