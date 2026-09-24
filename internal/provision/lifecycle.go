package provision

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	// errNoDeadline means Output was asked to follow a log with nothing to end it.
	errNoDeadline = errors.New("following a container's output needs a deadline")
	// errNoLine means a followed log ended without the line asked for.
	errNoLine = errors.New("the container's output ended without the line")
)

// watchExit begins the wait whose end Exited reports. It outlives the caller's context and ends when
// the container stops, or the check closes.
func (e *Engine) watchExit(ctx context.Context, c *Container) {
	waitCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})

	e.mu.Lock()
	e.exited[c.seq] = done
	e.mu.Unlock()

	go func() {
		defer close(done)
		defer cancel()

		// Whatever ended the wait, the container no longer runs under it; its state says how.
		if _, err := e.run.call(waitCtx, request{verb: verbWait, args: []arg{{val: c.id}}}); err != nil {
			return
		}
	}()

	go func() {
		select {
		case <-e.closing:
			cancel()
		case <-done:
		}
	}()
}

// Exited returns a channel closed when the container stops. It is nil before Start.
func (e *Engine) Exited(c *Container) <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()

	done, ok := e.exited[c.seq]
	if !ok {
		return nil
	}

	return done
}

// Health returns the container's health status, "" when it has no healthcheck.
func (e *Engine) Health(ctx context.Context, c *Container) (string, error) {
	report, err := e.inspectContainer(ctx, c)

	return report.Health, err
}

// Output follows a container's stdout and returns its first line starting with prefix: a relay's
// ready line. ctx must carry a deadline; a stream that ends without the line is an error.
func (e *Engine) Output(ctx context.Context, c *Container, prefix string) (string, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		return "", errNoDeadline
	}

	if err := e.owns(c); err != nil {
		return "", err
	}

	follow, stop := context.WithCancel(ctx)
	defer stop()

	watch := &lineWatch{prefix: prefix, found: stop}

	_, err := e.run.call(follow, request{verb: verbLogsFollow, args: []arg{{val: c.id}}, stdout: watch})
	if line, ok := watch.result(); ok {
		return line, nil
	}

	if err != nil {
		return "", err
	}

	return "", fmt.Errorf("%w: %s printed no %q line", errNoLine, c.name, prefix)
}

// lineWatch finds the first whole line starting with a prefix, then ends the follow.
type lineWatch struct {
	found   func()
	prefix  string
	line    string
	partial []byte
	mu      sync.Mutex
	matched bool
}

func (w *lineWatch) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.partial = append(w.partial, p...)

	for !w.matched {
		end := bytes.IndexByte(w.partial, '\n')
		if end < 0 {
			break
		}

		line := strings.TrimRight(string(w.partial[:end]), "\r")
		w.partial = w.partial[end+1:]

		if strings.HasPrefix(line, w.prefix) {
			w.line, w.matched = line, true
			w.found()
		}
	}

	return len(p), nil
}

func (w *lineWatch) result() (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.line, w.matched
}

// LastStderrLine returns the last line a container wrote to stderr: why a relay failed.
func (e *Engine) LastStderrLine(ctx context.Context, c *Container) (string, error) {
	if err := e.owns(c); err != nil {
		return "", err
	}

	var tail lastLine

	_, err := e.run.call(ctx, request{verb: verbLogs, args: []arg{{val: c.id}}, stdout: io.Discard, stderr: &tail})

	return tail.last(), err
}

// lastLine keeps the last non-empty line written to it.
type lastLine struct {
	line    string
	partial []byte
	mu      sync.Mutex
}

func (l *lastLine) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.partial = append(l.partial, p...)

	for {
		end := bytes.IndexByte(l.partial, '\n')
		if end < 0 {
			return len(p), nil
		}

		if line := strings.TrimRight(string(l.partial[:end]), "\r"); line != "" {
			l.line = line
		}

		l.partial = l.partial[end+1:]
	}
}

func (l *lastLine) last() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if tail := strings.TrimSpace(string(l.partial)); tail != "" {
		return tail
	}

	return l.line
}

// CopyLog copies a container's log, both streams, into the check's logs directory and returns the
// file's path. The log never reaches stdout or stderr.
func (e *Engine) CopyLog(ctx context.Context, c *Container) (string, error) {
	if err := e.owns(c); err != nil {
		return "", err
	}

	path := filepath.Join(e.private, logsDir, string(c.kind)+"-"+strconv.Itoa(c.seq)+".log")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fileMode)
	if err != nil {
		return "", fmt.Errorf("copy the log of %s: %w", c.name, err)
	}

	_, err = e.run.call(ctx, request{verb: verbLogs, args: []arg{{val: c.id}}, stdout: file, stderr: file})
	if closeErr := file.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("copy the log of %s: %w", c.name, closeErr)
	}

	return path, err
}
