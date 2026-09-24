package corpus

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// freshRestarts is how many more times a restart is repeated when the kernel hands back a port the
// previous start served on. Measured, it never has; the bound is what makes "a fresh port" a promise
// rather than a likelihood.
const freshRestarts = 3

// The server's layout inside a store: <store>/jetstream/<account>/streams/<stream>, and the directory a
// purge moves a stream's messages into while it removes them.
const (
	jetStreamDir = "jetstream"
	streamsDir   = "streams"
	purgedDir    = "__msgs__"
)

var (
	// ErrCheckpoint means the bus could not be checkpointed or restored: copying the store or
	// restarting the server failed. It is a flake of the machine, never a verdict on the service.
	ErrCheckpoint = errors.New("the bus store could not be checkpointed or restored")
	// ErrMemoryStorage means a stream or consumer keeps its state in memory, which a restart drops: a
	// restore would silently return the bus without it.
	ErrMemoryStorage = errors.New("memory storage does not survive the bus restarting")
	// errCheckpointDir means the directory a checkpoint was asked for cannot hold one.
	errCheckpointDir = errors.New("the checkpoint directory is unusable")
	// errForeignCheckpoint means a restore was handed a checkpoint of some other store.
	errForeignCheckpoint = errors.New("the checkpoint was taken from another bus store")
	// errClearAfterCheckpoint means Clear was asked for once a checkpoint holds the stream.
	errClearAfterCheckpoint = errors.New("the corpus stream is not cleared once a checkpoint was taken; " +
		"restore the checkpoint instead")
	// errStalePort means the kernel kept handing a restarted server the port it served on before.
	errStalePort = errors.New("the restarted bus kept the port it served on before")
)

// Checkpoint is a byte copy of the bus's whole store: every stream, key/value bucket, object store and
// consumer, with its state.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Checkpoint struct {
	dir   string
	store string
}

// Dir is the directory the checkpoint was copied into.
func (c Checkpoint) Dir() string {
	return c.dir
}

// Checkpoint copies the whole store into dir, a directory beside the store that does not exist yet.
//
// A stream or consumer with memory storage refuses the checkpoint, naming each, before anything is
// stopped. Otherwise the server is shut down (its final flush included), the store copied byte for
// byte, and the server started again on fresh ports.
//
// The caller guarantees the service under test is gone; shutting down closes any other connection.
// No other Corpus method may be in flight.
func (c *Corpus) Checkpoint(ctx context.Context, dir string) (Checkpoint, error) {
	if err := c.checkpointDir(dir); err != nil {
		return Checkpoint{}, err
	}

	if err := c.refuseMemoryStorage(ctx); err != nil {
		return Checkpoint{}, err
	}

	err := c.restart(ctx, func() error {
		if err := finishDeletions(c.dir); err != nil {
			return err
		}

		if err := os.Mkdir(dir, storeMode); err != nil {
			return fmt.Errorf("create the checkpoint directory: %w", err)
		}

		if err := os.CopyFS(dir, os.DirFS(c.dir)); err != nil {
			return fmt.Errorf("copy the store into %s: %w", dir, err)
		}

		return nil
	})
	if err != nil {
		return Checkpoint{}, err
	}

	c.checkpointed = true

	return Checkpoint{dir: dir, store: c.dir}, nil
}

// Restore returns the bus to a checkpoint of this store, byte for byte, on fresh ports.
//
// Whatever was created since — buckets, streams, consumers, pauses, a key's next revision — is gone,
// and whatever was deleted is back. The caller guarantees the service under test is gone; no other
// Corpus method may be in flight.
func (c *Corpus) Restore(ctx context.Context, checkpoint Checkpoint) error {
	if checkpoint.store != c.dir {
		return fmt.Errorf("%w: %s holds a checkpoint of %s, not of %s",
			errForeignCheckpoint, checkpoint.dir, checkpoint.store, c.dir)
	}

	return c.restart(ctx, func() error {
		if err := os.RemoveAll(c.dir); err != nil {
			return fmt.Errorf("remove the store: %w", err)
		}

		if err := os.Mkdir(c.dir, storeMode); err != nil {
			return fmt.Errorf("recreate the store: %w", err)
		}

		if err := os.CopyFS(c.dir, os.DirFS(checkpoint.dir)); err != nil {
			return fmt.Errorf("copy %s into the store: %w", checkpoint.dir, err)
		}

		return nil
	})
}

// finishDeletions removes what the stopped server was still deleting in store.
//
// The server answers a stream deletion before the files are gone: it renames the stream's directory
// with a "." prefix, which no stream name can carry, and removes it in the background. A purge does the
// same with the stream's __msgs__ directory. Shutting down waits for neither, so a copy made straight
// after either fails on a file that vanished mid-copy, or keeps a half-removed stream in the checkpoint.
// Both are what the server itself deletes at its next start.
func finishDeletions(store string) error {
	return eachStreamsDir(store, finishStreamDeletions)
}

// removeStream deletes a stream from a stopped server's store, and finishes whatever the server was
// still deleting there.
func removeStream(store, name string) error {
	return eachStreamsDir(store, func(streams string) error {
		if err := os.RemoveAll(filepath.Join(streams, name)); err != nil {
			return fmt.Errorf("remove stream %s: %w", name, err)
		}

		return finishStreamDeletions(streams)
	})
}

// eachStreamsDir calls visit with the streams directory of every account in store.
func eachStreamsDir(store string, visit func(streams string) error) error {
	accounts, err := os.ReadDir(filepath.Join(store, jetStreamDir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("list the bus store: %w", err)
	}

	for _, account := range accounts {
		if !account.IsDir() {
			continue
		}

		if err := visit(filepath.Join(store, jetStreamDir, account.Name(), streamsDir)); err != nil {
			return err
		}
	}

	return nil
}

// finishStreamDeletions removes the deleted streams and purged messages in one account's streams
// directory.
func finishStreamDeletions(streams string) error {
	entries, err := os.ReadDir(streams)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("list the streams in the bus store: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		removing := filepath.Join(streams, entry.Name(), purgedDir)
		if strings.HasPrefix(entry.Name(), ".") {
			removing = filepath.Join(streams, entry.Name())
		}

		if err := os.RemoveAll(removing); err != nil {
			return fmt.Errorf("finish removing %s: %w", removing, err)
		}
	}

	return nil
}

// checkpointDir refuses a checkpoint directory that exists, whose parent does not, or that lies
// inside the store a restore replaces.
func (c *Corpus) checkpointDir(dir string) error {
	if _, err := os.Lstat(dir); !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: %s already exists", errCheckpointDir, dir)
	}

	if _, err := os.Stat(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("%w: %s: %w", errCheckpointDir, dir, err)
	}

	store, err := filepath.Abs(c.dir)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", errCheckpointDir, dir, err)
	}

	target, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", errCheckpointDir, dir, err)
	}

	if relative, relErr := filepath.Rel(store, target); relErr == nil && !strings.HasPrefix(relative, "..") {
		return fmt.Errorf("%w: %s lies inside the store %s, which a restore replaces", errCheckpointDir, dir, c.dir)
	}

	return nil
}

// refuseMemoryStorage names every stream and consumer whose state a restart would drop.
func (c *Corpus) refuseMemoryStorage(ctx context.Context) error {
	var volatile []string

	streams := c.stream.ListStreams(ctx)
	for info := range streams.Info() {
		if info.Config.Storage == jetstream.MemoryStorage {
			volatile = append(volatile, "stream "+info.Config.Name)
		}

		consumers, err := c.memoryConsumers(ctx, info.Config.Name)
		if err != nil {
			return err
		}

		volatile = append(volatile, consumers...)
	}

	if err := streams.Err(); err != nil {
		return fmt.Errorf("list the bus's streams: %w", err)
	}

	if len(volatile) == 0 {
		return nil
	}

	slices.Sort(volatile)

	return fmt.Errorf("%w: %s", ErrMemoryStorage, strings.Join(volatile, ", "))
}

// memoryConsumers names the consumers on one stream that keep their state in memory.
func (c *Corpus) memoryConsumers(ctx context.Context, name string) ([]string, error) {
	stream, err := c.stream.Stream(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("open stream %s: %w", name, err)
	}

	var volatile []string

	consumers := stream.ListConsumers(ctx)
	for info := range consumers.Info() {
		if info.Config.MemoryStorage {
			volatile = append(volatile, fmt.Sprintf("consumer %s on stream %s", info.Name, name))
		}
	}

	if err := consumers.Err(); err != nil {
		return nil, fmt.Errorf("list the consumers of stream %s: %w", name, err)
	}

	return volatile, nil
}

// restart stops the server, lets replace work on the stopped store, and starts it again on ports it
// has not served on before.
func (c *Corpus) restart(ctx context.Context, replace func() error) error {
	client, monitor := c.ports()

	c.stop()

	if err := replace(); err != nil {
		return fmt.Errorf("%w: %w", ErrCheckpoint, err)
	}

	for range freshRestarts + 1 {
		if err := c.boot(ctx); err != nil {
			return fmt.Errorf("%w: restart the bus: %w", ErrCheckpoint, err)
		}

		now, nowMonitor := c.ports()
		if now != client && nowMonitor != monitor {
			return nil
		}

		c.stop()
	}

	return fmt.Errorf("%w: %w (client %d, monitoring %d)", ErrCheckpoint, errStalePort, client, monitor)
}

// ports are the client and monitoring ports the running server serves on.
func (c *Corpus) ports() (int, uint16) {
	var client int

	if addr, isTCP := c.server.Addr().(*net.TCPAddr); isTCP {
		client = addr.Port
	}

	return client, c.MonitorAddr().Port()
}

// stop closes Stutter's connection and shuts the server down, waiting for JetStream's final flush.
func (c *Corpus) stop() {
	if c.conn != nil {
		c.conn.Close()
	}

	shutdown(c.server)
}
