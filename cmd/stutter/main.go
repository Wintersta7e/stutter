// Command stutter is the command-line entry point for the Stutter delivery-fault fuzzer.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Wintersta7e/stutter/internal/cli"
)

func main() {
	os.Exit(interruptible(func(ctx context.Context) int {
		return cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	}))
}

// interruptible runs command under a context that the first SIGINT, SIGTERM or SIGHUP cancels.
//
// That first signal starts teardown: a check provisions containers and holds connections, and an
// interrupt has to unwind them rather than leave the sandbox running. The signals are released
// before the context is cancelled, so a second one takes the default action and ends the process at
// once instead of waiting on a teardown that may be stuck.
func interruptible(command func(context.Context) int) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		select {
		case <-signals:
		case <-ctx.Done():
		}

		signal.Stop(signals)
		cancel()
	}()

	return command(ctx)
}
