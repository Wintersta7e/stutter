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
	// A check provisions containers and holds connections; an interrupt has to unwind them rather
	// than leave the sandbox running.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// Released explicitly rather than deferred: os.Exit does not run deferred functions.
	code := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)

	stop()
	os.Exit(code)
}
