package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/richardrh/valet/internal/app"
	"github.com/richardrh/valet/internal/cli"
	"github.com/richardrh/valet/internal/diagnostics"
)

// version is replaced by release builds with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	os.Exit(run())
}

// run records which signal cancelled the command so cli can map SIGINT and
// SIGTERM to their conventional exit codes and keep other failures operational.
func run() int {
	diagnostics.IgnoreBrokenPipe()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		select {
		case sig := <-signals:
			cancel(cli.Interrupted{Signal: sig})
		case <-ctx.Done():
		}
	}()
	rt := app.Runtime{Stdout: os.Stdout, Stderr: os.Stderr, Version: version}
	return cli.Run(ctx, rt, os.Args[1:])
}
