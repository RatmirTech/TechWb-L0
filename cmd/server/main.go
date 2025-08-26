package main

import (
	"context"
	"github.com/ratmirtech/techwb-l0/internal/app"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app.Run(ctx)
}
