package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/nikhilvirdi/self-balancing-router/internal/backend"
	"github.com/nikhilvirdi/self-balancing-router/internal/config"
	"github.com/nikhilvirdi/self-balancing-router/internal/logging"
)

func main() {
	os.Exit(run())
}

func run() int {
	const component = "backend"
	cfg, err := backend.LoadConfig(os.LookupEnv)
	if err != nil {
		logging.New(os.Stderr, config.Config{}, component).Error("configuration failed", "error", err.Error())
		return 1
	}

	logger := logging.New(os.Stdout, cfg.Config, component)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := backend.Run(ctx, cfg, logger); err != nil {
		logging.New(os.Stderr, cfg.Config, component).Error("backend server failed", "error", err)
		return 1
	}
	return 0
}
