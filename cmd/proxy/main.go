package main

import (
	"os"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
	"github.com/nikhilvirdi/self-balancing-router/internal/logging"
)

func main() {
	const component = "proxy"
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		logging.New(os.Stderr, config.Config{}, component).Error("configuration failed", "error", err.Error())
		os.Exit(1)
	}
	logging.New(os.Stdout, cfg, component).Info("initialization succeeded")
}
