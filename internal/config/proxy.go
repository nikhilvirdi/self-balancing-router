package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type ProxyConfig struct {
	Config
	ListenAddr      string
	ShutdownTimeout time.Duration
	Backends        []BackendConfig
}

func LoadProxy(lookupEnv func(string) (string, bool)) (ProxyConfig, error) {
	common, err := Load(lookupEnv)
	if err != nil {
		return ProxyConfig{}, err
	}
	cfg := ProxyConfig{Config: common, ListenAddr: ":8080", ShutdownTimeout: 15 * time.Second}
	if value, ok := lookupEnv("SBR_PROXY_LISTEN_ADDR"); ok {
		host, port, err := net.SplitHostPort(value)
		if err != nil {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_LISTEN_ADDR %q: %w", value, err)
		}
		if strings.ContainsAny(host, " \t\r\n/\\") {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_LISTEN_ADDR %q: invalid TCP host", value)
		}
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_LISTEN_ADDR %q: expected a numeric TCP port from 0 to 65535", value)
		}
		cfg.ListenAddr = value
	}
	if value, ok := lookupEnv("SBR_PROXY_SHUTDOWN_TIMEOUT"); ok {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_SHUTDOWN_TIMEOUT %q: expected a positive Go duration", value)
		}
		cfg.ShutdownTimeout = timeout
	}
	if value, ok := lookupEnv("SBR_PROXY_BACKENDS"); ok {
		cfg.Backends, err = parseBackends(value)
		if err != nil {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_BACKENDS: %w", err)
		}
	}
	return cfg, nil
}
