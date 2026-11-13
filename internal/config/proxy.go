package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type ProxyPolicy string

const (
	PolicyFirstEligible ProxyPolicy = "first-eligible"
	PolicyRoundRobin    ProxyPolicy = "round-robin"
	PolicyLeastRequests ProxyPolicy = "least-requests"
	PolicyP2C           ProxyPolicy = "p2c"
)

type ProxyConfig struct {
	Config
	ListenAddr            string
	ShutdownTimeout       time.Duration
	DialTimeout           time.Duration
	ResponseHeaderTimeout time.Duration
	Backends              []BackendConfig
	HealthCheck           HealthCheckConfig
	Policy                ProxyPolicy
	P2CSeed               int64
}

func LoadProxy(lookupEnv func(string) (string, bool)) (ProxyConfig, error) {
	common, err := Load(lookupEnv)
	if err != nil {
		return ProxyConfig{}, err
	}
	cfg := ProxyConfig{
		Config:                common,
		ListenAddr:            ":8080",
		ShutdownTimeout:       15 * time.Second,
		DialTimeout:           3 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		Policy:                PolicyFirstEligible,
		P2CSeed:               1,
	}
	if value, ok := lookupEnv("SBR_PROXY_POLICY"); ok {
		policy := ProxyPolicy(strings.TrimSpace(value))
		switch policy {
		case PolicyFirstEligible, PolicyRoundRobin, PolicyLeastRequests, PolicyP2C:
			cfg.Policy = policy
		default:
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_POLICY %q: expected %q, %q, %q, or %q", value, PolicyFirstEligible, PolicyRoundRobin, PolicyLeastRequests, PolicyP2C)
		}
	}
	if value, ok := lookupEnv("SBR_PROXY_P2C_SEED"); ok {
		seed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_P2C_SEED %q: expected a signed 64-bit integer", value)
		}
		cfg.P2CSeed = seed
	}
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
	if value, ok := lookupEnv("SBR_PROXY_DIAL_TIMEOUT"); ok {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_DIAL_TIMEOUT %q: expected a positive Go duration", value)
		}
		cfg.DialTimeout = timeout
	}
	if value, ok := lookupEnv("SBR_PROXY_RESPONSE_HEADER_TIMEOUT"); ok {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout < 0 {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_RESPONSE_HEADER_TIMEOUT %q: expected a non-negative Go duration", value)
		}
		cfg.ResponseHeaderTimeout = timeout
	}
	if value, ok := lookupEnv("SBR_PROXY_BACKENDS"); ok {
		cfg.Backends, err = parseBackends(value)
		if err != nil {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_BACKENDS: %w", err)
		}
	}
	cfg.HealthCheck, err = loadHealthCheck(lookupEnv)
	if err != nil {
		return ProxyConfig{}, err
	}
	return cfg, nil
}
