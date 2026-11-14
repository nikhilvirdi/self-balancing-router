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
	PolicyFirstEligible    ProxyPolicy = "first-eligible"
	PolicyRoundRobin       ProxyPolicy = "round-robin"
	PolicyLeastRequests    ProxyPolicy = "least-requests"
	PolicyP2C              ProxyPolicy = "p2c"
	PolicyP2CPeakEWMA      ProxyPolicy = "p2c-peak-ewma"
	PolicyUCB1             ProxyPolicy = "ucb1"
	PolicySlidingWindowUCB ProxyPolicy = "sliding-window-ucb"
	PolicyDiscountedUCB    ProxyPolicy = "discounted-ucb"
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
	PeakEWMADecay         time.Duration
	PeakEWMAInitialRTT    time.Duration
	MABLatencyScale       time.Duration
	MABSWWindow           int
	MABDiscountFactor     float64
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
		PeakEWMADecay:         10 * time.Second,
		PeakEWMAInitialRTT:    10 * time.Millisecond,
		MABLatencyScale:       100 * time.Millisecond,
		MABSWWindow:           1000,
		MABDiscountFactor:     0.99,
	}
	if value, ok := lookupEnv("SBR_PROXY_POLICY"); ok {
		policy := ProxyPolicy(strings.TrimSpace(value))
		switch policy {
		case PolicyFirstEligible, PolicyRoundRobin, PolicyLeastRequests, PolicyP2C, PolicyP2CPeakEWMA, PolicyUCB1, PolicySlidingWindowUCB, PolicyDiscountedUCB:
			cfg.Policy = policy
		default:
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_POLICY %q: expected %q, %q, %q, %q, %q, %q, %q, or %q", value, PolicyFirstEligible, PolicyRoundRobin, PolicyLeastRequests, PolicyP2C, PolicyP2CPeakEWMA, PolicyUCB1, PolicySlidingWindowUCB, PolicyDiscountedUCB)
		}
	}
	if value, ok := lookupEnv("SBR_PROXY_P2C_SEED"); ok {
		seed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_P2C_SEED %q: expected a signed 64-bit integer", value)
		}
		cfg.P2CSeed = seed
	}
	for _, setting := range []struct {
		key     string
		target  *time.Duration
		maximum time.Duration
	}{
		{"SBR_PROXY_PEAK_EWMA_DECAY", &cfg.PeakEWMADecay, 24 * time.Hour},
		{"SBR_PROXY_PEAK_EWMA_INITIAL_RTT", &cfg.PeakEWMAInitialRTT, time.Hour},
	} {
		if value, ok := lookupEnv(setting.key); ok {
			duration, err := time.ParseDuration(value)
			if err != nil || duration <= 0 || duration > setting.maximum {
				return ProxyConfig{}, fmt.Errorf("invalid %s %q: expected a positive Go duration no greater than %s", setting.key, value, setting.maximum)
			}
			*setting.target = duration
		}
	}
	if value, ok := lookupEnv("SBR_PROXY_MAB_LATENCY_SCALE"); ok {
		scale, err := time.ParseDuration(value)
		if err != nil || scale <= 0 {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_MAB_LATENCY_SCALE %q: expected a positive Go duration", value)
		}
		cfg.MABLatencyScale = scale
	}
	if value, ok := lookupEnv("SBR_PROXY_MAB_SW_WINDOW"); ok {
		window, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || window <= 0 {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_MAB_SW_WINDOW %q: expected a positive integer", value)
		}
		cfg.MABSWWindow = window
	}
	if value, ok := lookupEnv("SBR_PROXY_MAB_DISCOUNT_FACTOR"); ok {
		factor, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || !(factor > 0 && factor < 1) {
			return ProxyConfig{}, fmt.Errorf("invalid SBR_PROXY_MAB_DISCOUNT_FACTOR %q: expected a finite number strictly between 0 and 1", value)
		}
		cfg.MABDiscountFactor = factor
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
