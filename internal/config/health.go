package config

import (
	"fmt"
	"strconv"
	"time"
)

type HealthCheckConfig struct {
	Enabled           bool
	Interval          time.Duration
	Timeout           time.Duration
	FailureThreshold  int
	RecoveryThreshold int
}

func loadHealthCheck(lookupEnv func(string) (string, bool)) (HealthCheckConfig, error) {
	cfg := HealthCheckConfig{
		Enabled:           true,
		Interval:          5 * time.Second,
		Timeout:           2 * time.Second,
		FailureThreshold:  2,
		RecoveryThreshold: 2,
	}
	if value, ok := lookupEnv("SBR_PROXY_HEALTH_CHECK_ENABLED"); ok {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return HealthCheckConfig{}, fmt.Errorf("invalid SBR_PROXY_HEALTH_CHECK_ENABLED %q: expected a boolean", value)
		}
		cfg.Enabled = enabled
	}
	for _, setting := range []struct {
		key string
		dst *time.Duration
	}{
		{"SBR_PROXY_HEALTH_CHECK_INTERVAL", &cfg.Interval},
		{"SBR_PROXY_HEALTH_CHECK_TIMEOUT", &cfg.Timeout},
	} {
		if value, ok := lookupEnv(setting.key); ok {
			duration, err := time.ParseDuration(value)
			if err != nil || duration <= 0 {
				return HealthCheckConfig{}, fmt.Errorf("invalid %s %q: expected a positive Go duration", setting.key, value)
			}
			*setting.dst = duration
		}
	}
	if cfg.Timeout >= cfg.Interval {
		return HealthCheckConfig{}, fmt.Errorf("SBR_PROXY_HEALTH_CHECK_TIMEOUT (%s) must be less than SBR_PROXY_HEALTH_CHECK_INTERVAL (%s)", cfg.Timeout, cfg.Interval)
	}
	for _, setting := range []struct {
		key string
		dst *int
	}{
		{"SBR_PROXY_HEALTH_FAILURE_THRESHOLD", &cfg.FailureThreshold},
		{"SBR_PROXY_HEALTH_RECOVERY_THRESHOLD", &cfg.RecoveryThreshold},
	} {
		if value, ok := lookupEnv(setting.key); ok {
			threshold, err := strconv.Atoi(value)
			if err != nil || threshold <= 0 {
				return HealthCheckConfig{}, fmt.Errorf("invalid %s %q: expected a positive integer", setting.key, value)
			}
			*setting.dst = threshold
		}
	}
	return cfg, nil
}
