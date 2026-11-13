package backend

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

const (
	defaultWorkers          = 4
	maxWorkers              = 1024
	defaultQueueCapacity    = 32
	maxQueueCapacity        = 65536
	defaultProcessingDelay  = 0 * time.Second
	maxProcessingDelay      = 5 * time.Minute
	defaultProcessingJitter = 0 * time.Second
	maxProcessingJitter     = 5 * time.Minute
	maxCombinedDelay        = 10 * time.Minute
	defaultDelaySeed        = int64(1)
	defaultAdminListenAddr  = "127.0.0.1:9090"
	minAdminTokenLength     = 16
)

type Config struct {
	config.Config
	ID               string
	ListenAddr       string
	ShutdownTimeout  time.Duration
	Workers          int
	QueueCapacity    int
	ProcessingDelay  time.Duration
	ProcessingJitter time.Duration
	DelaySeed        int64
	AdminEnabled     bool
	AdminListenAddr  string
	AdminToken       string
}

func validateWorkload(delay, jitter time.Duration) error {
	if delay < 0 || delay > maxProcessingDelay {
		return fmt.Errorf("invalid processing delay %v: expected a non-negative duration up to %v", delay, maxProcessingDelay)
	}
	if jitter < 0 || jitter > maxProcessingJitter {
		return fmt.Errorf("invalid processing jitter %v: expected a non-negative duration up to %v", jitter, maxProcessingJitter)
	}
	if delay > maxCombinedDelay-jitter {
		return fmt.Errorf("combined processing delay and jitter exceeds %v", maxCombinedDelay)
	}
	return nil
}

var backendID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

func LoadConfig(lookupEnv func(string) (string, bool)) (Config, error) {
	common, err := config.Load(lookupEnv)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Config:           common,
		ID:               "backend",
		ListenAddr:       ":8080",
		ShutdownTimeout:  15 * time.Second,
		Workers:          defaultWorkers,
		QueueCapacity:    defaultQueueCapacity,
		ProcessingDelay:  defaultProcessingDelay,
		ProcessingJitter: defaultProcessingJitter,
		DelaySeed:        defaultDelaySeed,
		AdminEnabled:     false,
		AdminListenAddr:  defaultAdminListenAddr,
		AdminToken:       "",
	}

	if value, ok := lookupEnv("SBR_BACKEND_ID"); ok {
		if !backendID.MatchString(value) {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_ID %q: expected a 1–63 character backend identifier starting with a lowercase letter and containing only lowercase letters, digits, or hyphens", value)
		}
		cfg.ID = value
	}

	if value, ok := lookupEnv("SBR_BACKEND_LISTEN_ADDR"); ok {
		host, port, err := net.SplitHostPort(value)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_LISTEN_ADDR %q: %w", value, err)
		}
		if strings.ContainsAny(host, " \t\r\n/\\") {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_LISTEN_ADDR %q: invalid TCP host", value)
		}
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_LISTEN_ADDR %q: expected a numeric TCP port from 0 to 65535", value)
		}
		cfg.ListenAddr = value
	}

	if value, ok := lookupEnv("SBR_BACKEND_SHUTDOWN_TIMEOUT"); ok {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_SHUTDOWN_TIMEOUT %q: expected a positive Go duration", value)
		}
		cfg.ShutdownTimeout = timeout
	}

	if value, ok := lookupEnv("SBR_BACKEND_WORKERS"); ok {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > maxWorkers {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_WORKERS %q: expected an integer from 1 to %d", value, maxWorkers)
		}
		cfg.Workers = n
	}

	if value, ok := lookupEnv("SBR_BACKEND_QUEUE_CAPACITY"); ok {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || n > maxQueueCapacity {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_QUEUE_CAPACITY %q: expected an integer from 0 to %d", value, maxQueueCapacity)
		}
		cfg.QueueCapacity = n
	}

	if value, ok := lookupEnv("SBR_BACKEND_PROCESSING_DELAY"); ok {
		d, err := time.ParseDuration(value)
		if err != nil || d < 0 || d > maxProcessingDelay {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_PROCESSING_DELAY %q: expected a non-negative duration up to %v", value, maxProcessingDelay)
		}
		cfg.ProcessingDelay = d
	}

	if value, ok := lookupEnv("SBR_BACKEND_PROCESSING_JITTER"); ok {
		d, err := time.ParseDuration(value)
		if err != nil || d < 0 || d > maxProcessingJitter {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_PROCESSING_JITTER %q: expected a non-negative duration up to %v", value, maxProcessingJitter)
		}
		cfg.ProcessingJitter = d
	}

	if cfg.ProcessingDelay > maxCombinedDelay-cfg.ProcessingJitter {
		return Config{}, fmt.Errorf("invalid SBR_BACKEND_PROCESSING_DELAY: combined processing delay and jitter exceeds %v", maxCombinedDelay)
	}

	if value, ok := lookupEnv("SBR_BACKEND_DELAY_SEED"); ok {
		seed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_DELAY_SEED %q: expected an integer", value)
		}
		cfg.DelaySeed = seed
	}

	if value, ok := lookupEnv("SBR_BACKEND_ADMIN_ENABLED"); ok {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_ADMIN_ENABLED %q: expected a boolean", value)
		}
		cfg.AdminEnabled = enabled
	}

	if value, ok := lookupEnv("SBR_BACKEND_ADMIN_LISTEN_ADDR"); ok {
		host, port, err := net.SplitHostPort(value)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_ADMIN_LISTEN_ADDR %q: %w", value, err)
		}
		if strings.ContainsAny(host, " \t\r\n/\\") {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_ADMIN_LISTEN_ADDR %q: invalid TCP host", value)
		}
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_ADMIN_LISTEN_ADDR %q: expected a numeric TCP port from 0 to 65535", value)
		}
		cfg.AdminListenAddr = value
	}

	if value, ok := lookupEnv("SBR_BACKEND_ADMIN_TOKEN"); ok {
		cfg.AdminToken = value
	}

	if cfg.AdminEnabled {
		if len(cfg.AdminToken) < minAdminTokenLength {
			return Config{}, fmt.Errorf("invalid SBR_BACKEND_ADMIN_TOKEN: expected a token with at least %d characters when administrative access is enabled", minAdminTokenLength)
		}
	}

	return cfg, nil
}
