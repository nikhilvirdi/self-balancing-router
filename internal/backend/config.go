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

type Config struct {
	config.Config
	ID              string
	ListenAddr      string
	ShutdownTimeout time.Duration
}

var backendID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

func LoadConfig(lookupEnv func(string) (string, bool)) (Config, error) {
	common, err := config.Load(lookupEnv)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Config:          common,
		ID:              "backend",
		ListenAddr:      ":8080",
		ShutdownTimeout: 15 * time.Second,
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

	return cfg, nil
}

