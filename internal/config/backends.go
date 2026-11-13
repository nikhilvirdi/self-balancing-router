package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

type BackendConfig struct {
	ID  string
	URL string
}

var (
	backendID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	dnsLabel  = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)
)

func parseBackends(value string) ([]BackendConfig, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	var backends []BackendConfig
	seen := make(map[string]struct{})
	for i, entry := range strings.Split(value, ",") {
		id, address, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("entry %d: expected identifier=http://host:port", i+1)
		}
		backend := BackendConfig{ID: strings.TrimSpace(id), URL: strings.TrimSpace(address)}
		if err := backend.Validate(); err != nil {
			return nil, fmt.Errorf("entry %d: %w", i+1, err)
		}
		if _, exists := seen[backend.ID]; exists {
			return nil, fmt.Errorf("entry %d: duplicate backend identifier %q", i+1, backend.ID)
		}
		seen[backend.ID] = struct{}{}
		backends = append(backends, backend)
	}
	return backends, nil
}

func (b BackendConfig) Validate() error {
	if !backendID.MatchString(b.ID) {
		return errors.New("expected a 1–63 character backend identifier starting with a lowercase letter and containing only lowercase letters, digits, or hyphens")
	}
	// URL parse errors can include credentials or other sensitive input.
	u, err := url.Parse(b.URL)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.Opaque != "" {
		return errors.New("expected an absolute HTTP backend URL")
	}
	if u.User != nil || u.ForceQuery || u.RawQuery != "" || strings.Contains(b.URL, "#") {
		return errors.New("backend URL cannot contain credentials, a query, or a fragment")
	}
	if path := u.EscapedPath(); path != "" && path != "/" {
		return errors.New("backend URL path must be empty or /")
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return errors.New("backend URL requires a hostname and an explicit TCP port")
	}
	if !validBackendHost(host, strings.HasPrefix(u.Host, "[")) {
		return errors.New("backend URL requires a valid DNS name or IP address")
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return errors.New("backend URL requires a TCP port from 1 to 65535")
	}
	return nil
}

func validBackendHost(host string, bracketed bool) bool {
	addr, err := netip.ParseAddr(host)
	if bracketed {
		return err == nil && addr.Is6()
	}
	if err == nil {
		return addr.Is4()
	}
	name := strings.TrimSuffix(host, ".")
	if len(name) > 253 || strings.Trim(name, "0123456789.") == "" {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if !dnsLabel.MatchString(label) {
			return false
		}
	}
	return true
}
