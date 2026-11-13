package config

import (
	"slices"
	"strings"
	"testing"
)

func TestLoadProxyBackends(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []BackendConfig
	}{
		{name: "empty"},
		{name: "whitespace only", value: " \t "},
		{
			name:  "ordered DNS IPv4 and IPv6",
			value: " z-9 = http://BACKEND-3.example.:80/ , ipv4=http://127.0.0.1:1, ipv6 = http://[2001:db8::1]:65535 ",
			want: []BackendConfig{
				{ID: "z-9", URL: "http://BACKEND-3.example.:80/"},
				{ID: "ipv4", URL: "http://127.0.0.1:1"},
				{ID: "ipv6", URL: "http://[2001:db8::1]:65535"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadProxy(func(key string) (string, bool) {
				return tt.value, key == "SBR_PROXY_BACKENDS"
			})
			if err != nil || !slices.Equal(cfg.Backends, tt.want) {
				t.Fatalf("LoadProxy() backends = %v, %v, want %v", cfg.Backends, err, tt.want)
			}
		})
	}
}

func TestLoadProxyInvalidBackends(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{"missing separator", "backend-a", "entry 1"},
		{"empty id", "=http://backend-a:8080", "entry 1"},
		{"uppercase id", "Backend=http://backend-a:8080", "entry 1"},
		{"leading digit", "1a=http://backend-a:8080", "entry 1"},
		{"underscore id", "backend_a=http://backend-a:8080", "entry 1"},
		{"overlong id", strings.Repeat("a", 64) + "=http://backend-a:8080", "entry 1"},
		{"missing URL", "backend-a=", "entry 1"},
		{"duplicate id", "a=http://first:80, a = http://second:80", "entry 2: duplicate"},
		{"empty element", "a=http://first:80,,b=http://second:80", "entry 2"},
		{"trailing separator", "a=http://first:80, ", "entry 2"},
		{"relative URL", "a=first:80", "entry 1"},
		{"opaque URL", "a=http:first:80", "entry 1"},
		{"malformed URL", "a=http://first host:80", "entry 1"},
		{"unsupported scheme", "a=https://first:80", "entry 1"},
		{"empty hostname", "a=http://:80", "entry 1"},
		{"invalid DNS name", "a=http://-first.example:80", "entry 1"},
		{"empty DNS label", "a=http://first..example:80", "entry 1"},
		{"overlong DNS label", "a=http://" + strings.Repeat("a", 64) + ".example:80", "entry 1"},
		{"invalid IPv4", "a=http://127.0.0.256:80", "entry 1"},
		{"unbracketed IPv6", "a=http://::1:80", "entry 1"},
		{"invalid IPv6", "a=http://[2001:db8::g]:80", "entry 1"},
		{"bracketed DNS", "a=http://[first]:80", "entry 1"},
		{"missing port", "a=http://first", "entry 1"},
		{"empty port", "a=http://first:", "entry 1"},
		{"named port", "a=http://first:http", "entry 1"},
		{"zero port", "a=http://first:0", "entry 1"},
		{"negative port", "a=http://first:-1", "entry 1"},
		{"out of range port", "a=http://first:65536", "entry 1"},
		{"credentials", "a=http://sensitive:password@first:80", "entry 1"},
		{"malformed credentials", "a=http://sensitive:%zz@first:80", "entry 1"},
		{"query", "a=http://first:80/?token=sensitive", "entry 1"},
		{"empty query", "a=http://first:80?", "entry 1"},
		{"fragment", "a=http://first:80/#sensitive", "entry 1"},
		{"empty fragment", "a=http://first:80#", "entry 1"},
		{"path", "a=http://first:80/sensitive", "entry 1"},
		{"escaped path", "a=http://first:80/%2F", "entry 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadProxy(func(key string) (string, bool) {
				return tt.value, key == "SBR_PROXY_BACKENDS"
			})
			if err == nil || !strings.Contains(err.Error(), "SBR_PROXY_BACKENDS: "+tt.wantErr) {
				t.Fatalf("LoadProxy() = %+v, %v, want configuration error for %s", cfg, err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "password") {
				t.Fatalf("configuration error exposed URL contents: %v", err)
			}
		})
	}
}
