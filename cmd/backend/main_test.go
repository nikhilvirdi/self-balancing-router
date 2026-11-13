package main

import (
	"net"
	"testing"
)

func TestRunConfigError(t *testing.T) {
	t.Setenv("SBR_BACKEND_ID", "INVALID_UPPERCASE")
	if code := run(); code != 1 {
		t.Fatalf("run() = %d, want 1 for configuration error", code)
	}
}

func TestRunListenError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	t.Setenv("SBR_BACKEND_LISTEN_ADDR", listener.Addr().String())
	if code := run(); code != 1 {
		t.Fatalf("run() = %d, want 1 for occupied address error", code)
	}
}

