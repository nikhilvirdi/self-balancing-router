package backend

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestComposeConfigurationFiles(t *testing.T) {
	basePath := filepath.Join("..", "..", "deployments", "compose.yaml")
	expPath := filepath.Join("..", "..", "deployments", "compose.experiments.yaml")

	baseData, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", basePath, err)
	}
	baseContent := string(baseData)

	// Base deployment must define proxy and five backends
	for _, svc := range []string{"proxy:", "backend-a:", "backend-b:", "backend-c:", "backend-d:", "backend-e:"} {
		if !strings.Contains(baseContent, svc) {
			t.Errorf("base compose.yaml missing service %s", svc)
		}
	}

	// Base deployment must have default proxy port 8080 and no admin ports published
	if !strings.Contains(baseContent, "127.0.0.1:${SBR_PROXY_PORT:-8080}:8080") {
		t.Errorf("base compose.yaml missing configurable proxy port with 8080 default")
	}
	if strings.Contains(baseContent, "9090") {
		t.Errorf("base compose.yaml should not publish administrative port 9090")
	}

	expData, err := os.ReadFile(expPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", expPath, err)
	}
	expContent := string(expData)

	// Experimental deployment must configure all five backends
	for _, svc := range []string{"backend-a:", "backend-b:", "backend-c:", "backend-d:", "backend-e:"} {
		if !strings.Contains(expContent, svc) {
			t.Errorf("compose.experiments.yaml missing service %s", svc)
		}
	}

	// Must bind admin ports exclusively to host loopback 127.0.0.1
	for _, port := range []string{"19091", "19092", "19093", "19094", "19095"} {
		if !strings.Contains(expContent, "127.0.0.1:") || !strings.Contains(expContent, port) {
			t.Errorf("compose.experiments.yaml missing loopback binding for port %s", port)
		}
	}

	// Must require SBR_BACKEND_ADMIN_TOKEN
	if !strings.Contains(expContent, "SBR_BACKEND_ADMIN_TOKEN: \"${SBR_BACKEND_ADMIN_TOKEN:?") {
		t.Errorf("compose.experiments.yaml must require SBR_BACKEND_ADMIN_TOKEN")
	}

	// Must enable round-robin policy by default
	if !strings.Contains(expContent, "SBR_PROXY_POLICY: \"${SBR_PROXY_POLICY:-round-robin}\"") {
		t.Errorf("compose.experiments.yaml must configure round-robin proxy policy")
	}
}

func TestDockerComposeConfigValidation(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found in PATH")
	}

	basePath := filepath.Join("..", "..", "deployments", "compose.yaml")
	expPath := filepath.Join("..", "..", "deployments", "compose.experiments.yaml")

	// 1. Base Compose config validation
	cmdBase := exec.Command("docker", "compose", "-f", basePath, "config", "--quiet")
	if out, err := cmdBase.CombinedOutput(); err != nil {
		t.Fatalf("docker compose config on base file failed: %v, output: %s", err, string(out))
	}

	// 2. Merged Compose config validation without token should fail
	cmdNoToken := exec.Command("docker", "compose", "-f", basePath, "-f", expPath, "config", "--quiet")
	// Ensure SBR_BACKEND_ADMIN_TOKEN is unset in environment
	cmdNoToken.Env = filterEnv(os.Environ(), "SBR_BACKEND_ADMIN_TOKEN")
	if out, err := cmdNoToken.CombinedOutput(); err == nil {
		t.Fatalf("docker compose config expected error with missing token, succeeded: %s", string(out))
	}

	// 3. Merged Compose config validation with token should succeed
	cmdWithToken := exec.Command("docker", "compose", "-f", basePath, "-f", expPath, "config", "--quiet")
	cmdWithToken.Env = append(filterEnv(os.Environ(), "SBR_BACKEND_ADMIN_TOKEN"), "SBR_BACKEND_ADMIN_TOKEN=test-token-1234567890")
	if out, err := cmdWithToken.CombinedOutput(); err != nil {
		t.Fatalf("docker compose config on merged files failed: %v, output: %s", err, string(out))
	}
}

func filterEnv(env []string, key string) []string {
	prefix := key + "="
	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

