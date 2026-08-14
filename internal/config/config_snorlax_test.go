package config

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

// `agent: snorlax` resolves as runnable iff the local bridge socket is present.
// Its availability does NOT consult exec.LookPath — the socket is the agent's
// "binary" (see internal/snorlax + resolveConfiguredAgent).
func TestResolveAgent_SnorlaxBridgeSocket(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "nm.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	defer os.Remove(socket)

	t.Setenv("SNORLAX_NM_SOCKET", socket)

	lookPathCalled := false
	cfg := &Config{Agent: types.AgentSnorlax}
	err = cfg.ResolveAgent(context.Background(), func(bin string) (string, error) {
		lookPathCalled = true
		return bin, nil
	})
	if err != nil {
		t.Fatalf("ResolveAgent with bridge up: %v", err)
	}
	if cfg.Agent != types.AgentSnorlax {
		t.Errorf("Agent = %q, want snorlax", cfg.Agent)
	}
	if lookPathCalled {
		t.Error("ResolveAgent called lookPath for snorlax; it should only stat the bridge socket")
	}
}

func TestResolveAgent_SnorlaxBridgeMissing(t *testing.T) {
	t.Setenv("SNORLAX_NM_SOCKET", filepath.Join(t.TempDir(), "missing.sock"))

	cfg := &Config{Agent: types.AgentSnorlax}
	err := cfg.ResolveAgent(context.Background(), func(bin string) (string, error) {
		t.Fatalf("lookPath should not be called for snorlax")
		return bin, nil
	})
	if err == nil {
		t.Fatal("expected error when the bridge socket is missing")
	}
	if !strings.Contains(err.Error(), "no runnable agent") {
		t.Errorf("err = %v, want \"no runnable agent\"", err)
	}
}

// agent_args_override.snorlax is accepted. Its reserved-flag set is the union
// of codex and pi (see config.go init), so managed flags of EITHER backend are
// protected.
func TestAgentArgsOverride_SnorlaxAccepted(t *testing.T) {
	if err := validateAgentArgsOverride(map[string][]string{
		"snorlax": {"-m", "gpt-5.5"},
	}); err != nil {
		t.Fatalf("expected snorlax override accepted, got: %v", err)
	}
	// A codex-reserved flag is reserved for snorlax too.
	if err := validateAgentArgsOverride(map[string][]string{
		"snorlax": {"--json"},
	}); err == nil || !strings.Contains(err.Error(), "cannot be overridden") {
		t.Errorf("expected --json reserved for snorlax, got: %v", err)
	}
}

// snorlax_backend selects the bridge's in-container CLI and is carried from
// global config through Merge into the resolved Config.
func TestLoadGlobal_SnorlaxBackend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("agent: snorlax\nsnorlax_backend: pi\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadGlobal(path)
	if err != nil {
		t.Fatalf("LoadGlobal: %v", err)
	}
	if cfg.SnorlaxBackend != "pi" {
		t.Errorf("SnorlaxBackend = %q, want pi", cfg.SnorlaxBackend)
	}
	merged := Merge(cfg, &RepoConfig{})
	if merged.SnorlaxBackend != "pi" {
		t.Errorf("merged SnorlaxBackend = %q, want pi", merged.SnorlaxBackend)
	}
}

func TestLoadGlobal_SnorlaxBackendInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("snorlax_backend: gemini\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadGlobal(path)
	if err == nil || !strings.Contains(err.Error(), `must be "codex" or "pi"`) {
		t.Errorf("err = %v, want snorlax_backend validation error", err)
	}
}

// pi's managed flags (--mode, --no-session) are reserved for snorlax too, so a
// pi-backend operator can't clobber no-mistakes' JSONL parsing flags.
func TestAgentArgsOverride_SnorlaxPiFlagsReserved(t *testing.T) {
	for _, reserved := range []string{"--mode", "--no-session"} {
		if err := validateAgentArgsOverride(map[string][]string{
			"snorlax": {reserved},
		}); err == nil || !strings.Contains(err.Error(), "cannot be overridden") {
			t.Errorf("expected %q reserved for snorlax, got: %v", reserved, err)
		}
	}
	if err := validateAgentArgsOverride(map[string][]string{
		"snorlax": {"--print", "--model", "inferx/deepseek-v4-flash-0731"},
	}); err != nil {
		t.Errorf("expected snorlax pi override accepted, got: %v", err)
	}
}
