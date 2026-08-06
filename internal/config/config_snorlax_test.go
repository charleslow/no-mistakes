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

// agent_args_override.snorlax is accepted (and reuses codex's reserved-flag set).
func TestAgentArgsOverride_SnorlaxAccepted(t *testing.T) {
	if err := validateAgentArgsOverride(map[string][]string{
		"snorlax": {"-m", "gpt-5.5"},
	}); err != nil {
		t.Fatalf("expected snorlax override accepted, got: %v", err)
	}
	// A codex-reserved flag must also be reserved for snorlax (same argv shape).
	if err := validateAgentArgsOverride(map[string][]string{
		"snorlax": {"--json"},
	}); err == nil || !strings.Contains(err.Error(), "cannot be overridden") {
		t.Errorf("expected --json reserved for snorlax, got: %v", err)
	}
}
