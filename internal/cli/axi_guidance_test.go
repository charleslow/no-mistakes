package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/kunchenguid/no-mistakes/internal/gatecontext"
	"github.com/kunchenguid/no-mistakes/internal/gateguidance"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// canonicalStaleMonitorPhrases are the load-bearing claims of the corrected
// "PR fell behind / conflicted after checks pass" guidance: the live CI monitor
// auto-rebases and re-pushes such a PR, so the agent runs no command and never
// hand-rebases, and `no-mistakes rerun` is only the dead-monitor recovery.
var canonicalStaleMonitorPhrases = []string{
	"never hand-rebase",
	"re-pushes",
	"no-mistakes rerun",
}

var canonicalPreserveGateFixPhrases = []string{
	"post-pipeline",
	"on top",
	"every pipeline fix commit",
}

const canonicalPipelineAgentPrerequisite = "a supported native agent binary, the `agent: cursor` ACP alias, or an explicit `acp:<target>` through `acpx`"

// TestStaleMonitorGuidance_InChecksPassedOutput ensures the guidance reaches the
// agent at its point of use: the `checks-passed` axi output, where the agent
// decides what to do about the still-monitored PR.
func TestStaleMonitorGuidance_InChecksPassedOutput(t *testing.T) {
	run := &ipc.RunInfo{
		ID:      "run-1",
		Branch:  "feature/x",
		Status:  types.RunRunning, // not terminal: daemon keeps monitoring until merge
		HeadSHA: "abcdef1234567890",
		PRURL:   strptr("https://github.com/user/repo/pull/42"),
		Steps: []ipc.StepResultInfo{
			{StepName: types.StepCI, Status: types.StepStatusRunning},
		},
	}

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	if err := renderDriveResult(cmd, run, true); err != nil {
		t.Fatalf("checks-passed must exit 0, got error: %v", err)
	}

	got := out.String()
	for _, phrase := range canonicalStaleMonitorPhrases {
		if !strings.Contains(got, phrase) {
			t.Errorf("checks-passed output missing stale-monitor guidance phrase %q in:\n%s", phrase, got)
		}
	}
}

func TestBranchSyncGuidance_InCommandHelp(t *testing.T) {
	for name, cmd := range map[string]*cobra.Command{
		"sync command help":     newSyncCmd(),
		"axi sync command help": newAxiSyncCmd(),
	} {
		var help bytes.Buffer
		cmd.SetOut(&help)
		cmd.SetErr(&help)
		cmd.SetArgs([]string{"--help"})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("render %s: %v", name, err)
		}
		for _, phrase := range []string{"user_owned", "empty-diff/already-delivered outcome", "submitted head"} {
			if !strings.Contains(help.String(), phrase) {
				t.Errorf("%s is missing user-owned release phrase %q:\n%s", name, phrase, help.String())
			}
		}
	}
}

func TestPipelineAgentPrerequisiteGuidance_InHelp(t *testing.T) {
	cmd := newAxiRunCmd()
	var help bytes.Buffer
	cmd.SetOut(&help)
	cmd.SetErr(&help)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("render axi run help: %v", err)
	}
	if !strings.Contains(strings.Join(strings.Fields(help.String()), " "), canonicalPipelineAgentPrerequisite) {
		t.Fatalf("axi run help is missing pipeline-agent prerequisite %q:\n%s", canonicalPipelineAgentPrerequisite, help.String())
	}
}

func TestGateStepBoundaryGuidance_SyncedAcrossSurfaces(t *testing.T) {
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	_ = emitGateContextRefusal(cmd, gatecontext.Result{Nested: true, RunID: "run-1", Phase: types.StepDocument})
	surfaces := map[string]string{
		"prompt boundary": gateguidance.PromptBoundary("document"),
		"live refusal":    out.String(),
	}
	phrases := []string{"assigned phase", "outer executor", "push", "PR", "CI"}
	for name, content := range surfaces {
		for _, phrase := range phrases {
			if !strings.Contains(content, phrase) {
				t.Errorf("%s is missing gate-step boundary phrase %q", name, phrase)
			}
		}
	}
	if !strings.Contains(surfaces["live refusal"], "nested_gate_context") {
		t.Error("live refusal is missing structured nested-context error code")
	}
}

func TestNormalDriveOutputDoesNotFloodBranchSyncGuidance(t *testing.T) {
	got := renderDriveResultForGuidanceTest(t, true, types.RunRunning)
	if strings.Contains(got, branchSyncAgentGuidance) || strings.Contains(got, "branch_sync.next_action") {
		t.Fatalf("ordinary drive output included irrelevant branch-sync guidance:\n%s", got)
	}
}

func TestPreserveGateFixGuidance_InPointOfUseOutputs(t *testing.T) {
	gate := stepView{
		Name:   "review",
		Status: "awaiting_approval",
		FindingsJSON: findingsJSON(t, []types.Finding{
			{ID: "review-1", Severity: "warning", File: "main.go", Action: types.ActionAskUser, Description: "calls os.Exit"},
		}, "1 blocking issue"),
	}
	surfaces := map[string]string{
		"gate output":          axiDoc(gateFields(gate)...),
		"checks-passed output": renderDriveResultForGuidanceTest(t, true, types.RunRunning),
		"failed output":        renderDriveResultForGuidanceTest(t, false, types.RunFailed),
	}
	for name, content := range surfaces {
		for _, phrase := range canonicalPreserveGateFixPhrases {
			if !strings.Contains(content, phrase) {
				t.Errorf("%s is missing the canonical preserve-gate-fix guidance phrase %q in:\n%s", name, phrase, content)
			}
		}
	}
}

func renderDriveResultForGuidanceTest(t *testing.T, ciReady bool, status types.RunStatus) string {
	t.Helper()
	run := &ipc.RunInfo{
		ID:      "run-1",
		Branch:  "feature/x",
		Status:  status,
		HeadSHA: "abcdef1234567890",
		PRURL:   strptr("https://github.com/user/repo/pull/42"),
		Steps: []ipc.StepResultInfo{
			{StepName: types.StepCI, Status: types.StepStatusRunning},
		},
	}

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	err := renderDriveResult(cmd, run, ciReady)
	var exit *exitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatalf("renderDriveResult returned unexpected error: %v", err)
	}
	return out.String()
}
