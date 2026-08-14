package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

// fakeBridge is an in-process server speaking the snorlax bridge wire protocol
// (the server half), so the adapter can be exercised end-to-end without Docker
// or the Snorlax host. Each connection runs `handle`.
type fakeBridge struct {
	t         *testing.T
	socket    string
	ln        net.Listener
	handle    func(conn net.Conn, request *snorlaxBridgeRequest)
	lastReq   *snorlaxBridgeRequest
	mu        sync.Mutex
	gotCancel bool
}

func startFakeBridge(t *testing.T, handle func(conn net.Conn, request *snorlaxBridgeRequest)) *fakeBridge {
	t.Helper()
	dir := t.TempDir()
	socket := filepath.Join(dir, "nm.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fb := &fakeBridge{t: t, socket: socket, ln: ln, handle: handle}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go fb.serve(conn)
		}
	}()
	return fb
}

func (fb *fakeBridge) serve(conn net.Conn) {
	defer conn.Close()
	typ, payload, err := snorlaxReadFrame(conn)
	if err != nil {
		return
	}
	if typ != snorlaxFrameRequest {
		fb.t.Errorf("expected REQUEST frame, got %d", typ)
		return
	}
	var req snorlaxBridgeRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		fb.t.Errorf("REQUEST decode: %v", err)
		return
	}
	fb.mu.Lock()
	fb.lastReq = &req
	fb.mu.Unlock()
	fb.handle(conn, &req)
}

func (fb *fakeBridge) request() *snorlaxBridgeRequest {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	return fb.lastReq
}

func (fb *fakeBridge) close() {
	_ = fb.ln.Close()
}

func writeStarted(conn net.Conn, container string) {
	b, _ := json.Marshal(snorlaxStartedPayload{ContainerName: container})
	_ = snorlaxWriteFrame(conn, snorlaxFrameStarted, b)
}

func writeStdout(conn net.Conn, line string) {
	_ = snorlaxWriteFrame(conn, snorlaxFrameStdout, []byte(line+"\n"))
}

func writeExit(conn net.Conn, code int, timedOut bool) {
	b, _ := json.Marshal(snorlaxExitPayload{Code: code, TimedOut: timedOut})
	_ = snorlaxWriteFrame(conn, snorlaxFrameExit, b)
}

func writeBridgeError(conn net.Conn, msg string) {
	b, _ := json.Marshal(snorlaxErrorPayload{Message: msg})
	_ = snorlaxWriteFrame(conn, snorlaxFrameError, b)
}

func TestSnorlaxAdapter_NameAndCapabilities(t *testing.T) {
	a := &snorlaxAgent{codex: &codexAgent{}}
	if got := a.Name(); got != "snorlax" {
		t.Errorf("Name() = %q, want snorlax", got)
	}
	if !a.SupportsSessionResume() {
		t.Error("SupportsSessionResume() = false, want true")
	}
	if !a.ReportsAgentAttempts() {
		t.Error("ReportsAgentAttempts() = false, want true")
	}
}

func TestSnorlaxAdapter_ForwardsCodexArgvAndParsesJSONL(t *testing.T) {
	const prompt = "review the diff for bugs"
	fb := startFakeBridge(t, func(conn net.Conn, req *snorlaxBridgeRequest) {
		writeStarted(conn, "snorlax-test-1")
		writeStdout(conn, `{"type":"thread.started","thread_id":"th_abc"}`)
		writeStdout(conn, `{"type":"item.completed","item":{"id":"i1","type":"agent_message","text":"the answer"}}`)
		writeStdout(conn, `{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5,"cached_input_tokens":2,"reasoning_output_tokens":1}}`)
		writeExit(conn, 0, false)
	})
	defer fb.close()

	a := &snorlaxAgent{codex: &codexAgent{}, socketPath: fb.socket}
	var chunks []string
	res, err := a.Run(context.Background(), RunOpts{
		Prompt:  prompt,
		CWD:     "/home/u/nm/worktree",
		OnChunk: func(s string) { chunks = append(chunks, s) },
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The adapter forwards the exact upstream codex argv.
	req := fb.request()
	if req.Agent != "codex" {
		t.Errorf("request.Agent = %q, want codex", req.Agent)
	}
	if len(req.Argv) == 0 || req.Argv[0] != "exec" {
		t.Errorf("request.Argv = %v, want leading \"exec\"", req.Argv)
	}
	if !contains(req.Argv, "--json") {
		t.Errorf("request.Argv = %v, want --json", req.Argv)
	}
	if !contains(req.Argv, prompt) {
		t.Errorf("request.Argv = %v, want the prompt %q", req.Argv, prompt)
	}
	if req.Cwd != "/home/u/nm/worktree" {
		t.Errorf("request.Cwd = %q", req.Cwd)
	}
	if req.ExecutionID == "" {
		t.Error("request.ExecutionID is empty")
	}

	// JSONL parsed into Result.
	if res.Text != "the answer" {
		t.Errorf("Result.Text = %q, want \"the answer\"", res.Text)
	}
	if res.SessionID != "th_abc" {
		t.Errorf("Result.SessionID = %q, want th_abc", res.SessionID)
	}
	if res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 5 || res.Usage.CacheReadTokens != 2 {
		t.Errorf("Result.Usage = %+v", res.Usage)
	}
	if !res.UsageReported {
		t.Error("Result.UsageReported = false, want true")
	}
	if len(chunks) == 0 || chunks[len(chunks)-1] != "the answer" {
		t.Errorf("OnChunk stream = %v", chunks)
	}
}

func TestSnorlaxAdapter_UnavailableBridge(t *testing.T) {
	a := &snorlaxAgent{codex: &codexAgent{}, socketPath: filepath.Join(t.TempDir(), "missing.sock")}
	_, err := a.Run(context.Background(), RunOpts{Prompt: "x", CWD: "/tmp"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "snorlax bridge unavailable") {
		t.Errorf("err = %q, want \"bridge unavailable\"", err.Error())
	}
}

func TestSnorlaxAdapter_BridgeErrorFrame(t *testing.T) {
	fb := startFakeBridge(t, func(conn net.Conn, _ *snorlaxBridgeRequest) {
		writeBridgeError(conn, "unsupported agent \"claude\"")
	})
	defer fb.close()
	a := &snorlaxAgent{codex: &codexAgent{}, socketPath: fb.socket}
	_, err := a.Run(context.Background(), RunOpts{Prompt: "x", CWD: "/tmp"})
	if err == nil || !strings.Contains(err.Error(), "unsupported agent \"claude\"") {
		t.Errorf("err = %v, want bridge error passthrough", err)
	}
}

func TestSnorlaxAdapter_NonZeroExit(t *testing.T) {
	fb := startFakeBridge(t, func(conn net.Conn, _ *snorlaxBridgeRequest) {
		writeStarted(conn, "c")
		writeStdout(conn, `{"type":"error","message":"rate limited"}`)
		writeExit(conn, 1, false)
	})
	defer fb.close()
	a := &snorlaxAgent{codex: &codexAgent{}, socketPath: fb.socket}
	_, err := a.Run(context.Background(), RunOpts{Prompt: "x", CWD: "/tmp"})
	if err == nil || !strings.Contains(err.Error(), "exited code=1") {
		t.Errorf("err = %v, want exited code=1", err)
	}
}

func TestSnorlaxAdapter_Timeout(t *testing.T) {
	fb := startFakeBridge(t, func(conn net.Conn, _ *snorlaxBridgeRequest) {
		writeExit(conn, 0, true) // bridge reports a timeout
	})
	defer fb.close()
	a := &snorlaxAgent{codex: &codexAgent{}, socketPath: fb.socket}
	_, err := a.Run(context.Background(), RunOpts{Prompt: "x", CWD: "/tmp"})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Errorf("err = %v, want timed out", err)
	}
}

func TestSnorlaxAdapter_Cancellation(t *testing.T) {
	// Server accepts, sends STARTED, then holds until the client cancels (which
	// sends CANCEL); on seeing CANCEL the server closes so the adapter resolves.
	fb := startFakeBridge(t, func(conn net.Conn, _ *snorlaxBridgeRequest) {
		writeStarted(conn, "c")
		// Read until CANCEL or close.
		buf := make([]byte, 64)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
			// Best-effort CANCEL detection: the read loop drains the 5-byte
			// CANCEL frame; either way, closing ends the run.
			return
		}
	})
	defer fb.close()

	a := &snorlaxAgent{codex: &codexAgent{}, socketPath: fb.socket}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := a.Run(ctx, RunOpts{Prompt: "x", CWD: "/tmp"})
	if err == nil {
		t.Fatal("expected cancellation error, got nil")
	}
	// ctx deadline exceeded surfaces directly from the adapter's ctx check, or
	// as the underlying deadline-exceeded wrapped string.
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "deadline") {
		t.Errorf("err = %v, want context deadline exceeded", err)
	}
}

func TestSnorlaxAdapter_ModelSurfaceViaExtraArgs(t *testing.T) {
	// The plan requires the adapter's model/config surface match Snorlax's
	// models rather than impose a fixed policy: model selection flows through
	// agent_args_override.snorlax (extraArgs), which buildArgs injects.
	fb := startFakeBridge(t, func(conn net.Conn, _ *snorlaxBridgeRequest) {
		writeStarted(conn, "c")
		writeStdout(conn, `{"type":"item.completed","item":{"id":"i1","type":"agent_message","text":"ok"}}`)
		writeStdout(conn, `{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}`)
		writeExit(conn, 0, false)
	})
	defer fb.close()
	a := &snorlaxAgent{codex: &codexAgent{extraArgs: []string{"-m", "gpt-5.5"}}, socketPath: fb.socket}
	if _, err := a.Run(context.Background(), RunOpts{Prompt: "p", CWD: "/tmp"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	argv := fb.request().Argv
	if !contains(argv, "-m") || !contains(argv, "gpt-5.5") {
		t.Errorf("argv = %v, want -m gpt-5.5 injected from extraArgs", argv)
	}
}

func TestSnorlaxAdapter_SchemaIsVisibleFromMountedNMHomeScratch(t *testing.T) {
	nmHome := t.TempDir()
	worktree := t.TempDir()
	t.Setenv("NM_HOME", nmHome)
	schemaPaths := make(chan string, 1)
	fb := startFakeBridge(t, func(conn net.Conn, req *snorlaxBridgeRequest) {
		var schemaPath string
		for i := 0; i < len(req.Argv)-1; i++ {
			if req.Argv[i] == "--output-schema" {
				schemaPath = req.Argv[i+1]
				break
			}
		}
		if schemaPath == "" {
			writeBridgeError(conn, "missing output schema")
			return
		}
		if !isPathWithin(schemaPath, nmHome) {
			writeBridgeError(conn, "schema is outside mounted NM_HOME")
			return
		}
		if isPathWithin(schemaPath, worktree) {
			writeBridgeError(conn, "schema is inside source worktree")
			return
		}
		if _, err := os.ReadFile(schemaPath); err != nil {
			writeBridgeError(conn, "schema is not readable: "+err.Error())
			return
		}
		schemaPaths <- schemaPath
		writeStdout(conn, `{"type":"item.completed","item":{"id":"i1","type":"agent_message","text":"{\"ok\":true}"}}`)
		writeStdout(conn, `{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}`)
		writeExit(conn, 0, false)
	})
	defer fb.close()

	a := &snorlaxAgent{codex: &codexAgent{}, socketPath: fb.socket}
	if _, err := a.Run(context.Background(), RunOpts{
		Prompt:     "p",
		CWD:        worktree,
		JSONSchema: json.RawMessage(`{"type":"object","properties":{"ok":{"type":"boolean"}}}`),
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	schemaPath := <-schemaPaths
	if _, err := os.Stat(schemaPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("schema %q remains after Run: %v", schemaPath, err)
	}
	matches, err := filepath.Glob(filepath.Join(worktree, ".no-mistakes-codex-schema-*.json"))
	if err != nil {
		t.Fatalf("glob schema artifacts: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("schema artifacts in source worktree: %v", matches)
	}
}

func TestSnorlaxAdapter_ColdSessionPlaceholderIsNotForwarded(t *testing.T) {
	fb := startFakeBridge(t, func(conn net.Conn, req *snorlaxBridgeRequest) {
		if req.Session != nil {
			writeBridgeError(conn, "empty session placeholder was forwarded")
			return
		}
		writeStdout(conn, `{"type":"item.completed","item":{"id":"i1","type":"agent_message","text":"ok"}}`)
		writeStdout(conn, `{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}`)
		writeExit(conn, 0, false)
	})
	defer fb.close()

	a := &snorlaxAgent{codex: &codexAgent{}, socketPath: fb.socket}
	if _, err := a.Run(context.Background(), RunOpts{
		Prompt:  "p",
		CWD:     t.TempDir(),
		Session: &SessionRef{},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// The pi backend drives the pi-agent-cli bridge agent: it sends Agent:"pi",
// forwards the prompt via stdinB64 (pi reads its prompt from stdin, not argv),
// reuses piAgent.buildArgs for the argv, and parses pi's JSONL (agent_end).
func TestSnorlaxAdapter_PiBackend(t *testing.T) {
	const prompt = "create probe.txt containing hi"
	fb := startFakeBridge(t, func(conn net.Conn, req *snorlaxBridgeRequest) {
		// Echo the stdin prompt back so we can prove it round-tripped.
		stdin, _ := base64.StdEncoding.DecodeString(req.StdinB64)
		reply := "got prompt: " + string(stdin)
		writeStarted(conn, "snorlax-pi-1")
		writeStdout(conn, `{"type":"agent_end","messages":[{"role":"assistant","content":[{"type":"text","text":`+strconv.Quote(reply)+`}],"usage":{"input":12,"output":7}}]}`)
		writeExit(conn, 0, false)
	})
	defer fb.close()

	a := &snorlaxAgent{
		backend:    snorlaxBackendPi,
		pi:         &piAgent{extraArgs: []string{"--print", "--model", "inferx/deepseek-v4-flash-0731"}},
		socketPath: fb.socket,
	}
	res, err := a.Run(context.Background(), RunOpts{
		Prompt: prompt,
		CWD:    "/home/u/nm/worktree",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	req := fb.request()
	if req.Agent != "pi" {
		t.Errorf("request.Agent = %q, want pi", req.Agent)
	}
	// Prompt is carried via stdinB64, not argv.
	gotStdin, _ := base64.StdEncoding.DecodeString(req.StdinB64)
	if string(gotStdin) != prompt {
		t.Errorf("stdinB64 = %q, want prompt %q", string(gotStdin), prompt)
	}
	if contains(req.Argv, prompt) {
		t.Errorf("argv = %v, prompt must NOT be in argv for pi", req.Argv)
	}
	// Model + pi managed flags come from extraArgs/buildArgs.
	if !contains(req.Argv, "--model") || !contains(req.Argv, "inferx/deepseek-v4-flash-0731") {
		t.Errorf("argv = %v, want --model inferx/deepseek-v4-flash-0731", req.Argv)
	}
	if !contains(req.Argv, "--mode") || !contains(req.Argv, "json") || !contains(req.Argv, "--no-session") {
		t.Errorf("argv = %v, want --mode json --no-session", req.Argv)
	}

	if res.Text != "got prompt: "+prompt {
		t.Errorf("Result.Text = %q", res.Text)
	}
	if res.Usage.InputTokens != 12 || res.Usage.OutputTokens != 7 {
		t.Errorf("Result.Usage = %+v", res.Usage)
	}
	if !res.UsageReported {
		t.Error("Result.UsageReported = false, want true")
	}
}

func TestSnorlaxAdapter_PiBackendNoResume(t *testing.T) {
	a := &snorlaxAgent{backend: snorlaxBackendPi, pi: &piAgent{}}
	if a.SupportsSessionResume() {
		t.Error("pi backend SupportsSessionResume = true, want false (pi runs --no-session)")
	}
}

// NewWithOptions selects the snorlax backend from Options.SnorlaxBackend:
// "" or "codex" → codex (default), "pi" → pi; anything else is rejected.
func TestNewWithOptions_SnorlaxBackend(t *testing.T) {
	codexAgt, err := NewWithOptions(types.AgentSnorlax, "bin", nil, Options{})
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	sa := codexAgt.(*snorlaxAgent)
	if sa.backend != snorlaxBackendCodex {
		t.Errorf("default backend = %q, want codex", sa.backend)
	}
	if sa.codex == nil {
		t.Error("codex backend agent not set")
	}

	piAgt, err := NewWithOptions(types.AgentSnorlax, "bin", []string{"--print", "--model", "inferx/deepseek-v4-flash-0731"}, Options{SnorlaxBackend: "pi"})
	if err != nil {
		t.Fatalf("pi: %v", err)
	}
	sa2 := piAgt.(*snorlaxAgent)
	if sa2.backend != snorlaxBackendPi {
		t.Errorf("backend = %q, want pi", sa2.backend)
	}
	if sa2.pi == nil || !contains(sa2.pi.extraArgs, "inferx/deepseek-v4-flash-0731") {
		t.Errorf("pi backend agent/extraArgs not wired: %+v", sa2.pi)
	}

	if _, err := NewWithOptions(types.AgentSnorlax, "bin", nil, Options{SnorlaxBackend: "gemini"}); err == nil ||
		!strings.Contains(err.Error(), `must be "codex" or "pi"`) {
		t.Errorf("expected invalid backend error, got %v", err)
	}
}

// An assistant error in pi's event stream (stopReason:"error") surfaces as a
// Go error even though pi exits 0 — matching the native pi adapter.
func TestSnorlaxAdapter_PiBackendAssistantError(t *testing.T) {
	fb := startFakeBridge(t, func(conn net.Conn, _ *snorlaxBridgeRequest) {
		writeStarted(conn, "c")
		writeStdout(conn, `{"type":"agent_end","messages":[{"role":"assistant","stopReason":"error","errorMessage":"model timed out","content":[]}]}`)
		writeExit(conn, 0, false)
	})
	defer fb.close()
	a := &snorlaxAgent{backend: snorlaxBackendPi, pi: &piAgent{}, socketPath: fb.socket}
	_, err := a.Run(context.Background(), RunOpts{Prompt: "x", CWD: "/tmp"})
	if err == nil || !strings.Contains(err.Error(), "model timed out") {
		t.Errorf("err = %v, want pi reported error", err)
	}
}

func contains(slice []string, want string) bool {
	for _, s := range slice {
		if s == want {
			return true
		}
	}
	return false
}

func isPathWithin(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
