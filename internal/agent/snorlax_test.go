package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestSnorlaxAdapter_SchemaIsVisibleFromMountedWorktree(t *testing.T) {
	worktree := t.TempDir()
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
		if filepath.Dir(schemaPath) != worktree {
			writeBridgeError(conn, "schema is outside mounted worktree")
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

func contains(slice []string, want string) bool {
	for _, s := range slice {
		if s == want {
			return true
		}
	}
	return false
}
