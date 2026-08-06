//go:build e2e

package e2e

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSnorlaxBridge speaks the production framed Unix-socket protocol and
// launches the recorded-fixture Codex fake. It deliberately permits only
// NM_HOME-visible schemas: a real Snorlax container mounts NM_HOME, not the
// host temporary directory.
type fakeSnorlaxBridge struct {
	t        *testing.T
	ln       net.Listener
	codexBin string
	nmHome   string

	mu       sync.Mutex
	requests []snorlaxBridgeRequest
	close    sync.Once
}

type snorlaxBridgeRequest struct {
	Agent       string   `json:"agent"`
	Argv        []string `json:"argv"`
	Cwd         string   `json:"cwd"`
	ExecutionID string   `json:"executionId"`
}

func startFakeSnorlaxBridge(t *testing.T, socket, codexBin, nmHome string) *fakeSnorlaxBridge {
	t.Helper()
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen on Snorlax bridge socket: %v", err)
	}
	b := &fakeSnorlaxBridge{t: t, ln: ln, codexBin: codexBin, nmHome: nmHome}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go b.serve(conn)
		}
	}()
	return b
}

func (b *fakeSnorlaxBridge) Close() {
	b.close.Do(func() { _ = b.ln.Close() })
}

func (b *fakeSnorlaxBridge) requestsSnapshot() []snorlaxBridgeRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]snorlaxBridgeRequest(nil), b.requests...)
}

func (b *fakeSnorlaxBridge) serve(conn net.Conn) {
	defer conn.Close()
	typ, payload, err := readSnorlaxFrame(conn)
	if err != nil {
		return
	}
	if typ != 0x01 {
		writeSnorlaxError(conn, "expected REQUEST first")
		return
	}
	var req snorlaxBridgeRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		writeSnorlaxError(conn, "REQUEST payload is not valid JSON")
		return
	}
	if err := b.validateRequest(req); err != nil {
		writeSnorlaxError(conn, err.Error())
		return
	}
	b.mu.Lock()
	b.requests = append(b.requests, req)
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, b.codexBin, req.Argv...)
	cmd.Dir = req.Cwd
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		writeSnorlaxError(conn, "open codex stdout: "+err.Error())
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		writeSnorlaxError(conn, "open codex stderr: "+err.Error())
		return
	}
	if err := cmd.Start(); err != nil {
		writeSnorlaxError(conn, "start codex: "+err.Error())
		return
	}
	started, _ := json.Marshal(map[string]string{"containerName": "snorlax-e2e"})
	if err := writeSnorlaxFrame(conn, 0x10, started); err != nil {
		return
	}

	var writes sync.Mutex
	var streams sync.WaitGroup
	forward := func(r io.Reader, frameType byte) {
		defer streams.Done()
		buf := make([]byte, 32*1024)
		for {
			n, readErr := r.Read(buf)
			if n > 0 {
				writes.Lock()
				writeErr := writeSnorlaxFrame(conn, frameType, buf[:n])
				writes.Unlock()
				if writeErr != nil {
					return
				}
			}
			if readErr != nil {
				return
			}
		}
	}
	streams.Add(2)
	go forward(stdout, 0x11)
	go forward(stderr, 0x12)
	err = cmd.Wait()
	streams.Wait()
	if ctx.Err() == context.DeadlineExceeded {
		finished, _ := json.Marshal(map[string]any{"code": -1, "timedOut": true})
		_ = writeSnorlaxFrame(conn, 0x13, finished)
		return
	}
	code := 0
	if err != nil {
		code = cmd.ProcessState.ExitCode()
	}
	finished, _ := json.Marshal(map[string]any{"code": code, "timedOut": false})
	_ = writeSnorlaxFrame(conn, 0x13, finished)
}

func (b *fakeSnorlaxBridge) validateRequest(req snorlaxBridgeRequest) error {
	if req.Agent != "codex" {
		return fmt.Errorf("unsupported agent %q", req.Agent)
	}
	if req.ExecutionID == "" || req.Cwd == "" || len(req.Argv) == 0 {
		return fmt.Errorf("invalid bridge request")
	}
	if !isWithin(req.Cwd, b.nmHome) {
		return fmt.Errorf("cwd must be under NM_HOME: %s", req.Cwd)
	}
	for i := 0; i < len(req.Argv)-1; i++ {
		if req.Argv[i] != "--output-schema" {
			continue
		}
		schema := req.Argv[i+1]
		if !isWithin(schema, b.nmHome) {
			return fmt.Errorf("output schema must be under NM_HOME: %s", schema)
		}
		if _, err := os.Stat(schema); err != nil {
			return fmt.Errorf("output schema is unavailable: %w", err)
		}
	}
	return nil
}

func isWithin(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func readSnorlaxFrame(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length > 4*1024*1024 {
		return 0, nil, fmt.Errorf("frame payload %d exceeds limit", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return header[4], payload, nil
}

func writeSnorlaxFrame(w io.Writer, typ byte, payload []byte) error {
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	header[4] = typ
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func writeSnorlaxError(w io.Writer, message string) {
	payload, _ := json.Marshal(map[string]string{"message": message})
	_ = writeSnorlaxFrame(w, 0x14, payload)
}

func assertSnorlaxBridgeJourney(t *testing.T, h *Harness) {
	t.Helper()
	if h.snorlaxBridge == nil {
		t.Fatal("Snorlax journey did not configure a bridge")
	}
	requests := h.snorlaxBridge.requestsSnapshot()
	invocations := h.AgentInvocations()
	if len(requests) == 0 {
		t.Fatal("Snorlax bridge received no agent invocations")
	}
	if len(requests) != len(invocations) {
		t.Fatalf("bridge requests = %d, fake Codex invocations = %d", len(requests), len(invocations))
	}
	for i, request := range requests {
		if request.Agent != "codex" {
			t.Errorf("request %d agent = %q, want codex", i, request.Agent)
		}
		if request.ExecutionID == "" {
			t.Errorf("request %d has no execution ID", i)
		}
		if !isWithin(request.Cwd, h.NMHome) {
			t.Errorf("request %d cwd %q escapes NM_HOME %q", i, request.Cwd, h.NMHome)
		}
		if !containsString(request.Argv, "exec") || !containsString(request.Argv, "--json") {
			t.Errorf("request %d argv = %v, want Codex exec JSONL arguments", i, request.Argv)
		}
		if !containsString(request.Argv, invocations[i].Prompt) {
			t.Errorf("request %d did not forward fake Codex prompt %q", i, invocations[i].Prompt)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
