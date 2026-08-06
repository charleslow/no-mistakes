package agent

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/snorlax"
)

// snorlaxAgent is the no-mistakes-side adapter that forwards each LLM
// invocation to Snorlax's controlled container environment over a per-user
// Unix-socket bridge (plan-no-mistakes.md). It owns the native codex argv
// (reusing codexAgent.buildArgs so the model/config surface matches upstream
// codex exactly) and parses the native JSONL stream itself; the bridge is a
// faithful streaming runner that selects a fixed in-container entrypoint,
// applies OneCLI credentials/CA/UID:GID/timeout, confines cwd to the mounted
// NM_HOME root, and streams raw stdout/stderr back. The pilot is codex-only;
// the protocol carries an `agent` field so Claude/GLM-5.2 can be added behind
// equivalent stream/resume/cancellation tests without a protocol rev.
//
// The adapter knows neither Docker nor credentials: it speaks only the framed
// socket protocol (src/nm/protocol.ts) and codex's JSONL. Rollback to direct
// `agent: codex` is config-only (switch the agent name back).
type snorlaxAgent struct {
	// codex supplies buildArgs, schema handling, and the JSONL parser so the
	// snorlax adapter's invocation shape is byte-for-byte upstream codex.
	codex *codexAgent
	// socketPath is resolved at Run time (env/default); injectable for tests.
	socketPath string
}

func (a *snorlaxAgent) Name() string { return "snorlax" }

// SupportsSessionResume mirrors codex: the bridge forwards `codex exec resume
// <id>` unchanged and the JSONL thread.started event carries the identity back.
func (a *snorlaxAgent) SupportsSessionResume() bool { return true }

func (a *snorlaxAgent) ReportsAgentAttempts() bool { return true }

// NeutralizesGateInstructions delegates to the embedded codex adapter: the
// argv it forwards carries the same project-settings suppression flags.
func (a *snorlaxAgent) NeutralizesGateInstructions() bool {
	return a.codex.NeutralizesGateInstructions()
}

func (a *snorlaxAgent) Close() error { return nil }

func (a *snorlaxAgent) Run(ctx context.Context, opts RunOpts) (*Result, error) {
	return runWithRetry(ctx, "snorlax", opts, claudeMaxRetries, classifyTransient, nil, func() (*Result, error) {
		return a.runOnce(ctx, opts)
	})
}

// --- bridge wire protocol (mirrors src/nm/protocol.ts) ----------------------

const (
	snorlaxFrameRequest  = 0x01
	snorlaxFrameStdin    = 0x02
	snorlaxFrameStdinEOF = 0x03
	snorlaxFrameCancel   = 0x04
	snorlaxFrameStarted  = 0x10
	snorlaxFrameStdout   = 0x11
	snorlaxFrameStderr   = 0x12
	snorlaxFrameExit     = 0x13
	snorlaxFrameError    = 0x14

	snorlaxMaxFramePayload = 4 * 1024 * 1024
)

type snorlaxBridgeRequest struct {
	Agent       string             `json:"agent"`
	Argv        []string           `json:"argv"`
	Cwd         string             `json:"cwd"`
	ExecutionID string             `json:"executionId"`
	Session     *snorlaxSessionRef `json:"session,omitempty"`
}

type snorlaxSessionRef struct {
	ID    string `json:"id"`
	Agent string `json:"agent"`
}

type snorlaxStartedPayload struct {
	ContainerName string `json:"containerName"`
}

type snorlaxExitPayload struct {
	Code     int  `json:"code"`
	TimedOut bool `json:"timedOut"`
}

type snorlaxErrorPayload struct {
	Message string `json:"message"`
}

func snorlaxWriteFrame(w io.Writer, typ byte, payload []byte) error {
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	header[4] = typ
	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func snorlaxReadFrame(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(header[:4])
	if n > snorlaxMaxFramePayload {
		return 0, nil, fmt.Errorf("snorlax bridge: frame payload %d exceeds max %d", n, snorlaxMaxFramePayload)
	}
	typ := header[4]
	payload := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, err
		}
	}
	return typ, payload, nil
}

// snorlaxBridgeSocket resolves the bridge socket path: explicit override
// (a.socketPath, e.g. from tests), else the shared per-user default
// (internal/snorlax.SocketPath, which honors SNORLAX_NM_SOCKET).
func (a *snorlaxAgent) bridgeSocket() string {
	if a.socketPath != "" {
		return a.socketPath
	}
	return snorlax.SocketPath()
}

func (a *snorlaxAgent) runOnce(ctx context.Context, opts RunOpts) (*Result, error) {
	schemaDir := ""
	if len(opts.JSONSchema) > 0 {
		var err error
		schemaDir, err = snorlaxSchemaDir()
		if err != nil {
			return nil, err
		}
	}
	schemaPath, validationSchema, schemaCleanup, err := prepareCodexSchemaInDir(opts.JSONSchema, schemaDir)
	if err != nil {
		return nil, err
	}
	if schemaCleanup != nil {
		defer schemaCleanup()
	}

	resumeID := ""
	var session *snorlaxSessionRef
	// RunSessions uses an empty SessionRef to represent a cold first turn. The
	// bridge validates session identities, so omit that placeholder rather than
	// serializing an invalid empty session object.
	if opts.Session != nil && opts.Session.ID != "" {
		resumeID = opts.Session.ID
		session = &snorlaxSessionRef{ID: opts.Session.ID, Agent: opts.Session.Agent}
	}
	// Reuse upstream codex argv verbatim so model/flags/sandbox/project-settings
	// surface matches `agent: codex` exactly (plan-no-mistakes.md §Contract).
	argv := a.codex.buildArgs(opts.Prompt, schemaPath, resumeID)

	conn, err := net.Dial("unix", a.bridgeSocket())
	if err != nil {
		return nil, fmt.Errorf("snorlax bridge unavailable at %s: %w (is src/host/nmBridge.ts running?)", a.bridgeSocket(), err)
	}
	cancelOnce := &sync.Once{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// On ctx cancellation, tell the bridge to kill the container. The server
	// then closes the socket; the reader below unblocks and the run resolves.
	go func() {
		<-ctx.Done()
		cancelOnce.Do(func() {
			_ = snorlaxWriteFrame(conn, snorlaxFrameCancel, nil)
		})
	}()

	req := snorlaxBridgeRequest{
		Agent:       "codex",
		Argv:        argv,
		Cwd:         opts.CWD,
		ExecutionID: fmt.Sprintf("nm-%d-%d", time.Now().UnixNano(), os.Getpid()),
		Session:     session,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("snorlax bridge request encode: %w", err)
	}
	if err := snorlaxWriteFrame(conn, snorlaxFrameRequest, reqJSON); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("snorlax bridge send: %w", err)
	}

	// Stream STDOUT frames into the JSONL parser via a pipe so OnChunk fires in
	// real time, exactly like the local codex adapter. STDERR is buffered and
	// used only to enrich error messages. STARTED/EXIT/ERROR are control frames.
	pr, pw := io.Pipe()
	type frameOutcome struct {
		exit      *snorlaxExitPayload
		bridgeErr string
		readErr   error
	}
	outcome := make(chan frameOutcome, 1)
	started := make(chan string, 1)

	go func() {
		defer pw.Close()
		for {
			typ, payload, rerr := snorlaxReadFrame(conn)
			if rerr != nil {
				outcome <- frameOutcome{readErr: rerr}
				return
			}
			switch typ {
			case snorlaxFrameStarted:
				var s snorlaxStartedPayload
				_ = json.Unmarshal(payload, &s)
				select {
				case started <- s.ContainerName:
				default:
				}
			case snorlaxFrameStdout:
				if len(payload) > 0 {
					if _, werr := pw.Write(payload); werr != nil {
						outcome <- frameOutcome{readErr: werr}
						return
					}
				}
			case snorlaxFrameStderr:
				// Best-effort: the parser does not consume stderr; the bridge's
				// audit log retains the full stream on the Snorlax side.
			case snorlaxFrameExit:
				var e snorlaxExitPayload
				_ = json.Unmarshal(payload, &e)
				outcome <- frameOutcome{exit: &e}
				return
			case snorlaxFrameError:
				var e snorlaxErrorPayload
				_ = json.Unmarshal(payload, &e)
				outcome <- frameOutcome{bridgeErr: e.Message}
				return
			}
		}
	}()

	// Surface the container start as a lifecycle event (no host pid; the bridge
	// owns the container). Non-blocking: STARTED may already have arrived.
	go func() {
		select {
		case name := <-started:
			emitLifecycle(opts, LifecycleEvent{
				Agent:   "snorlax",
				Phase:   LifecyclePhaseStart,
				Message: fmt.Sprintf("snorlax started container=%s", name),
			})
		case <-ctx.Done():
		}
	}()

	var usage TokenUsage
	var lastMessage string
	var codexErr string
	var threadID string
	metrics := newCodexMetricsAccumulator()
	parseErr := parseCodexEvents(ctx, pr, opts.OnChunk, &usage, &lastMessage, &codexErr, &threadID, metrics)

	var res *Result
	var retErr error
	select {
	case o := <-outcome:
		_ = conn.Close()
		switch {
		case o.bridgeErr != "":
			retErr = fmt.Errorf("snorlax bridge: %s", o.bridgeErr)
			emitAgentExited(opts, "snorlax", 0, retErr)
			return nil, retErr
		case o.exit != nil:
			if o.exit.TimedOut {
				retErr = fmt.Errorf("snorlax bridge: container timed out")
				emitAgentExited(opts, "snorlax", 0, retErr)
				return nil, retErr
			}
			if o.exit.Code != 0 {
				retErr = fmt.Errorf("snorlax bridge: codex exited code=%d%s", o.exit.Code, detailFromErr(codexErr))
				emitAgentExited(opts, "snorlax", 0, retErr)
				return nil, retErr
			}
		default:
			// Socket closed without EXIT. If ctx was cancelled, treat as cancel.
			if ctx.Err() != nil {
				emitAgentExited(opts, "snorlax", 0, ctx.Err())
				return nil, ctx.Err()
			}
			if errors.Is(o.readErr, io.EOF) || errors.Is(o.readErr, io.ErrUnexpectedEOF) {
				retErr = fmt.Errorf("snorlax bridge: connection closed unexpectedly")
				emitAgentExited(opts, "snorlax", 0, retErr)
				return nil, retErr
			}
			if o.readErr != nil {
				retErr = fmt.Errorf("snorlax bridge: read: %w", o.readErr)
				emitAgentExited(opts, "snorlax", 0, retErr)
				return nil, retErr
			}
		}
	case <-time.After(snorlaxResultWait):
		_ = conn.Close()
		retErr = fmt.Errorf("snorlax bridge: no terminal frame after parser returned")
		emitAgentExited(opts, "snorlax", 0, retErr)
		return nil, retErr
	}

	if parseErr != nil {
		retErr = fmt.Errorf("snorlax bridge: parse events: %w", parseErr)
		emitAgentExited(opts, "snorlax", 0, retErr)
		return nil, retErr
	}

	res, retErr = finalizeTextResult("snorlax", lastMessage, validationSchema, usage)
	if res != nil {
		res.SessionID = threadID
		res.Resumed = resumeID != ""
		res.SessionUsageCumulative = true
		m := metrics.metrics()
		res.Metrics = &m
		// resolveCodexModel reads a local rollout transcript that does not exist
		// on the no-mistakes host (codex ran inside the container); leave model
		// identity unknown rather than fabricate it.
	}
	emitAgentExited(opts, "snorlax", 0, retErr)
	return res, retErr
}

func snorlaxSchemaDir() (string, error) {
	p, err := paths.New()
	if err != nil {
		return "", fmt.Errorf("snorlax schema dir: %w", err)
	}
	dir := filepath.Join(p.Root(), "tmp", "codex-schemas")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("snorlax schema dir: %w", err)
	}
	return dir, nil
}

// snorlaxResultWait bounds how long the adapter waits for the terminal frame
// after the JSONL parser returns (the parser may finish before the bridge
// flushes EXIT, but never by much).
const snorlaxResultWait = 10 * time.Second

func detailFromErr(s string) string {
	if s == "" {
		return ""
	}
	return ": " + s
}
