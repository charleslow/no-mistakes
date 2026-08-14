package agent

import (
	"context"
	"encoding/base64"
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
type snorlaxBackendKind string

const (
	snorlaxBackendCodex snorlaxBackendKind = "codex"
	snorlaxBackendPi    snorlaxBackendKind = "pi"
)

type snorlaxAgent struct {
	backend snorlaxBackendKind
	// codex is used when backend == codex: it supplies buildArgs, schema
	// handling, and the JSONL parser so the adapter's invocation shape is
	// byte-for-byte upstream codex.
	codex *codexAgent
	// pi is used when backend == pi: it supplies buildArgs and the JSONL parser
	// so the adapter drives the pi-agent-cli backend (which can reach
	// openai-compatible providers like inferx that codex can't). pi is NOT
	// exec'd locally — only argv + parser are reused; execution stays in the
	// bridge container, exactly like the codex path.
	pi *piAgent
	// socketPath is resolved at Run time (env/default); injectable for tests.
	socketPath string
}

func (a *snorlaxAgent) Name() string { return "snorlax" }

// SupportsSessionResume is true for codex (the bridge forwards `codex exec
// resume <id>`) and false for pi (pi runs --no-session, so every turn is cold).
func (a *snorlaxAgent) SupportsSessionResume() bool {
	return a.backend != snorlaxBackendPi
}

func (a *snorlaxAgent) ReportsAgentAttempts() bool { return true }

// NeutralizesGateInstructions delegates to the active backend: codex's or pi's
// project-settings suppression flag (built into each backend's argv).
func (a *snorlaxAgent) NeutralizesGateInstructions() bool {
	if a.backend == snorlaxBackendPi && a.pi != nil {
		return a.pi.NeutralizesGateInstructions()
	}
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
	// StdinB64 carries pi's prompt (pi reads its prompt from stdin; codex takes
	// it as an argv arg and leaves this empty). The bridge writes it to the
	// container's stdin once at spawn, then closes.
	StdinB64 string `json:"stdinB64,omitempty"`
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
	if a.backend == snorlaxBackendPi {
		return a.runOncePi(ctx, opts)
	}
	return a.runOnceCodex(ctx, opts)
}

func (a *snorlaxAgent) runOnceCodex(ctx context.Context, opts RunOpts) (*Result, error) {
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

	req := snorlaxBridgeRequest{
		Agent:       "codex",
		Argv:        argv,
		Cwd:         opts.CWD,
		ExecutionID: fmt.Sprintf("nm-%d-%d", time.Now().UnixNano(), os.Getpid()),
		Session:     session,
	}

	var usage TokenUsage
	var lastMessage string
	var codexErr string
	var threadID string
	metrics := newCodexMetricsAccumulator()
	parse := func(ctx context.Context, r io.Reader) error {
		return parseCodexEvents(ctx, r, opts.OnChunk, &usage, &lastMessage, &codexErr, &threadID, metrics)
	}
	finalize := func() (*Result, error) {
		res, ferr := finalizeTextResult("snorlax", lastMessage, validationSchema, usage)
		if res != nil {
			res.SessionID = threadID
			res.Resumed = resumeID != ""
			res.SessionUsageCumulative = true
			m := metrics.metrics()
			res.Metrics = &m
			// resolveCodexModel reads a local rollout transcript that does not
			// exist on the no-mistakes host (codex ran inside the container);
			// leave model identity unknown rather than fabricate it.
		}
		return res, ferr
	}
	return a.runBridgeTurn(ctx, opts, req, parse, finalize, func() string { return codexErr })
}

func (a *snorlaxAgent) runOncePi(ctx context.Context, opts RunOpts) (*Result, error) {
	// pi reads its prompt from stdin and has no --output-schema equivalent, so
	// the JSON contract (if any) is inlined into the prompt (buildPiPrompt).
	// pi runs --no-session, so there is no resume: every turn is cold.
	prompt := buildPiPrompt(opts.Prompt, opts.JSONSchema)
	req := snorlaxBridgeRequest{
		Agent:       "pi",
		Argv:        a.pi.buildArgs(),
		Cwd:         opts.CWD,
		ExecutionID: fmt.Sprintf("nm-%d-%d", time.Now().UnixNano(), os.Getpid()),
		StdinB64:    base64.StdEncoding.EncodeToString([]byte(prompt)),
	}

	pp := &piParser{onChunk: opts.OnChunk}
	parse := func(ctx context.Context, r io.Reader) error {
		return pp.parse(ctx, r)
	}
	finalize := func() (*Result, error) {
		// pi exits 0 even on an in-run API/auth error (the outcome is read from
		// the event stream's last assistant message), so surface a reported
		// assistant error here exactly like the native pi adapter.
		if pp.assistantError != "" {
			return nil, fmt.Errorf("pi reported error: %s", pp.assistantError)
		}
		return finalizeTextResult("snorlax", pp.finalText(), opts.JSONSchema, pp.usage)
	}
	return a.runBridgeTurn(ctx, opts, req, parse, finalize, func() string { return "" })
}

// snorlaxFrameOutcome captures the terminal frame of one bridge invocation.
type snorlaxFrameOutcome struct {
	exit      *snorlaxExitPayload
	bridgeErr string
	readErr   error
}

// readBridgeFrames reads STDOUT/STDERR/STARTED/EXIT/ERROR frames, writing
// STDOUT bytes to out (so the caller's JSONL parser consumes them via a pipe)
// and surfacing STARTED on the started channel. Returns a channel that receives
// exactly one terminal outcome (on EXIT/ERROR) or a read error when the
// connection closes. out is closed when the reader stops so the parser
// unblocks at EOF.
func readBridgeFrames(conn net.Conn, out io.Writer, started chan<- string) <-chan snorlaxFrameOutcome {
	outcome := make(chan snorlaxFrameOutcome, 1)
	go func() {
		if closer, ok := out.(interface{ Close() error }); ok {
			defer closer.Close()
		}
		for {
			typ, payload, rerr := snorlaxReadFrame(conn)
			if rerr != nil {
				outcome <- snorlaxFrameOutcome{readErr: rerr}
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
					if _, werr := out.Write(payload); werr != nil {
						outcome <- snorlaxFrameOutcome{readErr: werr}
						return
					}
				}
			case snorlaxFrameStderr:
				// Best-effort: the parser does not consume stderr; the bridge's
				// audit log retains the full stream on the Snorlax side.
			case snorlaxFrameExit:
				var e snorlaxExitPayload
				_ = json.Unmarshal(payload, &e)
				outcome <- snorlaxFrameOutcome{exit: &e}
				return
			case snorlaxFrameError:
				var e snorlaxErrorPayload
				_ = json.Unmarshal(payload, &e)
				outcome <- snorlaxFrameOutcome{bridgeErr: e.Message}
				return
			}
		}
	}()
	return outcome
}

// runBridgeTurn is the shared codex/pi transport: it dials the bridge, sends
// REQUEST, streams STDOUT into parse, and resolves the result via finalize on a
// clean exit. Terminal failures (bridge ERROR, timeout, non-zero exit, dropped
// connection) take precedence over a parse error. exitDetail enriches a
// non-zero-exit message (codex's parser error string; "" for pi).
func (a *snorlaxAgent) runBridgeTurn(
	ctx context.Context,
	opts RunOpts,
	req snorlaxBridgeRequest,
	parse func(ctx context.Context, r io.Reader) error,
	finalize func() (*Result, error),
	exitDetail func() string,
) (*Result, error) {
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
	// real time, exactly like the local adapters. STARTED/EXIT/ERROR are control
	// frames; STDERR is dropped (the bridge's audit log retains it).
	pr, pw := io.Pipe()
	started := make(chan string, 1)
	outcome := readBridgeFrames(conn, pw, started)

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

	parseErr := parse(ctx, pr)

	select {
	case o := <-outcome:
		_ = conn.Close()
		switch {
		case o.bridgeErr != "":
			retErr := fmt.Errorf("snorlax bridge: %s", o.bridgeErr)
			emitAgentExited(opts, "snorlax", 0, retErr)
			return nil, retErr
		case o.exit != nil:
			if o.exit.TimedOut {
				retErr := fmt.Errorf("snorlax bridge: container timed out")
				emitAgentExited(opts, "snorlax", 0, retErr)
				return nil, retErr
			}
			if o.exit.Code != 0 {
				retErr := fmt.Errorf("snorlax bridge: agent exited code=%d%s", o.exit.Code, detailFromErr(exitDetail()))
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
				retErr := fmt.Errorf("snorlax bridge: connection closed unexpectedly")
				emitAgentExited(opts, "snorlax", 0, retErr)
				return nil, retErr
			}
			if o.readErr != nil {
				retErr := fmt.Errorf("snorlax bridge: read: %w", o.readErr)
				emitAgentExited(opts, "snorlax", 0, retErr)
				return nil, retErr
			}
		}
	case <-time.After(snorlaxResultWait):
		_ = conn.Close()
		retErr := fmt.Errorf("snorlax bridge: no terminal frame after parser returned")
		emitAgentExited(opts, "snorlax", 0, retErr)
		return nil, retErr
	}

	if parseErr != nil {
		retErr := fmt.Errorf("snorlax bridge: parse events: %w", parseErr)
		emitAgentExited(opts, "snorlax", 0, retErr)
		return nil, retErr
	}

	res, retErr := finalize()
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
