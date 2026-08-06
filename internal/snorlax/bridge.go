// Package snorlax holds the shared no-mistakes↔Snorlax bridge discovery helpers
// used by both config resolution (is the bridge up?) and the agent adapter
// (where do I dial?). It depends only on the standard library so it can sit
// below both internal/config and internal/agent without inverting layering.
package snorlax

import (
	"os"
	"path/filepath"
)

// SocketPath resolves the bridge Unix-socket path: SNORLAX_NM_SOCKET wins,
// else the per-user default ($XDG_RUNTIME_DIR/snorlax-nm.sock, falling back to
// ~/.snorlax/nm-bridge.sock). Mirrors src/config/config.ts defaultBridgeSocketPath.
func SocketPath() string {
	if v := os.Getenv("SNORLAX_NM_SOCKET"); v != "" {
		return v
	}
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		return filepath.Join(xdg, "snorlax-nm.sock")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "snorlax-nm.sock"
	}
	return filepath.Join(home, ".snorlax", "nm-bridge.sock")
}

// Available reports whether the bridge socket is present and is a socket. Used
// by config resolution so `agent: snorlax` resolves as runnable iff the local
// bridge is up — the socket is the agent's "binary".
func Available() bool {
	return AvailableAt(SocketPath())
}

// AvailableAt reports whether a specific path is a Unix socket. Split out so
// tests can point at a throwaway socket path.
func AvailableAt(path string) bool {
	if path == "" {
		return false
	}
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Mode().Type()&os.ModeSocket != 0
}
