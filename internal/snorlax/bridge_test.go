package snorlax

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestSocketPath_EnvOverride(t *testing.T) {
	t.Setenv("SNORLAX_NM_SOCKET", "/explicit/path.sock")
	if got := SocketPath(); got != "/explicit/path.sock" {
		t.Errorf("SocketPath() = %q, want /explicit/path.sock", got)
	}
}

func TestSocketPath_XDGDefault(t *testing.T) {
	t.Setenv("SNORLAX_NM_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	if got := SocketPath(); got != "/run/user/1000/snorlax-nm.sock" {
		t.Errorf("SocketPath() = %q, want /run/user/1000/snorlax-nm.sock", got)
	}
}

func TestAvailable_OnlyARealSocket(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "no.sock")
	if AvailableAt(missing) {
		t.Error("AvailableAt(missing) = true, want false")
	}

	// A regular file is not a socket.
	regular := filepath.Join(dir, "file")
	if err := os.WriteFile(regular, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if AvailableAt(regular) {
		t.Error("AvailableAt(regular file) = true, want false")
	}

	// A real listening unix socket is available.
	sock := filepath.Join(dir, "nm.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	defer os.Remove(sock)
	if !AvailableAt(sock) {
		t.Error("AvailableAt(socket) = false, want true")
	}
}
