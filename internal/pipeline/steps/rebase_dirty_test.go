package steps

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
)

func TestUpdateHeadSHA_DirtyWorktreePreservesChanges(t *testing.T) {
	dir := t.TempDir()
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "config", "user.name", "test")
	gitCmd(t, dir, "config", "user.email", "test@test.com")
	gitCmd(t, dir, "checkout", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "tracked.txt")
	gitCmd(t, dir, "commit", "-m", "base")
	baseSHA := gitCmd(t, dir, "rev-parse", "HEAD")
	gitCmd(t, dir, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(dir, "feature.txt"), []byte("feature\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", "feature.txt")
	gitCmd(t, dir, "commit", "-m", "feature")
	submitted := gitCmd(t, dir, "rev-parse", "HEAD")
	gitCmd(t, dir, "checkout", "main")
	gitCmd(t, dir, "merge", "--no-ff", "feature", "-m", "already delivered")
	observed := gitCmd(t, dir, "rev-parse", "HEAD")
	gitCmd(t, dir, "update-ref", "refs/remotes/origin/main", observed)
	if err := os.WriteFile(filepath.Join(dir, "tracked.txt"), []byte("operator edit\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	sctx := newTestContextWithDBRecords(t, &mockAgent{name: "test"}, dir, baseSHA, submitted, config.Commands{})
	sctx.Run.Branch = "refs/heads/feature"
	_, err := updateHeadSHA(context.Background(), sctx)
	if err == nil {
		t.Fatal("empty diff with dirty worktree unexpectedly succeeded")
	}
	content, contentErr := os.ReadFile(filepath.Join(dir, "tracked.txt"))
	if contentErr != nil {
		t.Fatal(contentErr)
	}
	if string(content) != "operator edit\n" {
		t.Fatalf("dirty worktree content = %q, want operator edit preserved", content)
	}
	if got := gitCmd(t, dir, "rev-parse", "HEAD"); got != observed {
		t.Fatalf("worktree head = %s, want %s", got, observed)
	}
	if sctx.Run.HeadSHA != submitted {
		t.Fatalf("in-memory run head = %s, want %s", sctx.Run.HeadSHA, submitted)
	}
	if !errors.Is(err, pipeline.ErrSkipTerminalHeadReconciliation) {
		t.Fatalf("error = %v, want terminal reconciliation error", err)
	}
}
