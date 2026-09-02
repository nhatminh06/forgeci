package runworkspace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveRequiresMatchingMarkerAndPath(t *testing.T) {
	root := t.TempDir()
	marker := Marker{RunID: "run", LeaseID: "lease", Generation: 1, SourceDigest: "digest"}
	dir, err := Create(root, marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := Remove(root, dir, Marker{RunID: "wrong", LeaseID: "lease"}); err == nil {
		t.Fatal("removed with wrong identity")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal("workspace removed on rejected cleanup")
	}
	if err := Remove(root, dir, marker); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("workspace remains: %v", err)
	}
}

func TestCleanupStalePreservesUnmarkedDirectories(t *testing.T) {
	root := t.TempDir()
	marker := Marker{RunID: "run", LeaseID: "lease", Generation: 1}
	dir, err := Create(root, marker)
	if err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(root, "runs", "unrelated", "data")
	if err := os.MkdirAll(unrelated, 0700); err != nil {
		t.Fatal(err)
	}
	if err := CleanupStale(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("stale workspace remains: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unmarked directory removed: %v", err)
	}
}

func TestRemoveRejectsSymlinkedParent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	marker := Marker{RunID: "run", LeaseID: "lease"}
	if err := os.Mkdir(filepath.Join(root, "runs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "runs", "run")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "runs", "run", "lease")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(marker)
	os.WriteFile(filepath.Join(dir, markerName), b, 0600)
	if err := Remove(root, dir, marker); err == nil {
		t.Fatal("symlinked parent accepted")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("outside workspace deleted: %v", err)
	}
}
