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

func TestJobWorkspacesAreFreshAndLeaseScoped(t *testing.T) {
	root := t.TempDir()
	first := Marker{RunID: "run", JobName: "build", LeaseID: "lease-1", Generation: 1, SourceDigest: "source"}
	second := Marker{RunID: "run", JobName: "build", LeaseID: "lease-2", Generation: 2, SourceDigest: "source"}
	firstDir, err := Create(root, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(firstDir, "undeclared"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	secondDir, err := Create(root, second)
	if err != nil {
		t.Fatal(err)
	}
	if firstDir == secondDir {
		t.Fatal("different leases reused one workspace")
	}
	if _, err := os.Stat(filepath.Join(secondDir, "undeclared")); !os.IsNotExist(err) {
		t.Fatalf("fresh workspace inherited undeclared file: %v", err)
	}
	if err := Remove(root, secondDir, first); err == nil {
		t.Fatal("workspace accepted another lease marker")
	}
	if err := Remove(root, firstDir, first); err != nil {
		t.Fatal(err)
	}
	if err := Remove(root, secondDir, second); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupStaleJobWorkspaceRequiresExactMarkerIdentity(t *testing.T) {
	root := t.TempDir()
	valid := Marker{RunID: "run", JobName: "job", LeaseID: "valid", Generation: 1}
	validDir, err := Create(root, valid)
	if err != nil {
		t.Fatal(err)
	}
	maliciousDir := filepath.Join(root, "jobs", "run", "job", "malicious")
	if err := os.MkdirAll(maliciousDir, 0700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(Marker{RunID: "other", JobName: "job", LeaseID: "malicious"})
	if err := os.WriteFile(filepath.Join(maliciousDir, markerName), b, 0600); err != nil {
		t.Fatal(err)
	}
	if err := CleanupStale(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(validDir); !os.IsNotExist(err) {
		t.Fatalf("valid stale job workspace remains: %v", err)
	}
	if _, err := os.Stat(maliciousDir); err != nil {
		t.Fatalf("mismatched marker workspace removed: %v", err)
	}
}
