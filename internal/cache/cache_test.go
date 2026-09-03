package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/artifact"
)

func TestDeterministicFileRoundTrip(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "cache")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(source, "tool.bin")
	if err := os.WriteFile(file, []byte("cache-value"), 0750); err != nil {
		t.Fatal(err)
	}
	limits := artifact.DefaultLimits()
	archive := filepath.Join(root, "cache.tar.gz")
	meta, err := Capture(source, "toolchain-v1", archive, limits)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Format != Format || meta.Key != "toolchain-v1" || meta.ContentSHA256 == "" || meta.BlobSHA256 == "" {
		t.Fatalf("metadata=%+v", meta)
	}
	destination := filepath.Join(root, "restore")
	if err := os.Mkdir(destination, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Extract(archive, destination, meta, limits); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(destination, "cache", "tool.bin"))
	if err != nil || string(got) != "cache-value" {
		t.Fatalf("restored=%q err=%v", got, err)
	}
	info, err := os.Stat(filepath.Join(destination, "cache", "tool.bin"))
	if err != nil || info.Mode().Perm() != 0750 {
		t.Fatalf("restored mode=%v err=%v", info.Mode(), err)
	}
}

func TestStoreOrphanAndTempGrace(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, artifact.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("orphan")
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	if _, err = s.Put(context.Background().Done(), digest, int64(len(payload)), bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	live := map[string]struct{}{}
	if err = s.CleanupOrphans(now, time.Hour, live); err != nil {
		t.Fatal(err)
	}
	if _, err = s.OpenBlob(digest); err != nil {
		t.Fatal("fresh orphan removed", err)
	}
	if err = os.Chtimes(filepath.Join(root, "blobs", "sha256", digest[:2], digest[2:]), now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = s.CleanupOrphans(now, time.Hour, live); err != nil {
		t.Fatal(err)
	}
	if _, err = s.OpenBlob(digest); !os.IsNotExist(err) {
		t.Fatalf("old orphan remains: %v", err)
	}
	tmp := filepath.Join(root, "tmp", "stale")
	if err = os.WriteFile(tmp, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.CleanupTemps(now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(tmp); err != nil {
		t.Fatal("fresh temp removed", err)
	}
	if err = os.Chtimes(tmp, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = s.CleanupTemps(now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("old temp remains: %v", err)
	}
}

func TestCorruptBlobRejected(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "cache")
	if err := os.WriteFile(source, []byte("good"), 0600); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "cache.tar.gz")
	meta, err := Capture(source, "key-v1", archive, artifact.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(archive, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := Extract(archive, filepath.Join(root, "restore"), meta, artifact.DefaultLimits()); err == nil {
		t.Fatal("corrupt cache accepted")
	}
}
