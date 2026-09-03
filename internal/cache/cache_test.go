package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/config"

	"github.com/nhatminh06/forgeci/internal/artifact"
)

type testRemote struct {
	meta                 Metadata
	archive              []byte
	uploadErr, commitErr error
}

func (r *testRemote) Lookup(_ context.Context, key string) (Metadata, error) {
	if r.meta.Key == "" || r.meta.Key != key {
		return Metadata{}, ErrCacheMiss
	}
	return r.meta, nil
}
func (r *testRemote) Download(_ context.Context, _ Metadata, w io.Writer) error {
	_, err := w.Write(r.archive)
	return err
}
func (r *testRemote) Upload(context.Context, Metadata, io.Reader) error { return r.uploadErr }
func (r *testRemote) Commit(context.Context, Metadata) error            { return r.commitErr }

func sessionFixture(t *testing.T) (*Session, *testRemote, string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "value"), []byte("cached-value"), 0600); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "cache.tgz")
	meta, err := Capture(source, "demo", archive, artifact.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	remote := &testRemote{meta: meta, archive: data}
	workspace := filepath.Join(root, "workspace")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(workspace, filepath.Join(root, "tmp"), remote)
	if err != nil {
		t.Fatal(err)
	}
	return session, remote, workspace
}

func TestSessionMissIsNonFatal(t *testing.T) {
	s, remote, workspace := sessionFixture(t)
	remote.meta.Key = "present"
	s.Restore(context.Background(), []config.CacheEntry{{Key: "missing", Path: ".cache/missing"}, {Key: "present", Path: ".cache/demo"}}, io.Discard)
	if _, err := os.Stat(filepath.Join(workspace, ".cache/demo/value")); err != nil {
		t.Fatalf("MISS prevented later restore: %v", err)
	}
}

func TestSessionLookupUsesExactKey(t *testing.T) {
	s, remote, workspace := sessionFixture(t)
	remote.meta.Key = "foo"
	s.Restore(context.Background(), []config.CacheEntry{{Key: "foobar", Path: ".cache/demo"}}, io.Discard)
	if _, err := os.Stat(filepath.Join(workspace, ".cache/demo")); !os.IsNotExist(err) {
		t.Fatalf("prefix cache restored: %v", err)
	}
}
func TestSessionLogicalContentCorruptionRejected(t *testing.T) {
	s, remote, workspace := sessionFixture(t)
	remote.meta.ContentSHA256 = fmt.Sprintf("%064x", 1)
	s.Restore(context.Background(), []config.CacheEntry{{Key: "demo", Path: ".cache/demo"}}, io.Discard)
	if _, err := os.Stat(filepath.Join(workspace, ".cache/demo")); !os.IsNotExist(err) {
		t.Fatalf("destination materialized: %v", err)
	}
}
func TestSessionSourceDestinationCollisionBypasses(t *testing.T) {
	s, remote, workspace := sessionFixture(t)
	_ = remote
	destination := filepath.Join(workspace, ".cache/demo")
	if err := os.MkdirAll(destination, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, "value"), []byte("source-authoritative"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Restore(context.Background(), []config.CacheEntry{{Key: "demo", Path: ".cache/demo"}}, io.Discard)
	got, err := os.ReadFile(filepath.Join(destination, "value"))
	if err != nil || string(got) != "source-authoritative" {
		t.Fatalf("got=%q err=%v", got, err)
	}
}
func TestSessionSaveFailureIsNonFatal(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	remote := &testRemote{uploadErr: fmt.Errorf("upload failed")}
	s, err := NewSession(workspace, filepath.Join(root, "tmp"), remote)
	if err != nil {
		t.Fatal(err)
	}
	s.Save(context.Background(), []config.CacheEntry{{Key: "demo", Path: "missing"}}, io.Discard)
}

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
