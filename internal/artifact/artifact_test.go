package artifact

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/config"
)

func TestCaptureExtractDeterministicDirectory(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "dist")
	mustMkdir(t, filepath.Join(source, "bin"))
	mustWrite(t, filepath.Join(source, "bin", "app"), []byte("hello\n"), 0755)
	if err := os.Chmod(filepath.Join(source, "bin"), 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(source, "bin"), 0755) })
	mustWrite(t, filepath.Join(source, "config.json"), []byte("{}\n"), 0640)
	if err := os.Symlink("bin/app", filepath.Join(source, "current")); err != nil {
		t.Fatal(err)
	}
	a := filepath.Join(root, "a.tar.gz")
	b := filepath.Join(root, "b.tar.gz")
	m1, err := Capture(source, "bundle", a, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	m2, err := Capture(source, "bundle", b, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if m1.ContentSHA256 != m2.ContentSHA256 || m1.BlobSHA256 != m2.BlobSHA256 {
		t.Fatalf("capture is not deterministic: %#v %#v", m1, m2)
	}
	destination := filepath.Join(root, "restore")
	mustMkdir(t, destination)
	if err := Extract(a, destination, m1, DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(destination, "dist", "bin", "app"))
	if err != nil || string(data) != "hello\n" {
		t.Fatalf("restored file: %q %v", data, err)
	}
	info, _ := os.Stat(filepath.Join(destination, "dist", "bin", "app"))
	if runtime.GOOS != "windows" && info.Mode().Perm()&0100 == 0 {
		t.Fatal("executable mode not preserved")
	}
	dirInfo, _ := os.Stat(filepath.Join(destination, "dist", "bin"))
	if dirInfo.Mode().Perm() != 0555 {
		t.Fatalf("directory mode=%o", dirInfo.Mode().Perm())
	}
	if err := os.Chmod(filepath.Join(destination, "dist", "bin"), 0755); err != nil {
		t.Fatal(err)
	}
}

func TestStoreDeduplicatesAndCollectsOnlyOldOrphans(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("blob")
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	done := make(chan struct{})
	if _, err := s.Put(done, digest, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(done, digest, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if !s.HasBlob(digest, int64(len(data))) {
		t.Fatal("deduplicated blob missing")
	}
	if err := s.CleanupOrphans(time.Now(), time.Hour, map[string]struct{}{}); err != nil {
		t.Fatal(err)
	}
	if !s.HasBlob(digest, int64(len(data))) {
		t.Fatal("recent orphan removed")
	}
	blob, _ := s.blobPath(digest)
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(blob, old, old); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupOrphans(time.Now(), time.Hour, map[string]struct{}{digest: {}}); err != nil {
		t.Fatal(err)
	}
	if !s.HasBlob(digest, int64(len(data))) {
		t.Fatal("live blob removed")
	}
	if err := s.CleanupOrphans(time.Now(), time.Hour, map[string]struct{}{}); err != nil {
		t.Fatal(err)
	}
	if s.HasBlob(digest, int64(len(data))) {
		t.Fatal("old orphan retained")
	}
}

type blockingReadCloser struct{ closed chan struct{} }

func (b *blockingReadCloser) Read([]byte) (int, error) { <-b.closed; return 0, io.EOF }
func (b *blockingReadCloser) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}
func TestCanceledUploadRemovesTemporaryFile(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	cancel := make(chan struct{})
	source := &blockingReadCloser{closed: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, err := s.Put(cancel, strings.Repeat("a", 64), 1, source); done <- err }()
	close(cancel)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "canceled") {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled upload hung")
	}
	entries, err := os.ReadDir(filepath.Join(root, "tmp"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary uploads=%v err=%v", entries, err)
	}
}

type failingTransport struct{}

func (failingTransport) Upload(context.Context, string, Metadata, io.Reader) error { return nil }
func (failingTransport) Commit(context.Context, string, []Metadata) error          { return nil }
func (failingTransport) Download(_ context.Context, _, _, _ string, w io.Writer) (Metadata, error) {
	_, _ = w.Write([]byte("partial"))
	return Metadata{}, context.Canceled
}
func TestCanceledDownloadRemovesTemporaryFile(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	mustMkdir(t, workspace)
	temp := filepath.Join(root, "temp")
	session, err := NewRemoteSession(workspace, temp, failingTransport{}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	err = session.Restore(context.Background(), "consume", []config.ArtifactDownload{{From: "build", Name: "app", Into: "input"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	entries, readErr := os.ReadDir(temp)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("temporary downloads=%v err=%v", entries, readErr)
	}
}

func TestCaptureRejectsEscapingSymlinkAndSpecialFile(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "dist")
	mustMkdir(t, source)
	if err := os.Symlink("../../outside", filepath.Join(source, "bad")); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(source, "bad", filepath.Join(root, "bad.tar.gz"), DefaultLimits()); err == nil {
		t.Fatal("escaping symlink accepted")
	}
	if runtime.GOOS != "windows" {
		_ = os.Remove(filepath.Join(source, "bad"))
		if err := makeFIFO(filepath.Join(source, "fifo")); err == nil {
			if _, err := Capture(source, "bad", filepath.Join(root, "fifo.tar.gz"), DefaultLimits()); err == nil {
				t.Fatal("FIFO accepted")
			}
		}
	}
}

func TestExtractRejectsTraversalAndBlobMismatch(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "bad.tar.gz")
	f, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	data := []byte("bad")
	if err := tw.WriteHeader(&tar.Header{Name: "../../outside", Typeflag: tar.TypeReg, Mode: 0600, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	_, _ = tw.Write(data)
	_ = tw.Close()
	_ = gz.Close()
	_ = f.Close()
	bytes, _ := os.ReadFile(archive)
	sum := sha256.Sum256(bytes)
	meta := Metadata{RootName: "dist", RootKind: "directory", ContentSHA256: hex.EncodeToString(sum[:]), BlobSHA256: hex.EncodeToString(sum[:]), Format: Format, ArchiveSizeBytes: int64(len(bytes)), LogicalSizeBytes: int64(len(data)), EntryCount: 1}
	dest := filepath.Join(root, "dest")
	mustMkdir(t, dest)
	if err := Extract(archive, dest, meta, DefaultLimits()); err == nil {
		t.Fatal("traversal accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "outside")); !os.IsNotExist(err) {
		t.Fatal("outside file created")
	}
	meta.BlobSHA256 = hex.EncodeToString(make([]byte, 32))
	if err := Extract(archive, dest, meta, DefaultLimits()); err == nil {
		t.Fatal("blob mismatch accepted")
	}
}

func TestExtractRejectsLogicalDigestMismatch(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "app")
	mustWrite(t, source, []byte("value"), 0755)
	archive := filepath.Join(root, "app.tar.gz")
	meta, err := Capture(source, "app", archive, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "restore")
	mustMkdir(t, destination)
	originalBlob := meta.BlobSHA256
	meta.BlobSHA256 = strings.Repeat("0", 64)
	if err := Extract(archive, destination, meta, DefaultLimits()); err == nil || !strings.Contains(err.Error(), "blob digest mismatch") {
		t.Fatalf("blob mismatch error=%v", err)
	}
	meta.BlobSHA256 = originalBlob
	meta.ContentSHA256 = strings.Repeat("0", 64)
	if err := Extract(archive, destination, meta, DefaultLimits()); err == nil || !strings.Contains(err.Error(), "content digest mismatch") {
		t.Fatalf("logical mismatch error=%v", err)
	}
}

func TestSessionPublicationIsAtomicAndRestoreDoesNotOverwrite(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	mustMkdir(t, workspace)
	mustWrite(t, filepath.Join(workspace, "good"), []byte("good"), 0600)
	store, err := Open(filepath.Join(root, "store"), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(workspace, filepath.Join(root, "tmp"), store, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	uploads := []config.ArtifactUpload{{Name: "good", Path: "good"}, {Name: "missing", Path: "missing"}}
	if err := session.Publish(context.Background(), "build", uploads); err == nil || !IsPipelineError(err) {
		t.Fatalf("missing artifact error=%v", err)
	}
	session.mu.RLock()
	count := len(session.records)
	session.mu.RUnlock()
	if count != 0 {
		t.Fatal("partial artifact set published")
	}
	if err := session.Publish(context.Background(), "build", uploads[:1]); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, filepath.Join(workspace, "inputs"))
	mustWrite(t, filepath.Join(workspace, "inputs", "good"), []byte("original"), 0600)
	downloads := []config.ArtifactDownload{{From: "build", Name: "good", Into: "inputs"}}
	if err := session.Restore(context.Background(), "consume", downloads); err == nil || !IsPipelineError(err) {
		t.Fatalf("collision error=%v", err)
	}
	data, _ := os.ReadFile(filepath.Join(workspace, "inputs", "good"))
	if string(data) != "original" {
		t.Fatal("existing destination overwritten")
	}
}

func TestLimits(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "dist")
	mustMkdir(t, source)
	mustWrite(t, filepath.Join(source, "one"), []byte("1234"), 0600)
	limits := DefaultLimits()
	limits.MaxLogicalBytes = 3
	if _, err := Capture(source, "small", filepath.Join(root, "small.tar.gz"), limits); err == nil {
		t.Fatal("logical limit ignored")
	}
	limits = DefaultLimits()
	limits.MaxEntries = 1
	if _, err := Capture(source, "entries", filepath.Join(root, "entries.tar.gz"), limits); err == nil {
		t.Fatal("entry limit ignored")
	}
}

func mustMkdir(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(p, 0700); err != nil {
		t.Fatal(err)
	}
}
func mustWrite(t *testing.T, p string, b []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(p, b, mode); err != nil {
		t.Fatal(err)
	}
}
