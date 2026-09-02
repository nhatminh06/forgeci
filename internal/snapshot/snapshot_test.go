package snapshot

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestCaptureDeterministicAndExtract(t *testing.T) {
	limits := DefaultLimits()
	first := makeTree(t, "hello\n")
	second := makeTree(t, "hello\n")
	old := time.Unix(10, 0)
	newer := time.Unix(999999, 0)
	if err := os.Chtimes(filepath.Join(first, "file.txt"), old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(second, "file.txt"), newer, newer); err != nil {
		t.Fatal(err)
	}
	store1, err := Open(t.TempDir(), first, limits)
	if err != nil {
		t.Fatal(err)
	}
	store2, err := Open(t.TempDir(), second, limits)
	if err != nil {
		t.Fatal(err)
	}
	a, err := store1.Capture(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store2.Capture(second)
	if err != nil {
		t.Fatal(err)
	}
	if a.SourceDigest != b.SourceDigest || a.BlobDigest != b.BlobDigest {
		t.Fatalf("snapshots differ: %#v %#v", a, b)
	}
	blob, err := store1.blobPath(a.SourceDigest)
	if err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	if err := Extract(blob, destination, a, limits); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(destination, "file.txt"))
	if err != nil || string(data) != "hello\n" {
		t.Fatalf("content=%q err=%v", data, err)
	}
	info, err := os.Stat(filepath.Join(destination, "bin", "run"))
	if err != nil || info.Mode().Perm()&0100 == 0 {
		t.Fatalf("executable mode lost: %v %v", info, err)
	}
	target, err := os.Readlink(filepath.Join(destination, "current"))
	if err != nil || target != "file.txt" {
		t.Fatalf("symlink=%q err=%v", target, err)
	}
	if _, err := os.Stat(filepath.Join(destination, ".git")); !os.IsNotExist(err) {
		t.Fatalf(".git included: %v", err)
	}
}

func TestCaptureRejectsSpecialAndEscapingSymlink(t *testing.T) {
	for name, setup := range map[string]func(string) error{
		"fifo":          func(root string) error { return syscall.Mkfifo(filepath.Join(root, "pipe"), 0600) },
		"absolute link": func(root string) error { return os.Symlink("/etc/passwd", filepath.Join(root, "link")) },
		"escaping link": func(root string) error { return os.Symlink("../outside", filepath.Join(root, "link")) },
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := setup(root); err != nil {
				t.Skip(err)
			}
			s, err := Open(t.TempDir(), root, DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Capture(root); err == nil {
				t.Fatal("capture unexpectedly succeeded")
			}
		})
	}
}

func TestCaptureLimitsAndDedup(t *testing.T) {
	root := makeTree(t, "hello")
	limits := DefaultLimits()
	limits.MaxLogicalBytes = 2
	s, err := Open(t.TempDir(), root, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Capture(root); err == nil {
		t.Fatal("logical limit not enforced")
	}
	limits = DefaultLimits()
	limits.MaxArchiveBytes = 1
	s, _ = Open(t.TempDir(), root, limits)
	if _, err := s.Capture(root); err == nil {
		t.Fatal("archive limit not enforced")
	}
	limits = DefaultLimits()
	storeRoot := t.TempDir()
	s, _ = Open(storeRoot, root, limits)
	a, err := s.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	if a.SourceDigest != b.SourceDigest || a.BlobDigest != b.BlobDigest {
		t.Fatal("dedup identities differ")
	}
	matches, _ := filepath.Glob(filepath.Join(storeRoot, "blobs", "sha256", "*", "*"))
	if len(matches) != 1 {
		t.Fatalf("blobs=%v", matches)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("changed"), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := s.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	if c.SourceDigest == a.SourceDigest {
		t.Fatal("source change did not change digest")
	}
}

func TestConcurrentCaptureDeduplicates(t *testing.T) {
	root := makeTree(t, "same")
	storeRoot := t.TempDir()
	s, _ := Open(storeRoot, root, DefaultLimits())
	const count = 12
	results := make(chan Metadata, count)
	failures := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, e := s.Capture(root)
			if e != nil {
				failures <- e
			} else {
				results <- m
			}
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	var digest string
	for m := range results {
		if digest == "" {
			digest = m.SourceDigest
		}
		if m.SourceDigest != digest {
			t.Fatal("source digests differ")
		}
	}
	matches, _ := filepath.Glob(filepath.Join(storeRoot, "blobs", "sha256", "*", "*"))
	if len(matches) != 1 {
		t.Fatalf("blobs=%v", matches)
	}
}

func TestExtractRejectsTraversalHardLinkCorruptionAndManifestMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header tar.Header
	}{
		{"traversal", tar.Header{Name: "../../outside", Typeflag: tar.TypeReg, Size: 1, Mode: 0600}},
		{"absolute", tar.Header{Name: "/outside", Typeflag: tar.TypeReg, Size: 1, Mode: 0600}},
		{"hardlink", tar.Header{Name: "link", Linkname: "target", Typeflag: tar.TypeLink}},
		{"escaping symlink", tar.Header{Name: "link", Linkname: "../../outside", Typeflag: tar.TypeSymlink}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			archive, meta := maliciousArchive(t, tc.header)
			if err := Extract(archive, t.TempDir(), meta, DefaultLimits()); err == nil {
				t.Fatal("malicious archive accepted")
			}
		})
	}
	root := makeTree(t, "hello")
	s, _ := Open(t.TempDir(), root, DefaultLimits())
	meta, err := s.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	blob, _ := s.blobPath(meta.SourceDigest)
	data, _ := os.ReadFile(blob)
	data[len(data)-1] ^= 1
	corrupt := filepath.Join(t.TempDir(), "corrupt")
	os.WriteFile(corrupt, data, 0600)
	if err := Extract(corrupt, t.TempDir(), meta, DefaultLimits()); err == nil {
		t.Fatal("corruption accepted")
	}
	wrong := meta
	wrong.SourceDigest = strings.Repeat("0", 64)
	if err := Extract(blob, t.TempDir(), wrong, DefaultLimits()); err == nil {
		t.Fatal("manifest mismatch accepted")
	}
}

func TestCopyVerifiedRejectsWrongSizeDigestAndLimit(t *testing.T) {
	data := []byte("snapshot")
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	cases := map[string]struct {
		input   []byte
		size    int64
		hash    string
		maximum int64
	}{
		"short":  {data[:3], int64(len(data)), digest, 100},
		"long":   {append(append([]byte{}, data...), 1), int64(len(data)), digest, 100},
		"digest": {data, int64(len(data)), strings.Repeat("0", 64), 100},
		"limit":  {data, int64(len(data)), digest, 2},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			if err := CopyVerified(&out, bytes.NewReader(tc.input), tc.size, tc.hash, tc.maximum); err == nil {
				t.Fatal("invalid download accepted")
			}
		})
	}
	var out bytes.Buffer
	if err := CopyVerified(&out, bytes.NewReader(data), int64(len(data)), digest, 100); err != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("valid download failed: %v", err)
	}
}

func makeTree(t *testing.T, content string) string {
	t.Helper()
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "bin"), 0755)
	os.Mkdir(filepath.Join(root, ".git"), 0700)
	os.WriteFile(filepath.Join(root, "file.txt"), []byte(content), 0644)
	os.WriteFile(filepath.Join(root, "bin", "run"), []byte("#!/bin/sh\n"), 0755)
	os.WriteFile(filepath.Join(root, ".git", "config"), []byte("secret"), 0600)
	if err := os.Symlink("file.txt", filepath.Join(root, "current")); err != nil {
		t.Fatal(err)
	}
	return root
}

func maliciousArchive(t *testing.T, header tar.Header) (string, Metadata) {
	t.Helper()
	name := filepath.Join(t.TempDir(), "bad.tar.gz")
	f, _ := os.Create(name)
	h := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(f, h))
	gz.Header.ModTime = time.Unix(0, 0)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&header); err != nil {
		t.Fatal(err)
	}
	if header.Size > 0 {
		tw.Write([]byte("x"))
	}
	tw.Close()
	gz.Close()
	f.Close()
	info, _ := os.Stat(name)
	return name, Metadata{SourceDigest: strings.Repeat("0", 64), BlobDigest: hex.EncodeToString(h.Sum(nil)), Format: Format, ArchiveSizeBytes: info.Size(), LogicalSizeBytes: header.Size, EntryCount: 1}
}
