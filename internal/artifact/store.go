package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Store struct {
	root   string
	limits Limits
}

func Open(root string, limits Limits) (*Store, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(abs, "blobs", "sha256"), 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(abs, "tmp"), 0700); err != nil {
		return nil, err
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	return &Store{root: abs, limits: limits}, nil
}
func (s *Store) Root() string   { return s.root }
func (s *Store) Limits() Limits { return s.limits }
func (s *Store) blobPath(digest string) (string, error) {
	if !ValidDigest(digest) {
		return "", fmt.Errorf("invalid artifact digest")
	}
	return filepath.Join(s.root, "blobs", "sha256", digest[:2], digest[2:]), nil
}
func (s *Store) OpenBlob(digest string) (*os.File, error) {
	p, err := s.blobPath(digest)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}
func (s *Store) HasBlob(digest string, size int64) bool {
	f, err := s.OpenBlob(digest)
	if err != nil {
		return false
	}
	defer f.Close()
	stat, err := f.Stat()
	return err == nil && stat.Size() == size
}
func (s *Store) Put(ctxDone <-chan struct{}, expected string, expectedSize int64, src io.Reader) (int64, error) {
	if !ValidDigest(expected) || expectedSize < 0 || expectedSize > s.limits.MaxArchiveBytes {
		return 0, fmt.Errorf("invalid artifact blob metadata")
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), ".upload-*")
	if err != nil {
		return 0, err
	}
	name := tmp.Name()
	defer os.Remove(name)
	h := sha256.New()
	reader := io.LimitReader(src, expectedSize+1)
	done := make(chan struct{})
	var n int64
	var copyErr error
	go func() { n, copyErr = io.Copy(io.MultiWriter(tmp, h), reader); close(done) }()
	select {
	case <-ctxDone:
		if closer, ok := src.(io.Closer); ok {
			_ = closer.Close()
		}
		_ = tmp.Close()
		<-done
		return n, fmt.Errorf("artifact upload canceled")
	case <-done:
	}
	if copyErr != nil {
		return n, copyErr
	}
	if n != expectedSize {
		return n, fmt.Errorf("artifact upload size mismatch")
	}
	if hex.EncodeToString(h.Sum(nil)) != expected {
		return n, fmt.Errorf("artifact blob digest mismatch")
	}
	if err := tmp.Sync(); err != nil {
		return n, err
	}
	if err := tmp.Close(); err != nil {
		return n, err
	}
	final, _ := s.blobPath(expected)
	if err := os.MkdirAll(filepath.Dir(final), 0700); err != nil {
		return n, err
	}
	if err := os.Link(name, final); err != nil && !errors.Is(err, fs.ErrExist) {
		return n, fmt.Errorf("publish artifact blob: %w", err)
	}
	f, err := os.Open(final)
	if err != nil {
		return n, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || stat.Size() != expectedSize {
		return n, fmt.Errorf("immutable artifact blob collision")
	}
	verify := sha256.New()
	copied, err := io.Copy(verify, f)
	if err != nil || copied != expectedSize || hex.EncodeToString(verify.Sum(nil)) != expected {
		return n, fmt.Errorf("immutable artifact blob collision")
	}
	return n, nil
}
func (s *Store) RemoveBlob(digest string) error {
	p, err := s.blobPath(digest)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
func (s *Store) CleanupTemps(now time.Time, grace time.Duration) error {
	items, err := os.ReadDir(filepath.Join(s.root, "tmp"))
	if err != nil {
		return err
	}
	cutoff := now.Add(-grace)
	for _, item := range items {
		info, err := item.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() && info.ModTime().Before(cutoff) {
			if err := os.Remove(filepath.Join(s.root, "tmp", item.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) CleanupOrphans(now time.Time, grace time.Duration, live map[string]struct{}) error {
	root := filepath.Join(s.root, "blobs", "sha256")
	return filepath.WalkDir(root, func(name string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		digest := strings.ReplaceAll(rel, string(filepath.Separator), "")
		if !ValidDigest(digest) {
			return nil
		}
		if _, ok := live[digest]; ok {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(now.Add(-grace)) {
			return nil
		}
		return os.Remove(name)
	})
}
