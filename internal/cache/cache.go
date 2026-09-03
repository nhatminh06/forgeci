// Package cache implements explicit, reusable build-cache objects.
// Cache blobs use the same deterministic and defensive archive implementation
// as artifacts, but have separate storage and metadata semantics.
package cache

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/nhatminh06/forgeci/internal/artifact"
)

const Format = "cache-tar-gzip-v1"

type Limits = artifact.Limits

type Metadata struct {
	Key string
	artifact.Metadata
	LastAccessedAt time.Time
	ExpiresAt      *time.Time
}

type Store struct {
	root   string
	cas    *artifact.Store
	limits Limits
}

func Open(root string, limits Limits) (*Store, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	cas, err := artifact.Open(abs, limits)
	if err != nil {
		return nil, err
	}
	return &Store{root: cas.Root(), cas: cas, limits: limits}, nil
}

func (s *Store) Root() string   { return s.root }
func (s *Store) Limits() Limits { return s.limits }

func Capture(source, key, destination string, limits Limits) (Metadata, error) {
	meta, err := artifact.Capture(source, key, destination, limits)
	if err != nil {
		return Metadata{}, err
	}
	meta.Format = Format
	return Metadata{Key: key, Metadata: meta, LastAccessedAt: time.Now().UTC()}, nil
}

func Extract(archive, destination string, expected Metadata, limits Limits) error {
	meta := expected.Metadata
	meta.Format = artifact.Format
	return artifact.Extract(archive, destination, meta, limits)
}

func (s *Store) Put(ctxDone <-chan struct{}, expected string, expectedSize int64, src io.Reader) (int64, error) {
	return s.cas.Put(ctxDone, expected, expectedSize, src)
}

func (s *Store) OpenBlob(digest string) (*os.File, error) { return s.cas.OpenBlob(digest) }
func (s *Store) RemoveBlob(digest string) error           { return s.cas.RemoveBlob(digest) }
func (s *Store) CleanupTemps(now time.Time, grace time.Duration) error {
	return s.cas.CleanupTemps(now, grace)
}
func (s *Store) CleanupOrphans(now time.Time, grace time.Duration, live map[string]struct{}) error {
	return s.cas.CleanupOrphans(now, grace, live)
}
