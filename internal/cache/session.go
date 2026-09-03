package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nhatminh06/forgeci/internal/artifact"
	"github.com/nhatminh06/forgeci/internal/config"
)

type Remote interface {
	Lookup(context.Context, string) (Metadata, error)
	Download(context.Context, Metadata, io.Writer) error
	Upload(context.Context, Metadata, io.Reader) error
	Commit(context.Context, Metadata) error
}

type Session struct {
	workspace string
	temp      string
	store     Remote
}

type LocalRemote struct {
	Store     *Store
	Workspace string
	LookupFn  func(context.Context, string) (Metadata, error)
	CommitFn  func(context.Context, Metadata) error
	mu        sync.Mutex
	entries   map[string]Metadata
}

func NewLocalRemote(store *Store, workspace string) *LocalRemote {
	return &LocalRemote{Store: store, Workspace: workspace, entries: make(map[string]Metadata)}
}
func (r *LocalRemote) Lookup(ctx context.Context, key string) (Metadata, error) {
	if r.LookupFn != nil {
		return r.LookupFn(ctx, key)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.entries[key]
	if !ok {
		return Metadata{}, ErrCacheMiss
	}
	return m, nil
}
func (r *LocalRemote) Download(_ context.Context, m Metadata, dst io.Writer) error {
	f, err := r.Store.OpenBlob(m.BlobSHA256)
	if err != nil {
		return err
	}
	defer f.Close()
	return artifact.CopyVerified(dst, f, m.ArchiveSizeBytes, m.BlobSHA256, r.Store.Limits().MaxArchiveBytes)
}
func (r *LocalRemote) Upload(ctx context.Context, m Metadata, src io.Reader) error {
	_, err := r.Store.Put(ctx.Done(), m.BlobSHA256, m.ArchiveSizeBytes, src)
	return err
}
func (r *LocalRemote) Commit(ctx context.Context, m Metadata) error {
	if r.CommitFn != nil {
		return r.CommitFn(ctx, m)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.entries[m.Key]; ok && !sameMetadata(old, m) {
		return fmt.Errorf("cache key conflict")
	}
	r.entries[m.Key] = m
	return nil
}

func sameMetadata(a, b Metadata) bool {
	return a.Key == b.Key && a.RootName == b.RootName && a.RootKind == b.RootKind && a.ContentSHA256 == b.ContentSHA256 && a.BlobSHA256 == b.BlobSHA256 && a.Format == b.Format && a.ArchiveSizeBytes == b.ArchiveSizeBytes && a.LogicalSizeBytes == b.LogicalSizeBytes && a.EntryCount == b.EntryCount
}

func NewSession(workspace, temp string, remote Remote) (*Session, error) {
	abs, err := filepath.Abs(workspace)
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(temp, 0700); err != nil {
		return nil, err
	}
	return &Session{workspace: resolved, temp: temp, store: remote}, nil
}

func (s *Session) Restore(ctx context.Context, items []config.CacheEntry, output io.Writer) {
	for _, item := range items {
		if ctx.Err() != nil {
			return
		}
		meta, err := s.store.Lookup(ctx, item.Key)
		if errors.Is(err, ErrCacheMiss) || errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(output, "[cache] MISS %s\n", item.Key)
			continue
		}
		if err != nil {
			fmt.Fprintf(output, "[cache] restore unavailable for %s: %v; continuing without cache\n", item.Key, err)
			continue
		}
		if meta.RootName == "" || meta.RootKind == "" {
			fmt.Fprintf(output, "[cache] restore rejected for %s: invalid metadata\n", item.Key)
			continue
		}
		destination, err := safePath(s.workspace, item.Path)
		if err != nil {
			fmt.Fprintf(output, "[cache] restore rejected for %s: %v\n", item.Key, err)
			continue
		}
		final := destination
		if _, err := os.Lstat(final); err == nil {
			fmt.Fprintf(output, "[cache] BYPASS %s: destination exists\n", item.Key)
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(output, "[cache] restore unavailable for %s: %v\n", item.Key, err)
			continue
		}
		if err := os.MkdirAll(s.temp, 0700); err != nil {
			fmt.Fprintf(output, "[cache] restore unavailable for %s: %v\n", item.Key, err)
			continue
		}
		archive, err := os.CreateTemp(s.temp, ".cache-download-*.tar.gz")
		if err != nil {
			fmt.Fprintf(output, "[cache] restore unavailable for %s: %v\n", item.Key, err)
			continue
		}
		archiveName := archive.Name()
		err = s.store.Download(ctx, meta, archive)
		closeErr := archive.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			stage, stageErr := os.MkdirTemp(s.temp, ".cache-extract-*")
			if stageErr != nil {
				err = stageErr
			} else {
				err = Extract(archiveName, stage, meta, artifactLimits(meta))
				if err == nil {
					if err = os.MkdirAll(filepath.Dir(destination), 0700); err == nil {
						err = os.Rename(filepath.Join(stage, meta.RootName), final)
					}
				}
				_ = os.RemoveAll(stage)
			}
		}
		_ = os.Remove(archiveName)
		if err != nil {
			fmt.Fprintf(output, "[cache] restore rejected for %s: %v; continuing without cache\n", item.Key, err)
			continue
		}
		fmt.Fprintf(output, "[cache] HIT %s\n", item.Key)
	}
}

func (s *Session) Save(ctx context.Context, items []config.CacheEntry, output io.Writer) {
	for _, item := range items {
		if ctx.Err() != nil {
			return
		}
		source, err := safePath(s.workspace, item.Path)
		if err != nil {
			fmt.Fprintf(output, "[cache] SAVE-SKIPPED %s: %v\n", item.Key, err)
			continue
		}
		info, err := os.Lstat(source)
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(output, "[cache] SAVE-SKIPPED %s: path missing\n", item.Key)
			continue
		}
		if err != nil {
			fmt.Fprintf(output, "[cache] SAVE-SKIPPED %s: %v\n", item.Key, err)
			continue
		}
		if !info.Mode().IsRegular() && !info.IsDir() {
			fmt.Fprintf(output, "[cache] SAVE-SKIPPED %s: unsupported root\n", item.Key)
			continue
		}
		archive, err := os.CreateTemp(s.temp, ".cache-capture-*.tar.gz")
		if err != nil {
			fmt.Fprintf(output, "[cache] SAVE-SKIPPED %s: %v\n", item.Key, err)
			continue
		}
		archiveName := archive.Name()
		_ = archive.Close()
		_ = os.Remove(archiveName)
		meta, err := Capture(source, item.Key, archiveName, artifactLimits(Metadata{}))
		if err == nil {
			f, openErr := os.Open(archiveName)
			err = openErr
			if err == nil {
				err = s.store.Upload(ctx, meta, f)
				_ = f.Close()
			}
		}
		if err == nil {
			err = s.store.Commit(ctx, meta)
		}
		_ = os.Remove(archiveName)
		if err != nil {
			fmt.Fprintf(output, "[cache] SAVE-SKIPPED %s: %v\n", item.Key, err)
			continue
		}
		fmt.Fprintf(output, "[cache] SAVE %s\n", item.Key)
	}
}

func artifactLimits(_ Metadata) Limits { return artifact.DefaultLimits() }

func safePath(root, value string) (string, error) {
	if value == "" || value == "." || filepath.IsAbs(value) || strings.Contains(value, `\`) || strings.ContainsRune(value, 0) {
		return "", fmt.Errorf("unsafe cache path %q", value)
	}
	clean := filepath.Clean(value)
	if clean != value || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe cache path %q", value)
	}
	return filepath.Join(root, filepath.FromSlash(clean)), nil
}

var ErrCacheMiss = errors.New("cache miss")
