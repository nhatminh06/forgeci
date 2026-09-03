package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nhatminh06/forgeci/internal/config"
)

type BlobStore interface {
	Put(<-chan struct{}, string, int64, io.Reader) (int64, error)
	OpenBlob(string) (*os.File, error)
}

type Record struct {
	Producer string
	Metadata Metadata
}

// Session implements artifact semantics for one run. Metadata remains isolated
// by session; the backing CAS may be shared by many runs.
type Session struct {
	workspace, temp string
	store           BlobStore
	limits          Limits
	mu              sync.RWMutex
	records         map[string]Record
	commit          func(context.Context, string, []Metadata) error
	lookup          func(context.Context, string, string) (Metadata, error)
}

func (s *Session) SetDurableCallbacks(commit func(context.Context, string, []Metadata) error, lookup func(context.Context, string, string) (Metadata, error)) {
	s.commit = commit
	s.lookup = lookup
}

func NewSession(workspace, temp string, store BlobStore, limits Limits) (*Session, error) {
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
	return &Session{workspace: resolved, temp: temp, store: store, limits: limits, records: make(map[string]Record)}, nil
}

func (s *Session) Publish(ctx context.Context, job string, items []config.ArtifactUpload) error {
	if len(items) == 0 {
		return nil
	}
	captured := make([]Record, 0, len(items))
	paths := make([]string, 0, len(items))
	defer func() {
		for _, p := range paths {
			_ = os.Remove(p)
		}
	}()
	for _, item := range items {
		source, err := s.safeSource(item.Path)
		if err != nil {
			return AsPipelineError(fmt.Errorf("artifact %q: %w", item.Name, err))
		}
		tmp, err := os.CreateTemp(s.temp, ".capture-*.tar.gz")
		if err != nil {
			return err
		}
		name := tmp.Name()
		_ = tmp.Close()
		_ = os.Remove(name)
		paths = append(paths, name)
		meta, err := Capture(source, item.Name, name, s.limits)
		if err != nil {
			return AsPipelineError(err)
		}
		captured = append(captured, Record{Producer: job, Metadata: meta})
	}
	for i, record := range captured {
		f, err := os.Open(paths[i])
		if err != nil {
			return err
		}
		_, putErr := s.store.Put(ctx.Done(), record.Metadata.BlobSHA256, record.Metadata.ArchiveSizeBytes, f)
		closeErr := f.Close()
		if putErr != nil {
			return putErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	if s.commit != nil {
		metas := make([]Metadata, len(captured))
		for i := range captured {
			metas[i] = captured[i].Metadata
		}
		if err := s.commit(ctx, job, metas); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range captured {
		key := record.Producer + "\x00" + record.Metadata.Name
		if existing, ok := s.records[key]; ok {
			if !sameMetadata(existing.Metadata, record.Metadata) {
				return fmt.Errorf("conflicting artifact publication")
			}
			continue
		}
		s.records[key] = record
	}
	return nil
}

func sameMetadata(a, b Metadata) bool {
	return a.Name == b.Name && a.RootName == b.RootName && a.RootKind == b.RootKind && a.ContentSHA256 == b.ContentSHA256 && a.BlobSHA256 == b.BlobSHA256 && a.Format == b.Format && a.ArchiveSizeBytes == b.ArchiveSizeBytes && a.LogicalSizeBytes == b.LogicalSizeBytes && a.EntryCount == b.EntryCount
}

func (s *Session) Restore(ctx context.Context, _ string, items []config.ArtifactDownload) error {
	for _, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s.mu.RLock()
		record, ok := s.records[item.From+"\x00"+item.Name]
		s.mu.RUnlock()
		if !ok && s.lookup != nil {
			meta, err := s.lookup(ctx, item.From, item.Name)
			if err != nil {
				return err
			}
			record, ok = Record{Producer: item.From, Metadata: meta}, true
		}
		if !ok {
			return fmt.Errorf("committed artifact %s/%s is unavailable", item.From, item.Name)
		}
		destination, err := s.safeDestination(item.Into)
		if err != nil {
			return AsPipelineError(err)
		}
		final := filepath.Join(destination, record.Metadata.RootName)
		if _, err := os.Lstat(final); err == nil {
			return AsPipelineError(fmt.Errorf("artifact destination %q already exists", filepath.ToSlash(filepath.Join(item.Into, record.Metadata.RootName))))
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.MkdirAll(destination, 0700); err != nil {
			return err
		}
		blob, err := s.store.OpenBlob(record.Metadata.BlobSHA256)
		if err != nil {
			return err
		}
		archive, err := os.CreateTemp(s.temp, ".download-*.tar.gz")
		if err != nil {
			_ = blob.Close()
			return err
		}
		archiveName := archive.Name()
		copyErr := CopyVerified(archive, blob, record.Metadata.ArchiveSizeBytes, record.Metadata.BlobSHA256, s.limits.MaxArchiveBytes)
		closeBlob := blob.Close()
		closeArchive := archive.Close()
		if copyErr != nil || closeBlob != nil || closeArchive != nil {
			_ = os.Remove(archiveName)
			if copyErr != nil {
				return copyErr
			}
			return fmt.Errorf("close artifact transfer")
		}
		stage, err := os.MkdirTemp(s.temp, ".extract-*")
		if err != nil {
			_ = os.Remove(archiveName)
			return err
		}
		extractErr := Extract(archiveName, stage, record.Metadata, s.limits)
		_ = os.Remove(archiveName)
		if extractErr != nil {
			_ = os.RemoveAll(stage)
			return extractErr
		}
		if err := os.Rename(filepath.Join(stage, record.Metadata.RootName), final); err != nil {
			_ = os.RemoveAll(stage)
			return err
		}
		if err := os.RemoveAll(stage); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) safeSource(relative string) (string, error) {
	candidate := filepath.Join(s.workspace, filepath.FromSlash(relative))
	parent, err := filepath.EvalSymlinks(filepath.Dir(candidate))
	if err != nil {
		return "", fmt.Errorf("resolve upload path: %w", err)
	}
	if !within(s.workspace, parent) {
		return "", fmt.Errorf("upload path escapes workspace")
	}
	return filepath.Join(parent, filepath.Base(candidate)), nil
}
func (s *Session) safeDestination(relative string) (string, error) {
	candidate := filepath.Join(s.workspace, filepath.FromSlash(relative))
	current := s.workspace
	rel, err := filepath.Rel(s.workspace, candidate)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("download destination escapes workspace")
	}
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "." || part == "" {
			continue
		}
		next := filepath.Join(current, part)
		info, err := os.Lstat(next)
		if errors.Is(err, os.ErrNotExist) {
			current = next
			continue
		}
		if err != nil {
			return "", err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("unsafe artifact destination parent")
		}
		current = next
	}
	return candidate, nil
}
