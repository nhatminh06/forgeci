package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/nhatminh06/forgeci/internal/config"
)

type Transport interface {
	Upload(context.Context, string, Metadata, io.Reader) error
	Commit(context.Context, string, []Metadata) error
	Download(context.Context, string, string, string, io.Writer) (Metadata, error)
}

type RemoteSession struct {
	local     *Session
	transport Transport
}

func NewRemoteSession(workspace, temp string, transport Transport, limits Limits) (*RemoteSession, error) {
	local, err := NewSession(workspace, temp, nil, limits)
	if err != nil {
		return nil, err
	}
	return &RemoteSession{local: local, transport: transport}, nil
}

func (s *RemoteSession) Publish(ctx context.Context, job string, items []config.ArtifactUpload) error {
	if len(items) == 0 {
		return nil
	}
	metas := make([]Metadata, 0, len(items))
	paths := make([]string, 0, len(items))
	defer func() {
		for _, p := range paths {
			_ = os.Remove(p)
		}
	}()
	for _, item := range items {
		source, err := s.local.safeSource(item.Path)
		if err != nil {
			return AsPipelineError(fmt.Errorf("artifact %q: %w", item.Name, err))
		}
		tmp, err := os.CreateTemp(s.local.temp, ".capture-*.tar.gz")
		if err != nil {
			return err
		}
		name := tmp.Name()
		_ = tmp.Close()
		_ = os.Remove(name)
		paths = append(paths, name)
		meta, err := Capture(source, item.Name, name, s.local.limits)
		if err != nil {
			return AsPipelineError(err)
		}
		metas = append(metas, meta)
	}
	for i, meta := range metas {
		f, err := os.Open(paths[i])
		if err != nil {
			return err
		}
		uploadErr := s.transport.Upload(ctx, job, meta, f)
		closeErr := f.Close()
		if uploadErr != nil {
			return uploadErr
		}
		if closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
			return closeErr
		}
	}
	return s.transport.Commit(ctx, job, metas)
}

func (s *RemoteSession) Restore(ctx context.Context, consumer string, items []config.ArtifactDownload) error {
	for _, item := range items {
		destination, err := s.local.safeDestination(item.Into)
		if err != nil {
			return AsPipelineError(err)
		}
		archive, err := os.CreateTemp(s.local.temp, ".download-*.tar.gz")
		if err != nil {
			return err
		}
		archiveName := archive.Name()
		meta, downloadErr := s.transport.Download(ctx, item.From, item.Name, consumer, archive)
		closeErr := archive.Close()
		if downloadErr != nil || closeErr != nil {
			_ = os.Remove(archiveName)
			if downloadErr != nil {
				return downloadErr
			}
			return closeErr
		}
		final := filepath.Join(destination, meta.RootName)
		if _, err := os.Lstat(final); err == nil {
			_ = os.Remove(archiveName)
			return AsPipelineError(fmt.Errorf("artifact destination already exists"))
		} else if !errors.Is(err, os.ErrNotExist) {
			_ = os.Remove(archiveName)
			return err
		}
		if err := os.MkdirAll(destination, 0700); err != nil {
			_ = os.Remove(archiveName)
			return err
		}
		stage, err := os.MkdirTemp(s.local.temp, ".extract-*")
		if err != nil {
			_ = os.Remove(archiveName)
			return err
		}
		extractErr := Extract(archiveName, stage, meta, s.local.limits)
		_ = os.Remove(archiveName)
		if extractErr != nil {
			_ = os.RemoveAll(stage)
			return extractErr
		}
		if err := os.Rename(filepath.Join(stage, meta.RootName), final); err != nil {
			_ = os.RemoveAll(stage)
			return err
		}
		if err := os.RemoveAll(stage); err != nil {
			return err
		}
	}
	return nil
}
