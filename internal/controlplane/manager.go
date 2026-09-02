package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/pipeline"
	"github.com/nhatminh06/forgeci/internal/runner"
	"github.com/nhatminh06/forgeci/internal/runworkspace"
	"github.com/nhatminh06/forgeci/internal/snapshot"
	"github.com/nhatminh06/forgeci/internal/store"
)

type Manager struct {
	store         store.Store
	workspace     string
	snapshots     *snapshot.Store
	workspaceRoot string
	output        io.Writer
	wake          chan struct{}
	workMu        sync.Mutex
	workSignal    chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	mu            sync.Mutex
	activeID      string
	activeCancel  context.CancelFunc
}

type InputError struct{ Err error }

func (e InputError) Error() string { return e.Err.Error() }
func (e InputError) Unwrap() error { return e.Err }
func inputError(err error) error   { return InputError{Err: err} }

func New(parent context.Context, persistence store.Store, workspace string, output io.Writer, snapshots ...*snapshot.Store) (*Manager, error) {
	resolved, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace: %w", err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	if output == nil {
		output = io.Discard
	}
	m := &Manager{store: persistence, workspace: resolved, output: output, wake: make(chan struct{}, 1), workSignal: make(chan struct{}), ctx: ctx, cancel: cancel}
	if len(snapshots) > 0 {
		m.snapshots = snapshots[0]
		m.workspaceRoot = filepath.Join(snapshots[0].Root(), "workspaces", "server")
	}
	return m, nil
}

func (m *Manager) Start(ctx context.Context) error {
	if err := m.store.RecoverInterrupted(ctx); err != nil {
		return fmt.Errorf("recover interrupted runs: %w", err)
	}
	m.wg.Add(1)
	go m.dispatch()
	m.Notify()
	return nil
}
func (m *Manager) Close() {
	m.cancel()
	m.mu.Lock()
	if m.activeCancel != nil {
		m.activeCancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
}
func (m *Manager) Notify() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
	m.workMu.Lock()
	close(m.workSignal)
	m.workSignal = make(chan struct{})
	m.workMu.Unlock()
}
func (m *Manager) WorkAvailable() <-chan struct{} {
	m.workMu.Lock()
	defer m.workMu.Unlock()
	return m.workSignal
}
func (m *Manager) Ping(ctx context.Context) error { return m.store.Ping(ctx) }
func (m *Manager) Get(ctx context.Context, id string) (*store.Run, error) {
	return m.store.GetRun(ctx, id)
}
func (m *Manager) List(ctx context.Context, limit int) ([]store.Run, error) {
	return m.store.ListRuns(ctx, limit)
}

func (m *Manager) Submit(ctx context.Context, pipelineFile string, maxParallel int) (*store.Run, error) {
	if pipelineFile == "" {
		pipelineFile = "forge.yaml"
	}
	if maxParallel == 0 {
		maxParallel = 1
	}
	if maxParallel < 1 {
		return nil, inputError(fmt.Errorf("max_parallel must be greater than zero"))
	}
	path, err := m.safePipelinePath(pipelineFile)
	if err != nil {
		return nil, inputError(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, inputError(fmt.Errorf("read pipeline: %w", err))
	}
	cfg, err := config.ParseBytes(data, pipelineFile)
	if err != nil {
		return nil, inputError(err)
	}
	graph, err := pipeline.Compile(cfg)
	if err != nil {
		return nil, inputError(fmt.Errorf("compile pipeline: %w", err))
	}
	sum := sha256.Sum256(data)
	in := store.CreateRun{ID: uuid.NewString(), PipelineFile: pipelineFile, PipelineYAML: data, PipelineSHA256: hex.EncodeToString(sum[:]), Workspace: m.workspace, MaxParallel: maxParallel}
	if m.snapshots != nil {
		meta, err := m.snapshots.Capture(m.workspace)
		if err != nil {
			return nil, inputError(fmt.Errorf("capture source snapshot: %w", err))
		}
		in.Snapshot = &store.SourceSnapshot{SourceDigest: meta.SourceDigest, BlobDigest: meta.BlobDigest, Format: meta.Format, ArchiveSizeBytes: meta.ArchiveSizeBytes, LogicalSizeBytes: meta.LogicalSizeBytes, EntryCount: meta.EntryCount, CreatedAt: meta.CreatedAt}
	}
	for _, name := range graph.Order {
		job := graph.Nodes[name].Job
		in.Jobs = append(in.Jobs, store.Job{Name: name, Status: store.JobPending, Image: job.Image})
	}
	run, err := m.store.CreateRun(ctx, in)
	if err != nil {
		return nil, err
	}
	m.Notify()
	return run, nil
}

func (m *Manager) safePipelinePath(name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || strings.IndexFunc(name, func(r rune) bool { return unicode.IsControl(r) }) >= 0 {
		return "", fmt.Errorf("pipeline_file must be a safe relative path")
	}
	clean := filepath.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("pipeline_file escapes workspace")
	}
	candidate := filepath.Join(m.workspace, clean)
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve pipeline_file: %w", err)
	}
	rel, err := filepath.Rel(m.workspace, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("pipeline_file escapes workspace")
	}
	return resolved, nil
}

func (m *Manager) Cancel(ctx context.Context, id string) (store.RunStatus, error) {
	r, err := m.store.GetRun(ctx, id)
	if err != nil {
		return "", err
	}
	switch r.Status {
	case store.RunQueued:
		if err := m.store.CancelQueued(ctx, id); err != nil {
			return "", err
		}
		return store.RunCanceled, nil
	case store.RunRunning:
		status, err := m.store.RequestCancel(ctx, id)
		if err != nil {
			return status, err
		}
		m.mu.Lock()
		if m.activeID == id && m.activeCancel != nil {
			m.activeCancel()
		}
		m.mu.Unlock()
		return status, nil
	default:
		return r.Status, store.ErrConflict
	}
}

func (m *Manager) dispatch() {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wake:
			for {
				if m.ctx.Err() != nil {
					return
				}
				run, err := m.store.ClaimNextQueuedRun(m.ctx)
				if err != nil {
					fmt.Fprintf(m.output, "control-plane claim error: %v\n", err)
					break
				}
				if run == nil {
					break
				}
				m.execute(run)
			}
		}
	}
}

type observer struct {
	runID  string
	store  store.Store
	cancel context.CancelFunc
	once   sync.Once
	err    error
}

func (o *observer) OnJobState(name string, _ runner.State, newState runner.State) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := o.store.UpdateJob(ctx, o.runID, name, store.JobStatus(newState), nil); err != nil {
		o.once.Do(func() { o.err = err; o.cancel() })
	}
}

func (m *Manager) execute(runRecord *store.Run) {
	ctx, cancel := context.WithCancel(m.ctx)
	m.mu.Lock()
	m.activeID = runRecord.ID
	m.activeCancel = cancel
	m.mu.Unlock()
	defer func() {
		cancel()
		m.mu.Lock()
		m.activeID = ""
		m.activeCancel = nil
		m.mu.Unlock()
		m.Notify()
	}()
	executionWorkspace := runRecord.Workspace
	var marker runworkspace.Marker
	if m.snapshots != nil && runRecord.SourceSnapshotSHA256 != nil {
		marker = runworkspace.Marker{RunID: runRecord.ID, LeaseID: "local", Generation: 0, SourceDigest: *runRecord.SourceSnapshotSHA256}
		workspace, err := runworkspace.Create(m.workspaceRoot, marker)
		if err != nil {
			message := err.Error()
			m.finish(runRecord.ID, store.RunError, &message)
			return
		}
		defer func() {
			if err := runworkspace.Remove(m.workspaceRoot, workspace, marker); err != nil {
				fmt.Fprintf(m.output, "workspace cleanup failed: %v\n", err)
			}
		}()
		sourceDir := filepath.Join(workspace, "source")
		if err := os.Mkdir(sourceDir, 0700); err != nil {
			message := err.Error()
			m.finish(runRecord.ID, store.RunError, &message)
			return
		}
		blob, err := m.snapshots.OpenBlob(*runRecord.SourceSnapshotSHA256)
		if err != nil {
			message := err.Error()
			m.finish(runRecord.ID, store.RunError, &message)
			return
		}
		blobPath := blob.Name()
		blob.Close()
		meta := snapshot.Metadata{SourceDigest: *runRecord.SourceSnapshotSHA256, BlobDigest: runRecord.SnapshotBlobSHA256, Format: runRecord.SnapshotFormat, ArchiveSizeBytes: runRecord.SnapshotArchiveSize, LogicalSizeBytes: runRecord.SnapshotLogicalSize, EntryCount: runRecord.SnapshotEntryCount}
		if err := snapshot.Extract(blobPath, sourceDir, meta, m.snapshots.Limits()); err != nil {
			message := err.Error()
			m.finish(runRecord.ID, store.RunError, &message)
			return
		}
		executionWorkspace = sourceDir
	}
	cfg, err := config.ParseBytes(runRecord.PipelineYAML, "stored pipeline snapshot")
	if err != nil {
		message := err.Error()
		m.finish(runRecord.ID, store.RunError, &message)
		return
	}
	graph, err := pipeline.Compile(cfg)
	if err != nil {
		message := err.Error()
		m.finish(runRecord.ID, store.RunError, &message)
		return
	}
	local := executor.Local{Directory: executionWorkspace}
	var docker *executor.Docker
	for _, job := range cfg.Jobs {
		if job.Image != nil {
			docker, err = executor.NewDocker(executionWorkspace)
			break
		}
	}
	if err != nil {
		message := err.Error()
		m.finish(runRecord.ID, store.RunError, &message)
		return
	}
	obs := &observer{runID: runRecord.ID, store: m.store, cancel: cancel}
	exec := executor.Job{Local: local, Docker: docker}
	result := (runner.Runner{Executor: exec, Output: m.output, ErrorOutput: m.output, MaxParallel: runRecord.MaxParallel, Observer: obs}).Run(ctx, graph)
	status := store.RunFailed
	var message *string
	if obs.err != nil {
		status = store.RunError
		text := obs.err.Error()
		message = &text
	} else if result.InternalError {
		status = store.RunError
		text := "job infrastructure failure"
		message = &text
		for name, state := range result.States {
			if state == runner.Failed {
				updateCtx, updateCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = m.store.UpdateJob(updateCtx, runRecord.ID, name, store.JobFailed, &text)
				updateCancel()
			}
		}
	} else if result.Interrupted {
		status = store.RunCanceled
	} else if result.Succeeded() {
		status = store.RunPassed
	}
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finishCancel()
	if err := m.store.FinishRun(finishCtx, runRecord.ID, status, message); err != nil {
		fmt.Fprintf(m.output, "run %s final persistence failed: %v\n", runRecord.ID, err)
	}
}

func (m *Manager) finish(id string, status store.RunStatus, message *string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.store.FinishRun(ctx, id, status, message); err != nil {
		fmt.Fprintf(m.output, "run %s final persistence failed: %v\n", id, err)
	}
}

func IsClientError(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict)
}
