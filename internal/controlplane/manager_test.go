package controlplane

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

type fakeStore struct {
	mu        sync.Mutex
	runs      map[string]*store.Run
	order     []string
	claimGate <-chan struct{}
	recovered bool
	updateErr error
}

func newFakeStore() *fakeStore                  { return &fakeStore{runs: map[string]*store.Run{}} }
func (f *fakeStore) Ping(context.Context) error { return nil }
func (f *fakeStore) Close()                     {}
func (f *fakeStore) CreateRun(_ context.Context, in store.CreateRun) (*store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UTC()
	r := &store.Run{ID: in.ID, Status: store.RunQueued, PipelineFile: in.PipelineFile, PipelineYAML: append([]byte(nil), in.PipelineYAML...), PipelineSHA256: in.PipelineSHA256, Workspace: in.Workspace, MaxParallel: in.MaxParallel, CreatedAt: now, Jobs: append([]store.Job(nil), in.Jobs...)}
	f.runs[in.ID] = r
	f.order = append(f.order, in.ID)
	return cloneRun(r), nil
}
func (f *fakeStore) GetRun(_ context.Context, id string) (*store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.runs[id]
	if r == nil {
		return nil, store.ErrNotFound
	}
	return cloneRun(r), nil
}
func (f *fakeStore) ListRuns(_ context.Context, limit int) ([]store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.Run
	for i := len(f.order) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, *cloneRun(f.runs[f.order[i]]))
	}
	return out, nil
}
func (f *fakeStore) ClaimNextQueuedRun(ctx context.Context) (*store.Run, error) {
	if f.claimGate != nil {
		select {
		case <-f.claimGate:
			f.claimGate = nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.order {
		r := f.runs[id]
		if r.Status == store.RunQueued {
			now := time.Now().UTC()
			r.Status = store.RunRunning
			r.StartedAt = &now
			return cloneRun(r), nil
		}
	}
	return nil, nil
}
func (f *fakeStore) FinishRun(_ context.Context, id string, status store.RunStatus, message *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.runs[id]
	if r == nil {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	r.Status = status
	r.FinishedAt = &now
	r.ErrorMessage = message
	return nil
}
func (f *fakeStore) UpdateJob(_ context.Context, id, name string, status store.JobStatus, message *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.updateErr != nil {
		return f.updateErr
	}
	r := f.runs[id]
	for i := range r.Jobs {
		if r.Jobs[i].Name == name {
			now := time.Now().UTC()
			r.Jobs[i].Status = status
			r.Jobs[i].ErrorMessage = message
			if status == store.JobRunning {
				r.Jobs[i].StartedAt = &now
			}
			if status != store.JobPending && status != store.JobRunning {
				r.Jobs[i].FinishedAt = &now
			}
			return nil
		}
	}
	return errors.New("unknown job")
}

func TestPersistenceFailureStopsRunWithError(t *testing.T) {
	dir := t.TempDir()
	writeConfig(t, dir, "forge.yaml", "touch "+filepath.Join(dir, "must-not-complete"))
	persistence := newFakeStore()
	persistence.updateErr = errors.New("database unavailable")
	manager, _ := New(context.Background(), persistence, dir, nil)
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	run, err := manager.Submit(context.Background(), "forge.yaml", 1)
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, persistence, run.ID, store.RunError)
	if _, err := os.Stat(filepath.Join(dir, "must-not-complete")); !os.IsNotExist(err) {
		t.Fatal("work completed after persistence failure")
	}
}
func (f *fakeStore) CancelQueued(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.runs[id]
	if r == nil {
		return store.ErrNotFound
	}
	if r.Status != store.RunQueued {
		return store.ErrConflict
	}
	now := time.Now().UTC()
	r.Status = store.RunCanceled
	r.FinishedAt = &now
	r.CancelRequestedAt = &now
	for i := range r.Jobs {
		r.Jobs[i].Status = store.JobCanceled
	}
	return nil
}
func (f *fakeStore) RequestCancel(_ context.Context, id string) (store.RunStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.runs[id]
	if r == nil {
		return "", store.ErrNotFound
	}
	if r.Status != store.RunRunning {
		return r.Status, store.ErrConflict
	}
	now := time.Now().UTC()
	r.CancelRequestedAt = &now
	return r.Status, nil
}
func (f *fakeStore) RecoverInterrupted(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recovered = true
	for _, r := range f.runs {
		if r.Status == store.RunRunning {
			r.Status = store.RunAborted
			for i := range r.Jobs {
				if r.Jobs[i].Status == store.JobPending || r.Jobs[i].Status == store.JobRunning {
					r.Jobs[i].Status = store.JobAborted
				}
			}
		}
	}
	return nil
}
func cloneRun(r *store.Run) *store.Run {
	copy := *r
	copy.PipelineYAML = append([]byte(nil), r.PipelineYAML...)
	copy.Jobs = append([]store.Job(nil), r.Jobs...)
	sort.Slice(copy.Jobs, func(i, j int) bool { return copy.Jobs[i].Name < copy.Jobs[j].Name })
	return &copy
}

func writeConfig(t *testing.T, dir, name, command string) {
	t.Helper()
	body := "version: 1\njobs:\n  test:\n    steps:\n      - run: " + command + "\n"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
func waitStatus(t *testing.T, f *fakeStore, id string, want store.RunStatus) *store.Run {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r, _ := f.GetRun(context.Background(), id)
		if r.Status == want {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	r, _ := f.GetRun(context.Background(), id)
	t.Fatalf("status=%s want=%s", r.Status, want)
	return nil
}

func TestManagerExecutesPersistedPipelineSnapshot(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "result")
	writeConfig(t, dir, "forge.yaml", "echo VERSION_A > "+output)
	gate := make(chan struct{})
	persistence := newFakeStore()
	persistence.claimGate = gate
	manager, _ := New(context.Background(), persistence, dir, nil)
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	run, err := manager.Submit(context.Background(), "forge.yaml", 1)
	if err != nil {
		t.Fatal(err)
	}
	writeConfig(t, dir, "forge.yaml", "echo VERSION_B > "+output)
	close(gate)
	waitStatus(t, persistence, run.ID, store.RunPassed)
	data, err := os.ReadFile(output)
	if err != nil || string(data) != "VERSION_A\n" {
		t.Fatalf("output=%q err=%v", data, err)
	}
}

func TestManagerRunsOnePipelineAtATimeFIFO(t *testing.T) {
	dir := t.TempDir()
	release := filepath.Join(dir, "release")
	bStarted := filepath.Join(dir, "b-started")
	gate := make(chan struct{})
	persistence := newFakeStore()
	persistence.claimGate = gate
	manager, _ := New(context.Background(), persistence, dir, nil)
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	writeConfig(t, dir, "a.yaml", "while [ ! -f "+release+" ]; do sleep 0.02; done")
	a, _ := manager.Submit(context.Background(), "a.yaml", 4)
	writeConfig(t, dir, "b.yaml", "touch "+bStarted)
	b, _ := manager.Submit(context.Background(), "b.yaml", 4)
	close(gate)
	waitStatus(t, persistence, a.ID, store.RunRunning)
	br, _ := persistence.GetRun(context.Background(), b.ID)
	if br.Status != store.RunQueued {
		t.Fatalf("B status=%s", br.Status)
	}
	if _, err := os.Stat(bStarted); !os.IsNotExist(err) {
		t.Fatal("run B started while A was active")
	}
	if err := os.WriteFile(release, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, persistence, a.ID, store.RunPassed)
	waitStatus(t, persistence, b.ID, store.RunPassed)
}

func TestManagerCancelsQueuedRun(t *testing.T) {
	dir := t.TempDir()
	gate := make(chan struct{})
	persistence := newFakeStore()
	persistence.claimGate = gate
	manager, _ := New(context.Background(), persistence, dir, nil)
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	writeConfig(t, dir, "forge.yaml", "true")
	run, _ := manager.Submit(context.Background(), "forge.yaml", 1)
	status, err := manager.Cancel(context.Background(), run.ID)
	if err != nil || status != store.RunCanceled {
		t.Fatalf("status=%s err=%v", status, err)
	}
	close(gate)
	r, _ := persistence.GetRun(context.Background(), run.ID)
	if r.Status != store.RunCanceled || r.Jobs[0].Status != store.JobCanceled {
		t.Fatalf("run=%+v", r)
	}
}

func TestManagerRejectsEscapingPipelinePaths(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.yaml")
	if err := os.WriteFile(outside, []byte("version: 1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "link.yaml")); err != nil {
		t.Fatal(err)
	}
	manager, _ := New(context.Background(), newFakeStore(), dir, nil)
	for _, name := range []string{"/absolute.yaml", "../outside.yaml", "ci/../../outside.yaml", "link.yaml", "bad\x00.yaml"} {
		t.Run(name, func(t *testing.T) {
			if _, err := manager.Submit(context.Background(), name, 1); err == nil {
				t.Fatal("Submit accepted unsafe path")
			}
		})
	}
}

func TestStartupRecoveryAbortsOnlyRunning(t *testing.T) {
	dir := t.TempDir()
	persistence := newFakeStore()
	persistence.runs["running"] = &store.Run{ID: "running", Status: store.RunRunning, Jobs: []store.Job{{Name: "a", Status: store.JobRunning}, {Name: "done", Status: store.JobPassed}}}
	persistence.runs["queued"] = &store.Run{ID: "queued", Status: store.RunQueued}
	persistence.runs["passed"] = &store.Run{ID: "passed", Status: store.RunPassed}
	manager, _ := New(context.Background(), persistence, dir, nil)
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Close()
	if persistence.runs["running"].Status != store.RunAborted || persistence.runs["running"].Jobs[0].Status != store.JobAborted || persistence.runs["running"].Jobs[1].Status != store.JobPassed || persistence.runs["passed"].Status != store.RunPassed {
		t.Fatalf("recovery=%+v", persistence.runs)
	}
}
