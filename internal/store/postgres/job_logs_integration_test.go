package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/store"
)

func TestJobLogsLifecycle(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	r, err := s.CreateRun(ctx, store.CreateRun{ID: uuid.NewString(), PipelineFile: "x", PipelineYAML: []byte("version: 1"), PipelineSHA256: strings.Repeat("a", 64), Workspace: "/logs", MaxParallel: 1, Jobs: []store.Job{{Name: "build"}, {Name: "empty"}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	put := func(n int64, stream store.JobLogStream, data string) error {
		return s.AppendJobLog(ctx, store.JobLogChunk{RunID: r.ID, JobName: "build", Sequence: n, Stream: stream, Payload: []byte(data)})
	}
	if err := put(1, store.JobLogStdout, "alpha"); err != nil {
		t.Fatal(err)
	}
	if err := put(2, store.JobLogStderr, "warning"); err != nil {
		t.Fatal(err)
	}
	if err := put(3, store.JobLogStdout, "omega"); err != nil {
		t.Fatal(err)
	}
	if err := put(2, store.JobLogStderr, "warning"); err != nil {
		t.Fatal(err)
	}
	if err := put(2, store.JobLogStderr, "bad"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if err := put(5, store.JobLogStdout, "gap"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("gap=%v", err)
	}
	got, err := s.ListJobLogs(ctx, r.ID, "build", 0, 2)
	if err != nil || len(got) != 2 || got[0].Sequence != 1 || got[1].Sequence != 2 {
		t.Fatalf("page=%+v err=%v", got, err)
	}
	got, err = s.ListJobLogs(ctx, r.ID, "build", 1, 1000)
	if err != nil || len(got) != 2 || got[0].Sequence != 2 {
		t.Fatalf("cursor=%+v err=%v", got, err)
	}
	if got[0].CreatedAt.IsZero() {
		t.Fatal("timestamp zero")
	}
	got, err = s.ListJobLogs(ctx, r.ID, "empty", 0, 256)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty=%+v err=%v", got, err)
	}
	if _, err = s.ListJobLogs(ctx, r.ID, "missing", 0, 256); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown job=%v", err)
	}
}
