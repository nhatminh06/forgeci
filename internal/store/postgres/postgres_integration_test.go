package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/store"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	s, err := Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), `TRUNCATE job_runs, pipeline_runs`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestMigrationIsIdempotentAndCreateRunIsAtomic(t *testing.T) {
	s := integrationStore(t)
	var versions int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations WHERE version=1`).Scan(&versions); err != nil || versions != 1 {
		t.Fatalf("versions=%d err=%v", versions, err)
	}
	second, err := Open(context.Background(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	second.Close()

	id := uuid.NewString()
	_, err = s.CreateRun(context.Background(), store.CreateRun{ID: id, PipelineFile: "forge.yaml", PipelineYAML: []byte("version: 1"), PipelineSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Workspace: "/workspace", MaxParallel: 1, Jobs: []store.Job{{Name: "same"}, {Name: "same"}}})
	if err == nil {
		t.Fatal("duplicate job insertion unexpectedly succeeded")
	}
	var runs, jobs int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM pipeline_runs WHERE id=$1`, id).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM job_runs WHERE run_id=$1`, id).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 || jobs != 0 {
		t.Fatalf("transaction leaked runs=%d jobs=%d", runs, jobs)
	}
}

func TestClaimCancelAndRecovery(t *testing.T) {
	s := integrationStore(t)
	create := func() string {
		id := uuid.NewString()
		_, err := s.CreateRun(context.Background(), store.CreateRun{ID: id, PipelineFile: "forge.yaml", PipelineYAML: []byte("version: 1"), PipelineSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Workspace: "/workspace", MaxParallel: 1, Jobs: []store.Job{{Name: "job"}}})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first, second := create(), create()
	claimed, err := s.ClaimNextQueuedRun(context.Background())
	if err != nil || claimed.ID != first {
		t.Fatalf("claimed=%v err=%v want=%s", claimed, err, first)
	}
	if err := s.CancelQueued(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateJob(context.Background(), first, "job", store.JobRunning, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.RecoverInterrupted(context.Background()); err != nil {
		t.Fatal(err)
	}
	run, _ := s.GetRun(context.Background(), first)
	if run.Status != store.RunAborted || run.Jobs[0].Status != store.JobAborted {
		t.Fatalf("recovered=%+v", run)
	}
	canceled, _ := s.GetRun(context.Background(), second)
	if canceled.Status != store.RunCanceled || canceled.Jobs[0].Status != store.JobCanceled {
		t.Fatalf("canceled=%+v", canceled)
	}
}
