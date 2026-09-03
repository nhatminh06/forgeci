package postgres

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/artifact"
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
	if _, err := s.pool.Exec(context.Background(), `TRUNCATE runners, job_runs, pipeline_runs CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func testSnapshot() *store.SourceSnapshot {
	return &store.SourceSnapshot{SourceDigest: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", BlobDigest: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Format: "tar-gzip-v1", ArchiveSizeBytes: 10, LogicalSizeBytes: 5, EntryCount: 1, CreatedAt: time.Now().UTC()}
}

func createRemoteRun(t *testing.T, s *Store, image *string) *store.Run {
	t.Helper()
	r, err := s.CreateRun(context.Background(), store.CreateRun{ID: uuid.NewString(), PipelineFile: "forge.yaml", PipelineYAML: []byte("version: 1\njobs:\n  job:\n    steps:\n      - run: true\n"), PipelineSHA256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Workspace: "/workspace", MaxParallel: 4, Jobs: []store.Job{{Name: "job", Image: image}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestArtifactSetCommitPrecedesPassedAndExpires(t *testing.T) {
	s := integrationStore(t)
	s.SetArtifactRetention(time.Hour)
	ctx := context.Background()
	yaml := []byte("version: 1\njobs:\n  build:\n    steps:\n      - run: true\n    artifacts:\n      upload:\n        - name: app\n          path: dist/app\n")
	run, err := s.CreateRun(ctx, store.CreateRun{ID: uuid.NewString(), PipelineFile: "forge.yaml", PipelineYAML: yaml, PipelineSHA256: strings.Repeat("c", 64), Workspace: "/workspace", MaxParallel: 1, Jobs: []store.Job{{Name: "build"}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimNextQueuedRun(ctx)
	if err != nil || claimed.ID != run.ID {
		t.Fatal(err)
	}
	if err := s.UpdateJob(ctx, run.ID, "build", store.JobRunning, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateJob(ctx, run.ID, "build", store.JobPassed, nil); err == nil {
		t.Fatal("job passed without committed artifacts")
	}
	item := store.Artifact{RunID: run.ID, ProducerJob: "build", Name: "app", RootName: "app", RootKind: "file", ContentSHA256: strings.Repeat("a", 64), BlobSHA256: strings.Repeat("b", 64), Format: artifact.Format, ArchiveSizeBytes: 10, LogicalSizeBytes: 5, EntryCount: 1, CreatedAt: time.Now().UTC()}
	owner := store.ArtifactOwnership{RunID: run.ID, JobName: "build"}
	if err := s.CommitArtifacts(ctx, owner, []store.Artifact{item}); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitArtifacts(ctx, owner, []store.Artifact{item}); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	conflict := item
	conflict.BlobSHA256 = strings.Repeat("d", 64)
	if err := s.CommitArtifacts(ctx, owner, []store.Artifact{conflict}); err == nil {
		t.Fatal("conflicting replay accepted")
	}
	if err := s.UpdateJob(ctx, run.ID, "build", store.JobPassed, nil); err != nil {
		t.Fatal(err)
	}
	active, err := s.ListArtifacts(ctx, run.ID)
	if err != nil || active[0].ExpiresAt != nil {
		t.Fatalf("active-run expiry=%+v err=%v", active, err)
	}
	if digests, err := s.ExpireArtifacts(ctx, time.Now().Add(365*24*time.Hour)); err != nil || len(digests) != 0 {
		t.Fatalf("active run expired digests=%v err=%v", digests, err)
	}
	if err := s.FinishRun(ctx, run.ID, store.RunPassed, nil); err != nil {
		t.Fatal(err)
	}
	items, err := s.ListArtifacts(ctx, run.ID)
	if err != nil || len(items) != 1 || items[0].ExpiresAt == nil || !items[0].Available {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	digests, err := s.ExpireArtifacts(ctx, items[0].ExpiresAt.Add(time.Nanosecond))
	if err != nil || len(digests) != 1 || digests[0] != item.BlobSHA256 {
		t.Fatalf("digests=%v err=%v", digests, err)
	}
	expired, err := s.GetArtifact(ctx, run.ID, "build", "app")
	if err != nil || expired.Available {
		t.Fatalf("expired=%+v err=%v", expired, err)
	}
}

func TestArtifactDownloadLookupIsRunAndLeaseScoped(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	yaml := []byte("version: 1\njobs:\n  build:\n    steps:\n      - run: true\n    artifacts:\n      upload:\n        - name: app\n          path: out/app\n  consume:\n    needs: [build]\n    artifacts:\n      download:\n        - from: build\n          name: app\n          into: input\n    steps:\n      - run: true\n")
	run, err := s.CreateRun(ctx, store.CreateRun{ID: uuid.NewString(), PipelineFile: "forge.yaml", PipelineYAML: yaml, PipelineSHA256: strings.Repeat("c", 64), Workspace: "/workspace", MaxParallel: 1, Jobs: []store.Job{{Name: "build"}, {Name: "consume"}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	worker := createRunner(t, s, "artifact-owner", false)
	lease, err := s.LeaseRun(ctx, worker.ID, "")
	if err != nil || lease.ID != run.ID {
		t.Fatal(err)
	}
	owner := store.ArtifactOwnership{RunID: run.ID, RunnerID: worker.ID, LeaseID: *lease.LeaseID, Generation: lease.LeaseGeneration, JobName: "build"}
	if err := s.ReportJobEvent(ctx, run.ID, worker.ID, *lease.LeaseID, lease.LeaseGeneration, "build", store.JobRunning); err != nil {
		t.Fatal(err)
	}
	item := store.Artifact{RunID: run.ID, ProducerJob: "build", Name: "app", RootName: "app", RootKind: "file", ContentSHA256: strings.Repeat("a", 64), BlobSHA256: strings.Repeat("b", 64), Format: artifact.Format, ArchiveSizeBytes: 10, LogicalSizeBytes: 5, EntryCount: 1, CreatedAt: time.Now().UTC()}
	if err := s.CommitArtifacts(ctx, owner, []store.Artifact{item}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReportJobEvent(ctx, run.ID, worker.ID, *lease.LeaseID, lease.LeaseGeneration, "build", store.JobPassed); err != nil {
		t.Fatal(err)
	}
	if err := s.ReportJobEvent(ctx, run.ID, worker.ID, *lease.LeaseID, lease.LeaseGeneration, "consume", store.JobRunning); err != nil {
		t.Fatal(err)
	}
	owner.JobName = "consume"
	if _, err := s.GetArtifactForLease(ctx, owner, "build", "app"); err != nil {
		t.Fatalf("valid download: %v", err)
	}
	wrongRun := owner
	wrongRun.RunID = uuid.NewString()
	if _, err := s.GetArtifactForLease(ctx, wrongRun, "build", "app"); err == nil {
		t.Fatal("cross-run artifact lookup accepted")
	}
	wrongRunner := owner
	wrongRunner.RunnerID = uuid.NewString()
	if _, err := s.GetArtifactForLease(ctx, wrongRunner, "build", "app"); err == nil {
		t.Fatal("wrong-runner artifact lookup accepted")
	}
}

func createRunner(t *testing.T, s *Store, name string, docker bool) *store.Runner {
	t.Helper()
	r, err := s.RegisterRunner(context.Background(), store.Runner{ID: uuid.NewString(), Name: name, ProtocolVersion: 1, OS: "linux", Arch: "amd64", DockerAvailable: docker, MaxParallel: 2})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRemoteLeaseOwnershipTransitionsAndExpiration(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	run := createRemoteRun(t, s, nil)
	owner := createRunner(t, s, "owner", false)
	other := createRunner(t, s, "other", false)
	lease, err := s.LeaseRun(ctx, owner.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	id, generation := *lease.LeaseID, lease.LeaseGeneration
	assertUnchanged := func() {
		got, err := s.GetRun(ctx, run.ID)
		if err != nil || got.Status != store.RunRunning || got.Jobs[0].Status != store.JobPending {
			t.Fatalf("unexpected state: %+v err=%v", got, err)
		}
	}
	if err := s.RenewLease(ctx, run.ID, other.ID, id, generation, time.Now().Add(time.Minute)); err == nil {
		t.Fatal("wrong runner renewed lease")
	}
	assertUnchanged()
	if err := s.ReportJobEvent(ctx, run.ID, other.ID, id, generation, "job", store.JobRunning); err == nil {
		t.Fatal("wrong runner changed job")
	}
	assertUnchanged()
	if err := s.CompleteRun(ctx, run.ID, other.ID, id, generation, store.RunPassed, nil); err == nil {
		t.Fatal("wrong runner completed run")
	}
	assertUnchanged()
	if err := s.ReportJobEvent(ctx, run.ID, owner.ID, id, generation, "job", store.JobRunning); err != nil {
		t.Fatal(err)
	}
	if err := s.ReportJobEvent(ctx, run.ID, owner.ID, id, generation, "job", store.JobRunning); err != nil {
		t.Fatalf("duplicate replay: %v", err)
	}
	if err := s.ReportJobEvent(ctx, run.ID, owner.ID, id, generation, "job", store.JobPassed); err != nil {
		t.Fatal(err)
	}
	if err := s.ReportJobEvent(ctx, run.ID, owner.ID, id, generation, "job", store.JobRunning); err == nil {
		t.Fatal("regressive transition accepted")
	}
	if _, err := s.pool.Exec(ctx, `UPDATE pipeline_runs SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteRun(ctx, run.ID, owner.ID, id, generation, store.RunPassed, nil); err == nil {
		t.Fatal("expired running lease completed run")
	}
	if err := s.ExpireLeases(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteRun(ctx, run.ID, owner.ID, id, generation, store.RunPassed, nil); err == nil {
		t.Fatal("stale completion accepted")
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Status != store.RunAborted {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestConcurrentRemoteClaimsAndOneRunPerRunner(t *testing.T) {
	for attempt := 0; attempt < 10; attempt++ {
		s := integrationStore(t)
		ctx := context.Background()
		wanted := createRemoteRun(t, s, nil)
		a := createRunner(t, s, "a", false)
		b := createRunner(t, s, "b", false)
		start := make(chan struct{})
		results := make(chan *store.Run, 2)
		errors := make(chan error, 2)
		var wg sync.WaitGroup
		for _, id := range []string{a.ID, b.ID} {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				<-start
				r, err := s.LeaseRun(ctx, id, "")
				results <- r
				errors <- err
			}(id)
		}
		close(start)
		wg.Wait()
		close(results)
		close(errors)
		for err := range errors {
			if err != nil {
				t.Fatal(err)
			}
		}
		claims := 0
		for r := range results {
			if r != nil {
				claims++
				if r.ID != wanted.ID {
					t.Fatalf("wrong run %s", r.ID)
				}
			}
		}
		if claims != 1 {
			t.Fatalf("attempt %d claims=%d", attempt, claims)
		}
		createRemoteRun(t, s, nil)
		var owner string
		if err := s.pool.QueryRow(ctx, `SELECT runner_id::text FROM pipeline_runs WHERE id=$1`, wanted.ID).Scan(&owner); err != nil {
			t.Fatal(err)
		}
		if second, err := s.LeaseRun(ctx, owner, ""); err != nil || second != nil {
			t.Fatalf("second lease=%v err=%v", second, err)
		}
	}
}

func TestRenewalReaperRaceHasConsistentOutcome(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		s := integrationStore(t)
		ctx := context.Background()
		run := createRemoteRun(t, s, nil)
		runner := createRunner(t, s, "runner", false)
		lease, _ := s.LeaseRun(ctx, runner.ID, "")
		deadline := time.Now().Add(100 * time.Millisecond)
		_, _ = s.pool.Exec(ctx, `UPDATE pipeline_runs SET lease_expires_at=$2 WHERE id=$1`, run.ID, deadline)
		start := make(chan struct{})
		renewed := make(chan error, 1)
		reaped := make(chan error, 1)
		go func() {
			<-start
			renewed <- s.RenewLease(ctx, run.ID, runner.ID, *lease.LeaseID, lease.LeaseGeneration, time.Now().Add(time.Minute))
		}()
		go func() { <-start; reaped <- s.ExpireLeases(ctx, deadline.Add(time.Nanosecond)) }()
		close(start)
		renewErr := <-renewed
		if err := <-reaped; err != nil {
			t.Fatal(err)
		}
		got, err := s.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == store.RunRunning && (got.LeaseExpiresAt == nil || !got.LeaseExpiresAt.After(deadline)) {
			t.Fatal("running lease was not renewed")
		}
		if got.Status != store.RunRunning && got.Status != store.RunAborted {
			t.Fatalf("invalid outcome %s", got.Status)
		}
		if renewErr == nil && got.Status == store.RunAborted {
			t.Fatal("renewal succeeded but stale reaper aborted the run")
		}
	}
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
	_, err = s.CreateRun(context.Background(), store.CreateRun{ID: id, PipelineFile: "forge.yaml", PipelineYAML: []byte("version: 1"), PipelineSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Workspace: "/workspace", MaxParallel: 1, Jobs: []store.Job{{Name: "same"}, {Name: "same"}}, Snapshot: testSnapshot()})
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
		_, err := s.CreateRun(context.Background(), store.CreateRun{ID: id, PipelineFile: "forge.yaml", PipelineYAML: []byte("version: 1"), PipelineSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Workspace: "/workspace", MaxParallel: 1, Jobs: []store.Job{{Name: "job"}}, Snapshot: testSnapshot()})
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
