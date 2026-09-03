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
	run, err := s.CreateRun(ctx, store.CreateRun{ID: uuid.NewString(), PipelineFile: "forge.yaml", PipelineYAML: yaml, PipelineSHA256: strings.Repeat("c", 64), Workspace: "/workspace", MaxParallel: 1, Jobs: []store.Job{{Name: "build"}, {Name: "consume"}}, Dependencies: []store.JobDependency{{JobName: "consume", DependsOn: "build"}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	worker := createRunner(t, s, "artifact-owner", false)
	lease, err := s.LeaseJob(ctx, worker.ID)
	if err != nil || lease.RunID != run.ID {
		t.Fatal(err)
	}
	owner := store.ArtifactOwnership{RunID: run.ID, RunnerID: worker.ID, LeaseID: lease.LeaseID, Generation: lease.Generation, JobName: "build"}
	item := store.Artifact{RunID: run.ID, ProducerJob: "build", Name: "app", RootName: "app", RootKind: "file", ContentSHA256: strings.Repeat("a", 64), BlobSHA256: strings.Repeat("b", 64), Format: artifact.Format, ArchiveSizeBytes: 10, LogicalSizeBytes: 5, EntryCount: 1, CreatedAt: time.Now().UTC()}
	if err := s.CommitArtifacts(ctx, owner, []store.Artifact{item}); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteJob(ctx, owner, store.JobPassed, nil); err != nil {
		t.Fatal(err)
	}
	consumer, err := s.LeaseJob(ctx, worker.ID)
	if err != nil || consumer == nil || consumer.JobName != "consume" {
		t.Fatalf("consumer=%+v err=%v", consumer, err)
	}
	owner = store.ArtifactOwnership{RunID: run.ID, RunnerID: worker.ID, LeaseID: consumer.LeaseID, Generation: consumer.Generation, JobName: "consume"}
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
	r, err := s.RegisterRunner(context.Background(), store.Runner{ID: uuid.NewString(), Name: name, ProtocolVersion: 2, OS: "linux", Arch: "amd64", DockerAvailable: docker, MaxParallel: 2})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestJobClaimsEnforceDependencyRunAndRunnerCapacity(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	yaml := []byte("version: 1\njobs:\n  a:\n    steps: [{run: true}]\n  b:\n    steps: [{run: true}]\n  c:\n    steps: [{run: true}]\n")
	run, err := s.CreateRun(ctx, store.CreateRun{ID: uuid.NewString(), PipelineFile: "forge.yaml", PipelineYAML: yaml, PipelineSHA256: strings.Repeat("a", 64), Workspace: "/workspace", MaxParallel: 2, Jobs: []store.Job{{Name: "a"}, {Name: "b"}, {Name: "c"}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	one, err := s.RegisterRunner(ctx, store.Runner{ID: uuid.NewString(), Name: "one", ProtocolVersion: 2, OS: "linux", Arch: "amd64", MaxParallel: 1, Status: store.RunnerOnline})
	if err != nil {
		t.Fatal(err)
	}
	two, err := s.RegisterRunner(ctx, store.Runner{ID: uuid.NewString(), Name: "two", ProtocolVersion: 2, OS: "linux", Arch: "amd64", MaxParallel: 2, Status: store.RunnerOnline})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	claimsCh := make(chan *store.JobLease, 12)
	errs := make(chan error, 12)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; l, e := s.LeaseJob(ctx, one.ID); claimsCh <- l; errs <- e }()
	}
	close(start)
	wg.Wait()
	close(claimsCh)
	close(errs)
	claims := 0
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	for l := range claimsCh {
		if l != nil {
			claims++
		}
	}
	if claims != 1 {
		t.Fatalf("single-capacity runner claims=%d", claims)
	}
	owned, err := s.pool.Query(ctx, `SELECT run_id::text,job_name,lease_id::text,lease_generation FROM job_runs WHERE runner_id=$1 AND status='RUNNING'`, one.ID)
	if err != nil {
		t.Fatal(err)
	}
	var heartbeat store.LeaseHeartbeat
	if !owned.Next() {
		t.Fatal("owned lease missing")
	}
	if err = owned.Scan(&heartbeat.RunID, &heartbeat.JobName, &heartbeat.LeaseID, &heartbeat.Generation); err != nil {
		t.Fatal(err)
	}
	owned.Close()
	heartbeatResults, err := s.HeartbeatJobLeases(ctx, one.ID, []store.LeaseHeartbeat{heartbeat, {RunID: heartbeat.RunID, JobName: heartbeat.JobName, LeaseID: uuid.NewString(), Generation: heartbeat.Generation}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(heartbeatResults) != 2 || !heartbeatResults[0].Valid || heartbeatResults[1].Valid {
		t.Fatalf("heartbeat results=%+v", heartbeatResults)
	}
	second, err := s.LeaseJob(ctx, two.ID)
	if err != nil || second == nil {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	if third, err := s.LeaseJob(ctx, two.ID); err != nil || third != nil {
		t.Fatalf("run max_parallel exceeded: %+v %v", third, err)
	}
	active, _ := s.GetRun(ctx, run.ID)
	running := 0
	for _, j := range active.Jobs {
		if j.Status == store.JobRunning {
			running++
		}
	}
	if running != 2 {
		t.Fatalf("running=%d", running)
	}
}

func TestJobClaimMatchesDockerCapability(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	image := "alpine:3.22"
	run, err := s.CreateRun(ctx, store.CreateRun{
		ID: uuid.NewString(), PipelineFile: "forge.yaml",
		PipelineYAML:   []byte("version: 1\njobs:\n  a-docker:\n    image: alpine:3.22\n    steps: [{run: true}]\n  z-local:\n    steps: [{run: true}]\n"),
		PipelineSHA256: strings.Repeat("e", 64), Workspace: "/workspace", MaxParallel: 2,
		Jobs: []store.Job{{Name: "a-docker", Image: &image}, {Name: "z-local"}}, Snapshot: testSnapshot(),
	})
	if err != nil {
		t.Fatal(err)
	}
	plain := createRunner(t, s, "plain", false)
	lease, err := s.LeaseJob(ctx, plain.ID)
	if err != nil || lease == nil || lease.RunID != run.ID || lease.JobName != "z-local" {
		t.Fatalf("non-Docker runner lease=%+v err=%v", lease, err)
	}
	docker := createRunner(t, s, "docker", true)
	lease, err = s.LeaseJob(ctx, docker.ID)
	if err != nil || lease == nil || lease.JobName != "a-docker" {
		t.Fatalf("Docker runner lease=%+v err=%v", lease, err)
	}
}

func TestJobHeartbeatReaperRaceHasConsistentOutcome(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		run := createRemoteRun(t, s, nil)
		runner := createRunner(t, s, "race-"+uuid.NewString(), false)
		lease, err := s.LeaseJob(ctx, runner.ID)
		if err != nil || lease == nil {
			t.Fatalf("lease=%+v err=%v", lease, err)
		}
		deadline := time.Now().UTC()
		if _, err = s.pool.Exec(ctx, `UPDATE job_runs SET lease_expires_at=$3 WHERE run_id=$1 AND job_name=$2`, run.ID, lease.JobName, deadline); err != nil {
			t.Fatal(err)
		}
		heartbeat := store.LeaseHeartbeat{RunID: run.ID, JobName: lease.JobName, LeaseID: lease.LeaseID, Generation: lease.Generation}
		start := make(chan struct{})
		renewed := make(chan []store.LeaseHeartbeatResult, 1)
		errs := make(chan error, 2)
		go func() {
			<-start
			results, e := s.HeartbeatJobLeases(ctx, runner.ID, []store.LeaseHeartbeat{heartbeat}, deadline.Add(-time.Nanosecond))
			renewed <- results
			errs <- e
		}()
		go func() { <-start; errs <- s.ExpireJobLeases(ctx, deadline.Add(time.Nanosecond)) }()
		close(start)
		firstErr, secondErr := <-errs, <-errs
		if firstErr != nil || secondErr != nil {
			t.Fatalf("race errors: %v %v", firstErr, secondErr)
		}
		results := <-renewed
		got, err := s.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		valid := len(results) == 1 && results[0].Valid
		if valid && got.Jobs[0].Status != store.JobRunning {
			t.Fatalf("renewed job became %s", got.Jobs[0].Status)
		}
		if !valid && got.Jobs[0].Status != store.JobAborted {
			t.Fatalf("lost renewal left job %s", got.Jobs[0].Status)
		}
	}
}

func TestJobDependencyReadinessFailureAndStaleCompletion(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	yaml := []byte("version: 1\njobs:\n  root:\n    steps: [{run: true}]\n  child:\n    needs: [root]\n    steps: [{run: true}]\n")
	run, err := s.CreateRun(ctx, store.CreateRun{ID: uuid.NewString(), PipelineFile: "forge.yaml", PipelineYAML: yaml, PipelineSHA256: strings.Repeat("b", 64), Workspace: "/workspace", MaxParallel: 2, Jobs: []store.Job{{Name: "root"}, {Name: "child"}}, Dependencies: []store.JobDependency{{JobName: "child", DependsOn: "root"}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	r := createRunner(t, s, "scheduler", false)
	root, err := s.LeaseJob(ctx, r.ID)
	if err != nil || root == nil || root.JobName != "root" {
		t.Fatalf("root=%+v err=%v", root, err)
	}
	if early, err := s.LeaseJob(ctx, r.ID); err != nil || early != nil {
		t.Fatalf("dependent leased early: %+v %v", early, err)
	}
	wrong := store.ArtifactOwnership{RunID: run.ID, JobName: "root", RunnerID: uuid.NewString(), LeaseID: root.LeaseID, Generation: root.Generation}
	if err := s.CompleteJob(ctx, wrong, store.JobPassed, nil); err == nil {
		t.Fatal("wrong runner completed job")
	}
	owner := wrong
	owner.RunnerID = r.ID
	staleGeneration := owner
	staleGeneration.Generation++
	if err := s.CompleteJob(ctx, staleGeneration, store.JobPassed, nil); err == nil {
		t.Fatal("wrong generation completed live job")
	}
	if err := s.CompleteJob(ctx, owner, store.JobFailed, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteJob(ctx, owner, store.JobPassed, nil); err == nil {
		t.Fatal("stale completion accepted")
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Status != store.RunFailed {
		t.Fatalf("run status=%s", got.Status)
	}
	statuses := map[string]store.JobStatus{}
	for _, j := range got.Jobs {
		statuses[j.Name] = j.Status
	}
	if statuses["root"] != store.JobFailed || statuses["child"] != store.JobBlocked {
		t.Fatalf("statuses=%v", statuses)
	}
}

func TestExpiredJobIsAbortedAndNeverReassigned(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	run := createRemoteRun(t, s, nil)
	r := createRunner(t, s, "lost", false)
	lease, err := s.LeaseJob(ctx, r.ID)
	if err != nil || lease == nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE job_runs SET lease_expires_at=now()-interval '1 second' WHERE run_id=$1 AND job_name=$2`, lease.RunID, lease.JobName); err != nil {
		t.Fatal(err)
	}
	if err = s.ExpireJobLeases(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Status != store.RunAborted || got.Jobs[0].Status != store.JobAborted {
		t.Fatalf("got=%+v", got)
	}
	if next, err := s.LeaseJob(ctx, r.ID); err != nil || next != nil {
		t.Fatalf("aborted job reassigned: %+v %v", next, err)
	}
}

func TestErrorBlocksDescendantWhileIndependentBranchContinues(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	yaml := []byte("version: 1\njobs:\n  bad:\n    steps: [{run: true}]\n  child:\n    needs: [bad]\n    steps: [{run: true}]\n  independent:\n    steps: [{run: true}]\n")
	run, err := s.CreateRun(ctx, store.CreateRun{ID: uuid.NewString(), PipelineFile: "forge.yaml", PipelineYAML: yaml, PipelineSHA256: strings.Repeat("c", 64), Workspace: "/workspace", MaxParallel: 1, Jobs: []store.Job{{Name: "bad"}, {Name: "child"}, {Name: "independent"}}, Dependencies: []store.JobDependency{{JobName: "child", DependsOn: "bad"}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	r := createRunner(t, s, "error-runner", false)
	bad, err := s.LeaseJob(ctx, r.ID)
	if err != nil || bad == nil || bad.JobName != "bad" {
		t.Fatalf("lease=%+v err=%v", bad, err)
	}
	owner := store.ArtifactOwnership{RunID: run.ID, JobName: bad.JobName, RunnerID: r.ID, LeaseID: bad.LeaseID, Generation: bad.Generation}
	if err = s.CompleteJob(ctx, owner, store.JobError, nil); err != nil {
		t.Fatal(err)
	}
	mid, _ := s.GetRun(ctx, run.ID)
	if mid.Status != store.RunRunning {
		t.Fatalf("run finalized before independent branch: %s", mid.Status)
	}
	independent, err := s.LeaseJob(ctx, r.ID)
	if err != nil || independent == nil || independent.JobName != "independent" {
		t.Fatalf("independent=%+v err=%v", independent, err)
	}
	owner = store.ArtifactOwnership{RunID: run.ID, JobName: independent.JobName, RunnerID: r.ID, LeaseID: independent.LeaseID, Generation: independent.Generation}
	if err = s.CompleteJob(ctx, owner, store.JobPassed, nil); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Status != store.RunError {
		t.Fatalf("run status=%s", got.Status)
	}
}

func TestCancellationWithLostJobResolvesAborted(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	yaml := []byte("version: 1\njobs:\n  a:\n    steps: [{run: true}]\n  b:\n    steps: [{run: true}]\n")
	run, err := s.CreateRun(ctx, store.CreateRun{ID: uuid.NewString(), PipelineFile: "forge.yaml", PipelineYAML: yaml, PipelineSHA256: strings.Repeat("d", 64), Workspace: "/workspace", MaxParallel: 2, Jobs: []store.Job{{Name: "a"}, {Name: "b"}}, Snapshot: testSnapshot()})
	if err != nil {
		t.Fatal(err)
	}
	r := createRunner(t, s, "cancel-runner", false)
	a, _ := s.LeaseJob(ctx, r.ID)
	b, _ := s.LeaseJob(ctx, r.ID)
	if a == nil || b == nil {
		t.Fatal("jobs not leased")
	}
	if _, err = s.RequestCancel(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	hb := []store.LeaseHeartbeat{{RunID: a.RunID, JobName: a.JobName, LeaseID: a.LeaseID, Generation: a.Generation}, {RunID: b.RunID, JobName: b.JobName, LeaseID: b.LeaseID, Generation: b.Generation}}
	results, err := s.HeartbeatJobLeases(ctx, r.ID, hb, time.Now())
	if err != nil || !results[0].CancelRequested || !results[1].CancelRequested {
		t.Fatalf("results=%+v err=%v", results, err)
	}
	owner := store.ArtifactOwnership{RunID: a.RunID, JobName: a.JobName, RunnerID: r.ID, LeaseID: a.LeaseID, Generation: a.Generation}
	if err = s.CompleteJob(ctx, owner, store.JobCanceled, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE job_runs SET lease_expires_at=now()-interval '1 second' WHERE run_id=$1 AND job_name=$2`, b.RunID, b.JobName); err != nil {
		t.Fatal(err)
	}
	if err = s.ExpireJobLeases(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRun(ctx, run.ID)
	if got.Status != store.RunAborted {
		t.Fatalf("status=%s", got.Status)
	}
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
