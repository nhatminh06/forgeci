package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

type fakeManager struct {
	submitFile string
	submitJobs int
	submitErr  error
	cancelErr  error
	pingErr    error
}

type fakeStore struct {
	runners   []store.Runner
	pingErr   error
	artifacts []store.Artifact
	artifact  *store.Artifact
	logs      []store.JobLogChunk
	logErr    error
}

func (s *fakeStore) AppendJobLog(context.Context, store.JobLogChunk) error { return nil }
func (s *fakeStore) ListJobLogs(context.Context, string, string, int64, int) ([]store.JobLogChunk, error) {
	return s.logs, s.logErr
}

func TestLogsRejectsMalformedRunID(t *testing.T) {
	handler := (Server{Manager: &fakeManager{}, Store: &fakeStore{}}).Handler()
	if got := request(t, handler, http.MethodGet, "/v1/runs/not-a-uuid/logs?job=build", "").Code; got != http.StatusNotFound {
		t.Fatalf("status=%d", got)
	}
}

func TestLogsValidationAndBinaryPayload(t *testing.T) {
	id := "00000000-0000-4000-8000-000000000001"
	fs := &fakeStore{logs: []store.JobLogChunk{{RunID: id, JobName: "build", Sequence: 1, Stream: store.JobLogStdout, Payload: []byte{0, 0xff, 0xfe, 'A', '\n'}}, {RunID: id, JobName: "build", Sequence: 2, Stream: store.JobLogStderr, Payload: []byte("warn")}}}
	h := (Server{Manager: &fakeManager{}, Store: fs}).Handler()
	resp := request(t, h, http.MethodGet, "/v1/runs/"+id+"/logs?job=build&after=0&limit=2", "")
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), "AP/+QQo=") {
		t.Fatalf("code=%d body=%s", resp.Code, resp.Body)
	}
	for _, q := range []string{"job=build&limit=0", "job=build&limit=-1", "job=build&limit=1001", "job=build&limit=x", "job=build&after=-1", "job=build&after=x", "limit=2"} {
		if got := request(t, h, http.MethodGet, "/v1/runs/"+id+"/logs?"+q, "").Code; got != http.StatusBadRequest {
			t.Fatalf("query %s status=%d", q, got)
		}
	}
	fs.logErr = errors.New("db down")
	if got := request(t, h, http.MethodGet, "/v1/runs/"+id+"/logs?job=build", "").Code; got != http.StatusServiceUnavailable {
		t.Fatalf("error status=%d", got)
	}
}

func (s *fakeStore) Ping(ctx context.Context) error { return s.pingErr }
func (s *fakeStore) ListRunners(context.Context) ([]store.Runner, error) {
	return s.runners, nil
}
func (s *fakeStore) CommitArtifacts(context.Context, store.ArtifactOwnership, []store.Artifact) error {
	return nil
}
func (s *fakeStore) GetArtifactForLease(context.Context, store.ArtifactOwnership, string, string) (*store.Artifact, error) {
	return s.artifact, nil
}
func (s *fakeStore) ListArtifacts(context.Context, string) ([]store.Artifact, error) {
	return s.artifacts, nil
}
func (s *fakeStore) GetArtifact(context.Context, string, string, string) (*store.Artifact, error) {
	if s.artifact == nil {
		return nil, store.ErrNotFound
	}
	return s.artifact, nil
}
func (s *fakeStore) SetArtifactExpiry(context.Context, string, time.Time) error     { return nil }
func (s *fakeStore) ExpireArtifacts(context.Context, time.Time) ([]string, error)   { return nil, nil }
func (s *fakeStore) LiveArtifactBlobs(context.Context) (map[string]struct{}, error) { return nil, nil }

func (m *fakeManager) Ping(context.Context) error { return m.pingErr }
func (m *fakeManager) Submit(_ context.Context, file string, jobs int) (*store.Run, error) {
	m.submitFile = file
	m.submitJobs = jobs
	if m.submitErr != nil {
		return nil, m.submitErr
	}
	return &store.Run{ID: "00000000-0000-4000-8000-000000000001", Status: store.RunQueued}, nil
}

func TestArtifactListAndExpiredDownload(t *testing.T) {
	item := store.Artifact{ProducerJob: "build", Name: "app", Available: false}
	persistence := &fakeStore{artifacts: []store.Artifact{item}, artifact: &item}
	handler := (Server{Manager: &fakeManager{}, Store: persistence}).Handler()
	listed := request(t, handler, http.MethodGet, "/v1/runs/00000000-0000-4000-8000-000000000001/artifacts", "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"name":"app"`) {
		t.Fatalf("list code=%d body=%s", listed.Code, listed.Body.String())
	}
	expired := request(t, handler, http.MethodGet, "/v1/runs/00000000-0000-4000-8000-000000000001/artifacts/build/app", "")
	if expired.Code != http.StatusGone || expired.Body.String() != "{\"error\":\"artifact expired\"}\n" {
		t.Fatalf("expired code=%d body=%s", expired.Code, expired.Body.String())
	}
}
func (*fakeManager) List(context.Context, int) ([]store.Run, error) {
	return []store.Run{{ID: "id", Status: store.RunPassed, CreatedAt: time.Unix(1, 0)}}, nil
}
func (*fakeManager) Get(context.Context, string) (*store.Run, error) {
	return &store.Run{ID: "00000000-0000-4000-8000-000000000001", Status: store.RunRunning, Jobs: []store.Job{{Name: "a", Status: store.JobRunning}, {Name: "b", Status: store.JobPending}}}, nil
}
func (m *fakeManager) Cancel(context.Context, string) (store.RunStatus, error) {
	return store.RunRunning, m.cancelErr
}

func request(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestCreateRunStrictJSONAndDefaults(t *testing.T) {
	manager := &fakeManager{}
	handler := (Server{Manager: manager}).Handler()
	response := request(t, handler, http.MethodPost, "/v1/runs", `{"pipeline_file":"forge.yaml","max_parallel":3}`)
	if response.Code != http.StatusAccepted || manager.submitFile != "forge.yaml" || manager.submitJobs != 3 {
		t.Fatalf("status=%d manager=%+v body=%s", response.Code, manager, response.Body.String())
	}
	for _, body := range []string{`{"pipeline_file":"forge.yaml","typo":true}`, `{"pipeline_file":"forge.yaml"}{}`, `{`} {
		response = request(t, handler, http.MethodPost, "/v1/runs", body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body=%q status=%d", body, response.Code)
		}
	}
}

func TestListLimitValidation(t *testing.T) {
	handler := (Server{Manager: &fakeManager{}}).Handler()
	for _, path := range []string{"/v1/runs?limit=0", "/v1/runs?limit=101", "/v1/runs?limit=nope", "/v1/runs?unknown=1"} {
		if got := request(t, handler, http.MethodGet, path, "").Code; got != http.StatusBadRequest {
			t.Fatalf("%s status=%d", path, got)
		}
	}
	response := request(t, handler, http.MethodGet, "/v1/runs?limit=10", "")
	if response.Code != http.StatusOK {
		t.Fatal(response.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
}

func TestRunInspectionAndCancelConflict(t *testing.T) {
	manager := &fakeManager{cancelErr: store.ErrConflict}
	handler := (Server{Manager: manager}).Handler()
	response := request(t, handler, http.MethodGet, "/v1/runs/00000000-0000-4000-8000-000000000001", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"RUNNING"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	response = request(t, handler, http.MethodPost, "/v1/runs/00000000-0000-4000-8000-000000000001/cancel", "")
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestHealthReflectsDatabase(t *testing.T) {
	manager := &fakeManager{}
	handler := (Server{Manager: manager}).Handler()
	if got := request(t, handler, http.MethodGet, "/healthz", "").Code; got != http.StatusOK {
		t.Fatal(got)
	}
	manager.pingErr = errors.New("database down")
	if got := request(t, handler, http.MethodGet, "/healthz", "").Code; got != http.StatusServiceUnavailable {
		t.Fatal(got)
	}
}

func TestRunnersList(t *testing.T) {
	store := &fakeStore{
		runners: []store.Runner{
			{ID: "001", Name: "runner-1", Status: "ONLINE", OS: "linux", Arch: "amd64", DockerAvailable: true, MaxParallel: 2},
			{ID: "002", Name: "runner-2", Status: "OFFLINE", OS: "linux", Arch: "arm64", DockerAvailable: false, MaxParallel: 1},
		},
	}
	handler := (Server{Manager: &fakeManager{}, Store: store}).Handler()
	response := request(t, handler, http.MethodGet, "/v1/runners", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	runners, ok := payload["runners"].([]any)
	if !ok || len(runners) != 2 {
		t.Fatalf("runners=%+v", payload)
	}
}
