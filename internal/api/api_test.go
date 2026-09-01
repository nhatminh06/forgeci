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
	runners []store.Runner
	pingErr error
}

func (s *fakeStore) Ping(ctx context.Context) error  { return s.pingErr }
func (s *fakeStore) ListRunners(context.Context) ([]store.Runner, error) {
	return s.runners, nil
}

func (m *fakeManager) Ping(context.Context) error { return m.pingErr }
func (m *fakeManager) Submit(_ context.Context, file string, jobs int) (*store.Run, error) {
	m.submitFile = file
	m.submitJobs = jobs
	if m.submitErr != nil {
		return nil, m.submitErr
	}
	return &store.Run{ID: "00000000-0000-4000-8000-000000000001", Status: store.RunQueued}, nil
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
