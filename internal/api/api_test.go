package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/scm"
	"github.com/nhatminh06/forgeci/internal/store"
)

type fakeManager struct {
	submitFile string
	submitJobs int
	submitErr  error
	cancelErr  error
	pingErr    error
}
type terminalManager struct{ *fakeManager }

func (m terminalManager) Get(context.Context, string) (*store.Run, error) {
	return &store.Run{ID: "00000000-0000-4000-8000-000000000001", Status: store.RunPassed, FinishedAt: ptr(time.Now().UTC())}, nil
}
func ptr(t time.Time) *time.Time { return &t }

type fakeStore struct {
	runners     []store.Runner
	pingErr     error
	artifacts   []store.Artifact
	artifact    *store.Artifact
	logs        []store.JobLogChunk
	logErr      error
	logsOnce    bool
	logCalls    int
	repos       []scm.Repository
	repoErr     error
	deleteErr   error
	repository  *scm.Repository
	deliveries  []scm.Delivery
	deliveryErr error
}

func (s *fakeStore) CreateSCMRepository(_ context.Context, item scm.Repository) (*scm.Repository, error) {
	if s.repoErr != nil {
		return nil, s.repoErr
	}
	item.ID = "00000000-0000-4000-8000-000000000001"
	item.CreatedAt, item.UpdatedAt = time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC()
	return &item, nil
}
func (s *fakeStore) GetSCMRepository(context.Context, string) (*scm.Repository, error) {
	return nil, store.ErrNotFound
}
func (s *fakeStore) GetSCMRepositoryByIdentity(_ context.Context, provider scm.Provider, name string) (*scm.Repository, error) {
	if s.repository == nil || s.repository.Provider != provider || s.repository.FullName != name {
		return nil, store.ErrNotFound
	}
	return s.repository, nil
}
func (s *fakeStore) ListSCMRepositories(context.Context) ([]scm.Repository, error) {
	return s.repos, s.repoErr
}
func (s *fakeStore) DeleteSCMRepository(context.Context, string) error { return s.deleteErr }
func (s *fakeStore) CreateSCMDelivery(_ context.Context, item scm.Delivery) (*scm.Delivery, error) {
	if s.deliveryErr != nil {
		return nil, s.deliveryErr
	}
	for _, existing := range s.deliveries {
		if existing.Provider == item.Provider && existing.DeliveryID == item.DeliveryID {
			if existing.PayloadSHA256 != item.PayloadSHA256 {
				return nil, store.ErrConflict
			}
			return &existing, nil
		}
	}
	item.ID = "00000000-0000-4000-8000-000000000002"
	s.deliveries = append(s.deliveries, item)
	return &item, nil
}
func (s *fakeStore) GetSCMDelivery(context.Context, string) (*scm.Delivery, error) {
	return nil, store.ErrNotFound
}
func (s *fakeStore) GetSCMDeliveryByProviderDeliveryID(context.Context, scm.Provider, string) (*scm.Delivery, error) {
	return nil, store.ErrNotFound
}
func (s *fakeStore) CreateSCMRunTrigger(context.Context, scm.RunTrigger) (*scm.RunTrigger, error) {
	return nil, store.ErrNotFound
}
func (s *fakeStore) GetSCMRunTrigger(context.Context, string) (*scm.RunTrigger, error) {
	return nil, store.ErrNotFound
}
func (s *fakeStore) GetSCMRunTriggerByDelivery(context.Context, string) (*scm.RunTrigger, error) {
	return nil, store.ErrNotFound
}
func (s *fakeStore) GetSCMRunTriggerByRunID(context.Context, string) (*scm.RunTrigger, error) {
	return nil, store.ErrNotFound
}

func (s *fakeStore) AppendJobLog(context.Context, store.JobLogChunk) error    { return nil }
func (s *fakeStore) AppendJobLogs(context.Context, []store.JobLogChunk) error { return nil }
func (s *fakeStore) ListJobLogs(context.Context, string, string, int64, int) ([]store.JobLogChunk, error) {
	s.logCalls++
	if s.logsOnce && s.logCalls > 1 {
		return nil, s.logErr
	}
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

func TestFollowReturnsFinalUnreadChunksBeforeTerminalExit(t *testing.T) {
	id := "00000000-0000-4000-8000-000000000001"
	fs := &fakeStore{logsOnce: true, logs: []store.JobLogChunk{{RunID: id, JobName: "build", Sequence: 3, Stream: store.JobLogStdout, Payload: []byte("final")}}}
	h := (Server{Manager: terminalManager{&fakeManager{}}, Store: fs}).Handler()
	resp := request(t, h, http.MethodGet, "/v1/runs/"+id+"/logs?job=build&after=2&follow=true", "")
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), "ZmluYWw=") {
		t.Fatalf("code=%d body=%s", resp.Code, resp.Body.String())
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

func githubSignature(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(body))
	return "sha256=" + fmt.Sprintf("%x", mac.Sum(nil))
}

func githubRequest(t *testing.T, handler http.Handler, secret, event, delivery, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/hooks/github", strings.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", githubSignature(secret, body))
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-GitHub-Delivery", delivery)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}

func TestGitHubWebhook(t *testing.T) {
	const secret = "webhook-secret"
	registered := &scm.Repository{ID: "00000000-0000-4000-8000-000000000001", Provider: scm.GitHub, FullName: "owner/repo", Enabled: true}
	backend := &fakeStore{repository: registered}
	handler := (Server{Manager: &fakeManager{}, Store: backend, GitHubWebhookSecret: []byte(secret)}).Handler()
	push := `{"after":"0123456789abcdef0123456789abcdef01234567","ref":"refs/heads/main","repository":{"full_name":"Owner/Repo","clone_url":"http://169.254.169.254/latest","ssh_url":"ssh://bad"},"installation":{"id":7}}`
	if got := githubRequest(t, handler, secret, "push", "delivery-1", push).Code; got != http.StatusAccepted {
		t.Fatalf("push status=%d", got)
	}
	if len(backend.deliveries) != 1 || backend.deliveries[0].RepositoryID != registered.ID || backend.deliveries[0].PayloadSHA256 == "" || backend.deliveries[0].Status != scm.DeliveryPending || backend.deliveries[0].CommitSHA != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("delivery=%+v", backend.deliveries)
	}
	if got := githubRequest(t, handler, secret, "push", "delivery-1", push).Code; got != http.StatusAccepted {
		t.Fatalf("replay status=%d", got)
	}
	if got := githubRequest(t, handler, secret, "push", "delivery-1", strings.Replace(push, "main", "next", 1)).Code; got != http.StatusConflict {
		t.Fatalf("conflict status=%d", got)
	}
	for _, tc := range []struct{ event, body string }{
		{"ping", `{}`},
		{"issues", `{"repository":{"full_name":"owner/repo"}}`},
		{"push", `{"after":"0123456789abcdef0123456789abcdef01234567","ref":"refs/tags/v1","repository":{"full_name":"owner/repo"}}`},
		{"push", `{"after":"0000000000000000000000000000000000000000","ref":"refs/heads/main","repository":{"full_name":"owner/repo"}}`},
	} {
		if got := githubRequest(t, handler, secret, tc.event, "ignored-"+tc.event, tc.body).Code; got != http.StatusAccepted {
			t.Fatalf("%s status=%d", tc.event, got)
		}
	}
	pr := `{"action":"opened","repository":{"full_name":"owner/repo"},"pull_request":{"number":4,"head":{"sha":"0123456789abcdef0123456789abcdef01234567","ref":"feature"},"base":{"ref":"main"}}}`
	for _, action := range []string{"opened", "reopened", "synchronize", "ready_for_review"} {
		if got := githubRequest(t, handler, secret, "pull_request", "pr-"+action, strings.Replace(pr, "opened", action, 1)).Code; got != http.StatusAccepted {
			t.Fatalf("action=%s status=%d", action, got)
		}
	}
	draft := strings.Replace(pr, `"number":4`, `"number":4,"draft":true`, 1)
	if got := githubRequest(t, handler, secret, "pull_request", "draft", draft).Code; got != http.StatusAccepted {
		t.Fatalf("draft status=%d", got)
	}
	if got := githubRequest(t, handler, secret, "pull_request", "closed", strings.Replace(pr, "opened", "closed", 1)).Code; got != http.StatusAccepted {
		t.Fatalf("closed status=%d", got)
	}
	if len(backend.deliveries) != 7 || backend.deliveries[len(backend.deliveries)-1].Status != scm.DeliveryIgnored {
		t.Fatalf("deliveries=%+v", backend.deliveries)
	}
}

func TestGitHubWebhookValidationAndRegistrationGate(t *testing.T) {
	const secret = "webhook-secret"
	body := `{"after":"0123456789abcdef0123456789abcdef01234567","ref":"refs/heads/main","repository":{"full_name":"owner/repo"}}`
	for _, enabled := range []bool{false} {
		backend := &fakeStore{repository: &scm.Repository{ID: "00000000-0000-4000-8000-000000000001", Provider: scm.GitHub, FullName: "owner/repo", Enabled: enabled}}
		handler := (Server{Manager: &fakeManager{}, Store: backend, GitHubWebhookSecret: []byte(secret)}).Handler()
		if got := githubRequest(t, handler, secret, "push", "delivery", body).Code; got != http.StatusAccepted || len(backend.deliveries) != 0 {
			t.Fatalf("enabled=%t status=%d deliveries=%d", enabled, got, len(backend.deliveries))
		}
	}
	unregistered := &fakeStore{}
	if got := githubRequest(t, (Server{Manager: &fakeManager{}, Store: unregistered, GitHubWebhookSecret: []byte(secret)}).Handler(), secret, "push", "delivery", body).Code; got != http.StatusAccepted || len(unregistered.deliveries) != 0 {
		t.Fatalf("unregistered status=%d deliveries=%d", got, len(unregistered.deliveries))
	}
	backend := &fakeStore{repository: &scm.Repository{ID: "00000000-0000-4000-8000-000000000001", Provider: scm.GitHub, FullName: "owner/repo", Enabled: true}}
	handler := (Server{Manager: &fakeManager{}, Store: backend, GitHubWebhookSecret: []byte(secret)}).Handler()
	for _, signature := range []string{"", "sha1=abcd", "sha256=abcd", "sha256=" + strings.Repeat("z", 64)} {
		req := httptest.NewRequest(http.MethodPost, "/v1/hooks/github", strings.NewReader(body))
		req.Header.Set("X-Hub-Signature-256", signature)
		req.Header.Set("X-GitHub-Event", "push")
		req.Header.Set("X-GitHub-Delivery", "delivery")
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("signature=%q status=%d", signature, resp.Code)
		}
	}
	for _, header := range []string{"X-GitHub-Event", "X-GitHub-Delivery"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/hooks/github", strings.NewReader(body))
		req.Header.Set("X-Hub-Signature-256", githubSignature(secret, body))
		req.Header.Set("X-GitHub-Event", "push")
		req.Header.Set("X-GitHub-Delivery", "delivery")
		req.Header.Del(header)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("header=%s status=%d", header, resp.Code)
		}
	}
	oversized := strings.Repeat("x", githubWebhookMaxBody+1)
	if got := githubRequest(t, handler, secret, "push", "oversized", oversized).Code; got != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d", got)
	}
	if got := githubRequest(t, handler, secret, "push", "malformed", `{`).Code; got != http.StatusBadRequest {
		t.Fatalf("malformed status=%d", got)
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

func TestRepositoryRoutes(t *testing.T) {
	persistence := &fakeStore{}
	handler := (Server{Manager: &fakeManager{}, Store: persistence}).Handler()
	create := request(t, handler, http.MethodPost, "/v1/repos", `{"provider":"github","full_name":"Foo/Bar"}`)
	if create.Code != http.StatusCreated || !strings.Contains(create.Body.String(), `"pipeline":"forge.yaml"`) {
		t.Fatalf("create=%d %s", create.Code, create.Body.String())
	}
	for _, body := range []string{`{"provider":"gitlab","full_name":"x/y"}`, `{"provider":"github","full_name":"x/y","extra":true}`, `{`, `{"provider":"github","full_name":"x/y"}{}`} {
		if got := request(t, handler, http.MethodPost, "/v1/repos", body).Code; got != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d", body, got)
		}
	}
	oversized := `{"provider":"github","full_name":"x/y","pipeline":"` + strings.Repeat("a", repositoryMaxBody) + `"}`
	if got := request(t, handler, http.MethodPost, "/v1/repos", oversized).Code; got != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized=%d", got)
	}
	persistence.repoErr = store.ErrConflict
	if got := request(t, handler, http.MethodPost, "/v1/repos", `{"provider":"github","full_name":"x/y"}`).Code; got != http.StatusConflict {
		t.Fatalf("conflict=%d", got)
	}
	persistence.repoErr = nil
	persistence.repos = []scm.Repository{{ID: "1", Provider: scm.GitHub, FullName: "x/y", PipelinePath: "forge.yaml", Enabled: true}}
	if got := request(t, handler, http.MethodGet, "/v1/repos", "").Code; got != http.StatusOK {
		t.Fatalf("list=%d", got)
	}
	if got := request(t, handler, http.MethodDelete, "/v1/repos/not-a-uuid", "").Code; got != http.StatusNotFound {
		t.Fatalf("invalid delete=%d", got)
	}
	persistence.deleteErr = store.ErrNotFound
	if got := request(t, handler, http.MethodDelete, "/v1/repos/00000000-0000-4000-8000-000000000001", "").Code; got != http.StatusNotFound {
		t.Fatalf("missing delete=%d", got)
	}
}
