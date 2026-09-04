package runnerproto

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

type logStore struct {
	sourceStore
	chunks []store.JobLogChunk
}

func (s *logStore) AppendJobLog(_ context.Context, c store.JobLogChunk) error {
	s.chunks = append(s.chunks, c)
	return nil
}
func (*logStore) ListJobLogs(context.Context, string, string, int64, int) ([]store.JobLogChunk, error) {
	return nil, nil
}

func TestJobLogAppendRequiresActiveLease(t *testing.T) {
	runnerID, runID, leaseID := "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"
	exp := time.Now().Add(time.Minute)
	run := &store.Run{ID: runID, Status: store.RunRunning, Jobs: []store.Job{{Name: "build", Status: store.JobRunning, RunnerID: &runnerID, LeaseID: &leaseID, LeaseGeneration: 2, LeaseExpiresAt: &exp}}}
	s := &logStore{sourceStore: sourceStore{run: run}}
	h := NewHandlers(s, "token")
	send := func(body map[string]any, query string) *httptest.ResponseRecorder {
		data, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/v1/runner/leases/"+leaseID+"/jobs/build/logs?"+query, bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer token")
		w := httptest.NewRecorder()
		h.AuthMiddleware(http.HandlerFunc(h.HandleLeaseRoute)).ServeHTTP(w, req)
		return w
	}
	body := map[string]any{"runner_id": runnerID, "run_id": runID, "lease_id": leaseID, "generation": 2, "job_name": "build", "sequence": 1, "stream": "stdout", "payload": []byte("alpha")}
	q := "runner_id=" + runnerID + "&run_id=" + runID
	if w := send(body, q); w.Code != http.StatusNoContent {
		t.Fatalf("valid=%d %s", w.Code, w.Body)
	}
	if len(s.chunks) != 1 {
		t.Fatal("chunk missing")
	}
	body["runner_id"] = "00000000-0000-4000-8000-000000000099"
	if w := send(body, q); w.Code != http.StatusBadRequest || len(s.chunks) != 1 {
		t.Fatalf("wrong identity=%d chunks=%d", w.Code, len(s.chunks))
	}
	body["runner_id"] = runnerID
	body["run_id"] = "00000000-0000-4000-8000-000000000099"
	if w := send(body, q); w.Code != http.StatusBadRequest || len(s.chunks) != 1 {
		t.Fatalf("wrong run=%d chunks=%d", w.Code, len(s.chunks))
	}
	body["run_id"] = runID
	body["sequence"] = 0
	if w := send(body, q); w.Code != http.StatusBadRequest || len(s.chunks) != 1 {
		t.Fatalf("invalid sequence=%d", w.Code)
	}
}
