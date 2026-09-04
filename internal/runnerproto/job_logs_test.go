package runnerproto

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

type logStore struct {
	sourceStore
	chunks []store.JobLogChunk
}

func (s *logStore) AppendJobLog(_ context.Context, c store.JobLogChunk) error {
	return s.AppendJobLogs(context.Background(), []store.JobLogChunk{c})
}

func (s *logStore) AppendJobLogs(_ context.Context, chunks []store.JobLogChunk) error {
	max := int64(0)
	if len(s.chunks) > 0 {
		max = s.chunks[len(s.chunks)-1].Sequence
	}
	newChunks := make([]store.JobLogChunk, 0, len(chunks))
	for _, chunk := range chunks {
		for _, existing := range s.chunks {
			if existing.Sequence == chunk.Sequence {
				if existing.Stream == chunk.Stream && bytes.Equal(existing.Payload, chunk.Payload) {
					goto next
				}
				return store.ErrConflict
			}
		}
		if chunk.Sequence != max+1 {
			return store.ErrConflict
		}
		max++
		newChunks = append(newChunks, chunk)
	next:
	}
	s.chunks = append(s.chunks, newChunks...)
	return nil
}

func (*logStore) ListJobLogs(context.Context, string, string, int64, int) ([]store.JobLogChunk, error) {
	return nil, nil
}

type logFixture struct {
	runnerID, runID, leaseID string
	h                        *Handlers
	s                        *logStore
}

func newLogFixture(t *testing.T) logFixture {
	t.Helper()
	runnerID, runID, leaseID := "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"
	exp := time.Now().Add(time.Minute)
	run := &store.Run{ID: runID, Status: store.RunRunning, Jobs: []store.Job{{Name: "build", Status: store.JobRunning, RunnerID: &runnerID, LeaseID: &leaseID, LeaseGeneration: 2, LeaseExpiresAt: &exp}}}
	s := &logStore{sourceStore: sourceStore{run: run}}
	return logFixture{runnerID, runID, leaseID, NewHandlers(s, "token"), s}
}

func (f logFixture) body(chunks []map[string]any) map[string]any {
	return map[string]any{"runner_id": f.runnerID, "run_id": f.runID, "lease_id": f.leaseID, "generation": 2, "job_name": "build", "chunks": chunks}
}
func (f logFixture) query() string { return "runner_id=" + f.runnerID + "&run_id=" + f.runID }
func (f logFixture) send(body any, query string) *httptest.ResponseRecorder {
	return f.sendToLease(body, query, f.leaseID)
}
func (f logFixture) sendToLease(body any, query, leaseID string) *httptest.ResponseRecorder {
	var data []byte
	if raw, ok := body.([]byte); ok {
		data = raw
	} else {
		data, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/runner/leases/"+leaseID+"/jobs/build/logs?"+query, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer token")
	w := httptest.NewRecorder()
	f.h.AuthMiddleware(http.HandlerFunc(f.h.HandleLeaseRoute)).ServeHTTP(w, req)
	return w
}
func chunk(sequence int64, stream, payload string) map[string]any {
	return map[string]any{"sequence": sequence, "stream": stream, "payload": []byte(payload)}
}

func TestJobLogAppendBatch(t *testing.T) {
	f := newLogFixture(t)
	if w := f.send(f.body([]map[string]any{chunk(1, "stdout", "alpha")}), f.query()); w.Code != http.StatusNoContent {
		t.Fatalf("single=%d %s", w.Code, w.Body.String())
	}
	if w := f.send(f.body([]map[string]any{chunk(2, "stderr", "warning"), chunk(3, "stdout", "omega")}), f.query()); w.Code != http.StatusNoContent {
		t.Fatalf("multi=%d %s", w.Code, w.Body.String())
	}
	if len(f.s.chunks) != 3 || f.s.chunks[1].Stream != store.JobLogStderr || string(f.s.chunks[2].Payload) != "omega" {
		t.Fatalf("chunks=%+v", f.s.chunks)
	}
	if w := f.send(f.body([]map[string]any{chunk(1, "stdout", "alpha"), chunk(2, "stderr", "warning"), chunk(3, "stdout", "omega")}), f.query()); w.Code != http.StatusNoContent {
		t.Fatalf("replay=%d %s", w.Code, w.Body.String())
	}
	if w := f.send(f.body([]map[string]any{chunk(1, "stdout", "different")}), f.query()); w.Code != http.StatusConflict {
		t.Fatalf("conflict=%d %s", w.Code, w.Body.String())
	}
}

func TestJobLogAppendRejectsInvalidBatchWithoutPersistence(t *testing.T) {
	cases := []struct {
		name string
		body func(logFixture) any
	}{
		{"empty", func(f logFixture) any { return f.body(nil) }},
		{"too many", func(f logFixture) any {
			chunks := make([]map[string]any, MaxLogChunksPerRequest+1)
			for i := range chunks {
				chunks[i] = chunk(int64(i+1), "stdout", "x")
			}
			return f.body(chunks)
		}},
		{"large chunk", func(f logFixture) any {
			return f.body([]map[string]any{{"sequence": 1, "stream": "stdout", "payload": bytes.Repeat([]byte("x"), MaxLogChunkBytes+1)}})
		}},
		{"duplicate", func(f logFixture) any {
			return f.body([]map[string]any{chunk(1, "stdout", "a"), chunk(1, "stderr", "b")})
		}},
		{"non monotonic", func(f logFixture) any {
			return f.body([]map[string]any{chunk(2, "stdout", "a"), chunk(1, "stderr", "b")})
		}},
		{"invalid second", func(f logFixture) any { return f.body([]map[string]any{chunk(1, "stdout", "a"), chunk(2, "bad", "b")}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newLogFixture(t)
			if w := f.send(tc.body(f), f.query()); w.Code != http.StatusBadRequest || len(f.s.chunks) != 0 {
				t.Fatalf("status=%d chunks=%d body=%s", w.Code, len(f.s.chunks), w.Body.String())
			}
		})
	}
}

func TestJobLogAppendStrictDecodingAndOwnership(t *testing.T) {
	t.Run("strict JSON", func(t *testing.T) {
		for _, raw := range [][]byte{[]byte("{"), []byte(`{"runner_id":"x","unknown":true}`), []byte(`{} {}`), bytes.Repeat([]byte("x"), MaxLogAppendBodyBytes+1)} {
			f := newLogFixture(t)
			if w := f.send(raw, f.query()); (w.Code != http.StatusBadRequest && w.Code != http.StatusRequestEntityTooLarge) || len(f.s.chunks) != 0 {
				t.Fatalf("status=%d chunks=%d", w.Code, len(f.s.chunks))
			}
		}
	})
	for _, field := range []string{"runner_id", "run_id", "lease_id", "job_name", "generation"} {
		t.Run("wrong "+field, func(t *testing.T) {
			f := newLogFixture(t)
			body := f.body([]map[string]any{chunk(1, "stdout", "alpha")})
			switch field {
			case "generation":
				body[field] = 3
			case "job_name":
				body[field] = "other"
			default:
				body[field] = "00000000-0000-4000-8000-000000000099"
			}
			if w := f.send(body, f.query()); (w.Code != http.StatusBadRequest && w.Code != http.StatusConflict) || len(f.s.chunks) != 0 {
				t.Fatalf("status=%d chunks=%d", w.Code, len(f.s.chunks))
			}
		})
	}
	t.Run("wrong query", func(t *testing.T) {
		f := newLogFixture(t)
		if w := f.send(f.body([]map[string]any{chunk(1, "stdout", "alpha")}), "runner_id="+f.runnerID+"&run_id=00000000-0000-4000-8000-000000000099"); w.Code != http.StatusBadRequest || len(f.s.chunks) != 0 {
			t.Fatalf("status=%d chunks=%d", w.Code, len(f.s.chunks))
		}
	})
	t.Run("expired lease", func(t *testing.T) {
		f := newLogFixture(t)
		expired := time.Now().Add(-time.Minute)
		f.s.run.Jobs[0].LeaseExpiresAt = &expired
		if w := f.send(f.body([]map[string]any{chunk(1, "stdout", "alpha")}), f.query()); w.Code != http.StatusConflict || len(f.s.chunks) != 0 {
			t.Fatalf("status=%d chunks=%d", w.Code, len(f.s.chunks))
		}
	})
	t.Run("wrong live lease", func(t *testing.T) {
		f := newLogFixture(t)
		wrongLease := "00000000-0000-4000-8000-000000000099"
		body := f.body([]map[string]any{chunk(1, "stdout", "alpha")})
		body["lease_id"] = wrongLease
		if w := f.sendToLease(body, f.query(), wrongLease); w.Code != http.StatusConflict || len(f.s.chunks) != 0 {
			t.Fatalf("status=%d chunks=%d", w.Code, len(f.s.chunks))
		}
	})
	t.Run("terminal job", func(t *testing.T) {
		f := newLogFixture(t)
		f.s.run.Jobs[0].Status = store.JobPassed
		if w := f.send(f.body([]map[string]any{chunk(1, "stdout", "alpha")}), f.query()); w.Code != http.StatusConflict || len(f.s.chunks) != 0 {
			t.Fatalf("status=%d chunks=%d", w.Code, len(f.s.chunks))
		}
	})
	t.Run("bad token", func(t *testing.T) {
		f := newLogFixture(t)
		req := httptest.NewRequest(http.MethodPost, "/v1/runner/leases/"+f.leaseID+"/jobs/build/logs?"+f.query(), strings.NewReader("{}"))
		w := httptest.NewRecorder()
		f.h.AuthMiddleware(http.HandlerFunc(f.h.HandleLeaseRoute)).ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized || len(f.s.chunks) != 0 {
			t.Fatalf("status=%d chunks=%d", w.Code, len(f.s.chunks))
		}
	})
}
