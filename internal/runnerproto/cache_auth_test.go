package runnerproto

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/nhatminh06/forgeci/internal/artifact"
	"github.com/nhatminh06/forgeci/internal/cache"
	"github.com/nhatminh06/forgeci/internal/store"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCacheWriteAuthorizationRejectsWrongLeaseTerminalAndUndeclared(t *testing.T) {
	runnerID := "00000000-0000-0000-0000-000000000001"
	runID := "00000000-0000-0000-0000-000000000002"
	leaseID := "00000000-0000-0000-0000-000000000003"
	wrongLease := "00000000-0000-0000-0000-000000000004"
	exp := time.Now().Add(time.Hour)
	run := &store.Run{ID: runID, Status: store.RunRunning, PipelineYAML: []byte("version: 1\njobs:\n  build:\n    cache:\n      save:\n        - key: allowed-v1\n          path: .cache/demo\n    steps:\n      - run: true\n"), Jobs: []store.Job{{Name: "build", Status: store.JobRunning, RunnerID: &runnerID, LeaseID: &leaseID, LeaseGeneration: 2, LeaseExpiresAt: &exp}}}
	cas, err := cache.Open(t.TempDir(), artifact.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandlers(sourceStore{run: run}, "token")
	h.SetCacheStore(cas)
	data := []byte("cache-bytes")
	request := func(path, query string, body io.Reader, method string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path+"?"+query, body)
		req.Header.Set("Authorization", "Bearer token")
		if method == http.MethodPut {
			req.ContentLength = int64(len(data))
		}
		w := httptest.NewRecorder()
		h.AuthMiddleware(http.HandlerFunc(h.HandleLeaseRoute)).ServeHTTP(w, req)
		return w
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	base := "/v1/runner/leases/" + leaseID + "/jobs/build/cache-blobs/" + digest
	q := "runner_id=" + runnerID + "&run_id=" + runID + "&job_name=build&generation=2&key=allowed-v1"
	if w := request(base, q, strings.NewReader(string(data)), http.MethodPut); w.Code != http.StatusNoContent {
		t.Fatalf("valid upload=%d", w.Code)
	}
	if w := request("/v1/runner/leases/"+wrongLease+"/jobs/build/cache-blobs/"+digest, q, strings.NewReader(string(data)), http.MethodPut); w.Code != http.StatusConflict {
		t.Fatalf("wrong lease upload=%d", w.Code)
	}
	run.Jobs[0].Status = store.JobPassed
	if w := request(base, q, strings.NewReader(string(data)), http.MethodPut); w.Code != http.StatusConflict {
		t.Fatalf("terminal upload=%d", w.Code)
	}
	run.Jobs[0].Status = store.JobRunning
	if w := request(base, strings.Replace(q, "allowed-v1", "forbidden-v1", 1), strings.NewReader(string(data)), http.MethodPut); w.Code != http.StatusConflict {
		t.Fatalf("undeclared upload=%d", w.Code)
	}
	commit := func(path, query, key string) *httptest.ResponseRecorder {
		payload, _ := json.Marshal(map[string]any{"runner_id": runnerID, "run_id": runID, "lease_id": leaseID, "generation": 2, "job_name": "build", "key": key, "path": "", "root_name": "demo", "root_kind": "directory", "content_sha256": strings.Repeat("b", 64), "blob_sha256": digest, "format": cache.Format, "archive_size_bytes": int64(len(data)), "logical_size_bytes": 1, "entry_count": 1})
		return request(path, query, strings.NewReader(string(payload)), http.MethodPost)
	}
	commitPath := "/v1/runner/leases/" + leaseID + "/jobs/build/cache/commit"
	commitQuery := "runner_id=" + runnerID + "&run_id=" + runID + "&job_name=build&generation=2"
	if w := commit("/v1/runner/leases/"+wrongLease+"/jobs/build/cache/commit", commitQuery, "allowed-v1"); w.Code != http.StatusBadRequest {
		t.Fatalf("wrong lease commit=%d", w.Code)
	}
	run.Jobs[0].Status = store.JobPassed
	if w := commit(commitPath, commitQuery, "allowed-v1"); w.Code != http.StatusConflict {
		t.Fatalf("terminal commit=%d", w.Code)
	}
	run.Jobs[0].Status = store.JobRunning
	if w := commit(commitPath, commitQuery, "forbidden-v1"); w.Code != http.StatusConflict {
		t.Fatalf("undeclared commit=%d", w.Code)
	}
}

func TestCacheAuthorizationRejectsInvalidOwnershipAndDeclarations(t *testing.T) {
	runnerID := "00000000-0000-4000-8000-000000000001"
	runID := "00000000-0000-4000-8000-000000000002"
	leaseID := "00000000-0000-4000-8000-000000000003"
	exp := time.Now().Add(time.Minute)
	yaml := []byte("version: 1\njobs:\n  build:\n    cache:\n      restore:\n        - key: go-v1\n          path: .cache/go\n      save:\n        - key: go-v1\n          path: .cache/go\n    steps:\n      - run: true\n")
	run := &store.Run{ID: runID, Status: store.RunRunning, PipelineYAML: yaml, Jobs: []store.Job{{Name: "build", Status: store.JobRunning, RunnerID: &runnerID, LeaseID: &leaseID, LeaseGeneration: 2, LeaseExpiresAt: &exp}}}
	h := NewHandlers(sourceStore{run: run}, "token")
	request := func(method, path, auth string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		w := httptest.NewRecorder()
		h.AuthMiddleware(http.HandlerFunc(h.HandleLeaseRoute)).ServeHTTP(w, req)
		return w
	}
	base := "/v1/runner/leases/" + leaseID + "/cache/go-v1?runner_id=" + runnerID + "&run_id=" + runID + "&job_name=build&generation=2&path=.cache/go"
	for _, tc := range []struct{ name, auth, path string }{
		{"missing token", "", base}, {"bad token", "Bearer wrong", base}, {"wrong runner", "Bearer token", strings.Replace(base, runnerID, "00000000-0000-4000-8000-000000000099", 1)}, {"wrong run", "Bearer token", strings.Replace(base, runID, "00000000-0000-4000-8000-000000000099", 1)}, {"wrong job", "Bearer token", strings.Replace(base, "job_name=build", "job_name=other", 1)}, {"wrong generation", "Bearer token", strings.Replace(base, "generation=2", "generation=1", 1)}, {"wrong lease", "Bearer token", strings.Replace(base, leaseID, "00000000-0000-4000-8000-000000000099", 1)}, {"undeclared key", "Bearer token", strings.Replace(base, "go-v1", "other", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := request(http.MethodGet, tc.path, tc.auth)
			if w.Code == http.StatusOK {
				t.Fatalf("rejected request returned data")
			}
			if w.Code != http.StatusUnauthorized && w.Code != http.StatusConflict && w.Code != http.StatusNotFound && w.Code != http.StatusServiceUnavailable {
				t.Fatalf("unexpected status %d", w.Code)
			}
		})
	}
	past := time.Now().Add(-time.Minute)
	run.Jobs[0].LeaseExpiresAt = &past
	if w := request(http.MethodGet, base, "Bearer token"); w.Code == http.StatusOK {
		t.Fatal("expired lease accepted")
	}
	run.Jobs[0].LeaseExpiresAt = &exp
	run.Jobs[0].Status = store.JobPassed
	if w := request(http.MethodGet, base, "Bearer token"); w.Code == http.StatusOK {
		t.Fatal("terminal job restore accepted")
	}
}
