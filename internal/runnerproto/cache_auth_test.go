package runnerproto

import (
	"github.com/nhatminh06/forgeci/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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
		{"missing token", "", base}, {"bad token", "Bearer wrong", base}, {"wrong runner", "Bearer token", strings.Replace(base, runnerID, "00000000-0000-4000-8000-000000000099", 1)}, {"wrong run", "Bearer token", strings.Replace(base, runID, "00000000-0000-4000-8000-000000000099", 1)}, {"wrong job", "Bearer token", strings.Replace(base, "job_name=build", "job_name=other", 1)}, {"wrong generation", "Bearer token", strings.Replace(base, "generation=2", "generation=1", 1)}, {"undeclared key", "Bearer token", strings.Replace(base, "go-v1", "other", 1)},
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
}
