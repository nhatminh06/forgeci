package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReconcileCheckAdoptsLostCreateResponseAndUpdatesIdempotently(t *testing.T) {
	var mu sync.Mutex
	created := false
	posts, patches, tokenCalls := 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			t.Error("missing authorization")
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/access_tokens"):
			tokenCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "FORGECI_SECRET_TOKEN_TEST", "expires_at": time.Now().Add(time.Hour)})
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/commits/"):
			mu.Lock()
			exists := created
			mu.Unlock()
			if exists {
				_, _ = w.Write([]byte(`{"check_runs":[{"id":77,"external_id":"run-1"}]}`))
			} else {
				_, _ = w.Write([]byte(`{"check_runs":[]}`))
			}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/check-runs"):
			mu.Lock()
			created = true
			posts++
			mu.Unlock()
			http.Error(w, "response lost", http.StatusInternalServerError)
		case r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/check-runs/77"):
			patches++
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["status"] != "completed" || body["conclusion"] != "success" || body["name"] != CheckName {
				t.Errorf("body=%v err=%v", body, err)
			}
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, testApp(t), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	client.now = time.Now
	conclusion := "success"
	in := CheckRequest{Repository: "owner/repo", InstallationID: "7", CommitSHA: strings.Repeat("a", 40), ExternalID: "run-1", Status: "completed", Conclusion: &conclusion}
	if _, err := client.ReconcileCheck(context.Background(), in); err == nil {
		t.Fatal("lost response did not surface as retryable failure")
	}
	id, err := client.ReconcileCheck(context.Background(), in)
	if err != nil || id != "77" || posts != 1 || patches != 1 || tokenCalls != 1 {
		t.Fatalf("id=%q err=%v posts=%d patches=%d token calls=%d", id, err, posts, patches, tokenCalls)
	}
	in.CheckRunID = id
	if got, err := client.ReconcileCheck(context.Background(), in); err != nil || got != id || posts != 1 || patches != 2 {
		t.Fatalf("repeat id=%q err=%v posts=%d patches=%d", got, err, posts, patches)
	}
}

func TestReconcileCheckBoundsResponsesAndSafeErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/access_tokens") {
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "FORGECI_SECRET_TOKEN_TEST", "expires_at": time.Now().Add(time.Hour)})
			return
		}
		_, _ = w.Write([]byte(strings.Repeat("x", maxCheckResponse+1)))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, testApp(t), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	client.now = time.Now
	_, err = client.ReconcileCheck(context.Background(), CheckRequest{Repository: "owner/repo", InstallationID: "7", CommitSHA: strings.Repeat("a", 40), ExternalID: "run-1", Status: "queued"})
	if err == nil || strings.Contains(err.Error(), "FORGECI_SECRET_TOKEN_TEST") {
		t.Fatalf("err=%v", err)
	}
}
