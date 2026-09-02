package runnerproto

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

func TestAuthenticationBoundary(t *testing.T) {
	h := NewHandlers(nil, "correct-token")
	for _, header := range []string{"", "Basic abc", "Bearer", "Bearer wrong", "Bearer correct-token extra"} {
		t.Run(header, func(t *testing.T) {
			called := false
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			response := httptest.NewRecorder()
			h.AuthMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })).ServeHTTP(response, req)
			if response.Code != http.StatusUnauthorized || called || response.Body.String() != "{\"error\":\"unauthorized\"}\n" {
				t.Fatalf("code=%d called=%v body=%q", response.Code, called, response.Body.String())
			}
		})
	}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer correct-token")
	response := httptest.NewRecorder()
	called := false
	h.AuthMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })).ServeHTTP(response, req)
	if !called {
		t.Fatal("valid token rejected")
	}
}

type signalingLeaseStore struct {
	RunnerStore
	mu    sync.Mutex
	run   *store.Run
	calls int
}

func (s *signalingLeaseStore) LeaseRun(context.Context, string, string) (*store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.run, nil
}

func TestLeaseLongPollWakesOnSubmission(t *testing.T) {
	s := &signalingLeaseStore{}
	signal := make(chan struct{})
	h := NewHandlers(s, "token", func() <-chan struct{} { return signal })
	h.longPollTTL = time.Second
	request := httptest.NewRequest(http.MethodPost, "/v1/runner/lease", strings.NewReader(`{"runner_id":"00000000-0000-4000-8000-000000000001"}`))
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.Lease(response, request); close(done) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		calls := s.calls
		s.mu.Unlock()
		if calls > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease handler did not query store")
		}
		time.Sleep(time.Millisecond)
	}
	leaseID, parallel, expires := "00000000-0000-4000-8000-000000000002", 1, time.Now().Add(time.Minute)
	s.mu.Lock()
	s.run = &store.Run{ID: "00000000-0000-4000-8000-000000000003", LeaseID: &leaseID, LeaseGeneration: 1, EffectiveParallel: &parallel, LeaseExpiresAt: &expires}
	s.mu.Unlock()
	close(signal)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("work notification did not wake long poll")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStrictRunnerJSON(t *testing.T) {
	h := NewHandlers(nil, "token")
	for _, body := range []string{`{"id":"x","unknown":true}`, `{}`, `{} {}`, strings.Repeat("x", int(smallRequestLimit)+1)} {
		response := httptest.NewRecorder()
		h.Register(response, httptest.NewRequest(http.MethodPost, "/v1/runner/register", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body length=%d code=%d", len(body), response.Code)
		}
	}
}
