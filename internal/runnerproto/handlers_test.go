package runnerproto

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
