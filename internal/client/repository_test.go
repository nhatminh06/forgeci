package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nhatminh06/forgeci/internal/scm"
)

func TestRepositoryClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/repos":
			if body := readBody(t, r); body != `{"provider":"github","full_name":"owner/repo","pipeline":"forge.yaml"}` {
				t.Fatalf("body=%s", body)
			}
			w.Write([]byte(`{"id":"00000000-0000-4000-8000-000000000001","provider":"github","full_name":"owner/repo","pipeline":"forge.yaml","enabled":true,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/repos":
			w.Write([]byte(`{"repositories":[]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/repos/00000000-0000-4000-8000-000000000001":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("%s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	c := New(server.URL)
	item, err := c.AddRepository(context.Background(), scm.GitHub, "owner/repo", "forge.yaml")
	if err != nil || item.FullName != "owner/repo" || item.CreatedAt.IsZero() {
		t.Fatalf("item=%+v err=%v", item, err)
	}
	items, err := c.Repositories(context.Background())
	if err != nil || len(items) != 0 {
		t.Fatalf("items=%v err=%v", items, err)
	}
	if err := c.RemoveRepository(context.Background(), item.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryClientErrors(t *testing.T) {
	for _, code := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
		_, err := New(server.URL).Repositories(context.Background())
		server.Close()
		if err == nil {
			t.Fatalf("status=%d accepted", code)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("{")) }))
	defer server.Close()
	if _, err := New(server.URL).Repositories(context.Background()); err == nil {
		t.Fatal("malformed response accepted")
	}
}

func readBody(t *testing.T, r *http.Request) string {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
