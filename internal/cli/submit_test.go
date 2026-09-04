package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSubmitQuietAndDefaultOutput(t *testing.T) {
	const id = "00000000-0000-4000-8000-000000000001"
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.URL.Query().Get("unused") + r.Method + " " + r.URL.Path
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["pipeline_file"] != "custom.yaml" || body["max_parallel"].(float64) != 3 {
			t.Fatalf("body=%v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
	}))
	defer srv.Close()
	var quiet strings.Builder
	if got := Main(context.Background(), []string{"submit", "--quiet", "--server", srv.URL, "--file", "custom.yaml", "--jobs", "3"}, "", &quiet, io.Discard); got != 0 || quiet.String() != id+"\n" {
		t.Fatalf("code=%d out=%q", got, quiet.String())
	}
	if !strings.Contains(seen, "POST /v1/runs") {
		t.Fatal(seen)
	}
	var normal strings.Builder
	if got := Main(context.Background(), []string{"submit", "--server", srv.URL, "--file", "custom.yaml", "--jobs", "3"}, "", &normal, io.Discard); got != 0 || normal.String() != "Run "+id+" queued\n" {
		t.Fatalf("code=%d out=%q", got, normal.String())
	}
}

func TestSubmitRejectsInvalidAndBackendFailure(t *testing.T) {
	for _, args := range [][]string{{"submit", "--jobs", "0"}, {"submit", "extra"}} {
		if got := Main(context.Background(), args, "", io.Discard, io.Discard); got != 2 {
			t.Fatalf("args=%v code=%d", args, got)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "down", http.StatusServiceUnavailable) }))
	defer srv.Close()
	if got := Main(context.Background(), []string{"submit", "--quiet", "--server", srv.URL}, "", io.Discard, io.Discard); got != 2 {
		t.Fatal(got)
	}
}
