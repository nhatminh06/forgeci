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

func TestRepositoryCLI(t *testing.T) {
	const id = "00000000-0000-4000-8000-000000000001"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["pipeline"] != "ci/forge.yaml" {
				t.Fatalf("body=%v err=%v", body, err)
			}
			_, _ = w.Write([]byte(`{"id":"00000000-0000-4000-8000-000000000001","provider":"github","full_name":"owner/repo","pipeline":"ci/forge.yaml","enabled":true}`))
		case r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"repositories":[{"id":"00000000-0000-4000-8000-000000000001","provider":"github","full_name":"owner/repo","pipeline":"forge.yaml","enabled":true}]}`))
		case r.Method == http.MethodDelete:
			if r.URL.Path != "/v1/repos/"+id {
				t.Fatal(r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatal(r.Method)
		}
	}))
	defer srv.Close()
	var out strings.Builder
	if got := Main(context.Background(), []string{"repo", "add", "github", "owner/repo", "--pipeline", "ci/forge.yaml", "--server", srv.URL}, "", &out, io.Discard); got != 0 || out.String() != "Registered github owner/repo\nPipeline: ci/forge.yaml\nEnabled: true\n" {
		t.Fatalf("code=%d out=%q", got, out.String())
	}
	out.Reset()
	if got := Main(context.Background(), []string{"repo", "list", "--server", srv.URL}, "", &out, io.Discard); got != 0 || !strings.Contains(out.String(), "PROVIDER") || !strings.Contains(out.String(), "owner/repo") {
		t.Fatalf("code=%d out=%q", got, out.String())
	}
	if got := Main(context.Background(), []string{"repo", "remove", id, "--server", srv.URL}, "", io.Discard, io.Discard); got != 0 {
		t.Fatal(got)
	}
	for _, args := range [][]string{{"repo"}, {"repo", "add"}, {"repo", "add", "github", "x/y", "extra"}, {"repo", "list", "extra"}, {"repo", "remove"}} {
		if got := Main(context.Background(), args, "", io.Discard, io.Discard); got != 2 {
			t.Fatalf("args=%v code=%d", args, got)
		}
	}
}
