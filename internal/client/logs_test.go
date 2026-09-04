package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nhatminh06/forgeci/internal/store"
)

func TestJobLogsRequestAndBinaryDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/runs/run/logs" || r.URL.Query().Get("job") != "build/one" || r.URL.Query().Get("after") != "4" || r.URL.Query().Get("limit") != "7" {
			t.Fatalf("request=%s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"logs": []store.JobLogChunk{{Sequence: 5, Stream: store.JobLogStdout, Payload: []byte{0, 0xff, 0xfe}}}})
	}))
	defer server.Close()
	items, err := New(server.URL).JobLogs(context.Background(), "run", "build/one", 4, 7)
	if err != nil || len(items) != 1 || string(items[0].Payload) != string([]byte{0, 0xff, 0xfe}) {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}

func TestJobLogsFollowPreservesCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") != "9" || r.URL.Query().Get("follow") != "true" {
			t.Fatalf("request=%s", r.URL.String())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"logs": []store.JobLogChunk{}})
	}))
	defer server.Close()
	if _, err := New(server.URL).JobLogsFollow(context.Background(), "run", "job", 9, 256); err != nil {
		t.Fatal(err)
	}
}

func TestJobLogsHTTPAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"bad", 400, `{"error":"bad"}`}, {"missing", 404, `{"error":"missing"}`}, {"server", 503, `{"error":"down"}`}, {"malformed", 200, "{"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			_, err := New(srv.URL).JobLogs(context.Background(), "run", "build", 0, 256)
			if err == nil || strings.TrimSpace(err.Error()) == "" {
				t.Fatal("expected error")
			}
		})
	}
}
