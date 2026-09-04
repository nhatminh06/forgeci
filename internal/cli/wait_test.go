package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

func TestWaitTerminalStates(t *testing.T) {
	for _, tc := range []struct {
		status store.RunStatus
		code   int
	}{{store.RunPassed, 0}, {store.RunFailed, 1}, {store.RunCanceled, 1}, {store.RunError, 1}, {store.RunAborted, 1}} {
		t.Run(string(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				now := time.Now()
				_ = json.NewEncoder(w).Encode(store.Run{ID: "run", Status: tc.status, FinishedAt: &now, Jobs: []store.Job{{Name: "job", Status: store.JobStatus(tc.status)}}})
			}))
			defer srv.Close()
			var out strings.Builder
			if got := Main(context.Background(), []string{"wait", "run", "--server", srv.URL}, "", &out, io.Discard); got != tc.code {
				t.Fatalf("code=%d", got)
			}
			if !strings.Contains(out.String(), string(tc.status)) || !strings.Contains(out.String(), "job") {
				t.Fatalf("out=%s", out.String())
			}
		})
	}
}

func TestWaitEventuallyPassesAndRejectsBadArguments(t *testing.T) {
	old := waitPollInterval
	waitPollInterval = time.Millisecond
	defer func() { waitPollInterval = old }()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		run := store.Run{ID: "run", Status: store.RunRunning}
		if calls > 1 {
			now := time.Now()
			run.Status, run.FinishedAt = store.RunPassed, &now
		}
		_ = json.NewEncoder(w).Encode(run)
	}))
	defer srv.Close()
	if got := Main(context.Background(), []string{"wait", "run", "--server", srv.URL, "--timeout", "1s"}, "", io.Discard, io.Discard); got != 0 || calls < 2 {
		t.Fatalf("code=%d calls=%d", got, calls)
	}
	for _, args := range [][]string{{"wait"}, {"wait", "run", "extra"}, {"wait", "run", "--timeout", "0s"}} {
		if got := Main(context.Background(), args, "", io.Discard, io.Discard); got != 2 {
			t.Fatalf("args=%v code=%d", args, got)
		}
	}
}
