package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nhatminh06/forgeci/internal/store"
)

func TestLogsCLIConsumesPagesInSequence(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("job") != "build" {
			t.Fatalf("query=%s", r.URL.RawQuery)
		}
		if requests == 1 && r.URL.Query().Get("after") != "0" {
			t.Fatalf("first cursor=%s", r.URL.Query().Get("after"))
		}
		if requests == 2 && r.URL.Query().Get("after") != "256" {
			t.Fatalf("second cursor=%s", r.URL.Query().Get("after"))
		}
		chunks := []store.JobLogChunk{}
		if requests == 1 {
			chunks = []store.JobLogChunk{{Sequence: 1, Stream: store.JobLogStdout, Payload: []byte{0, 'a'}}, {Sequence: 2, Stream: store.JobLogStderr, Payload: []byte{0xff, 'b'}}}
			for i := 3; i <= 256; i++ {
				chunks = append(chunks, store.JobLogChunk{Sequence: int64(i), Stream: store.JobLogStdout, Payload: []byte{'x'}})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"logs": chunks})
	}))
	defer srv.Close()
	var out bytes.Buffer
	if got := Main(context.Background(), []string{"logs", "00000000-0000-4000-8000-000000000001", "--job", "build", "--server", srv.URL}, "", &out, io.Discard); got != 0 {
		t.Fatalf("exit=%d output=%q", got, out.String())
	}
	want := append([]byte{0, 'a', 0xff, 'b'}, bytes.Repeat([]byte{'x'}, 254)...)
	if requests != 2 || !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("requests=%d output-len=%d", requests, out.Len())
	}
}

func TestLogsCLIMissingArguments(t *testing.T) {
	for _, args := range [][]string{{"logs"}, {"logs", "id"}} {
		if got := Main(context.Background(), args, "", io.Discard, io.Discard); got != 2 {
			t.Fatalf("args=%v exit=%d", args, got)
		}
	}
}
