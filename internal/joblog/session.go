package joblog

import (
	"context"
	"io"

	"github.com/nhatminh06/forgeci/internal/store"
)

// Session owns durable logs for one run. Each job receives one shared sequence
// allocator, while stdout and stderr remain distinct streams.
type Session struct {
	store store.JobLogStore
	runID string
}

func NewSession(s store.JobLogStore, runID string) *Session { return &Session{store: s, runID: runID} }

func (s *Session) OpenJob(ctx context.Context, job string, stdout, stderr io.Writer) (io.Writer, io.Writer) {
	w := NewWithContext(ctx, s.store, s.runID, job)
	return tee{w: w.For(store.JobLogStdout), dst: stdout}, tee{w: w.For(store.JobLogStderr), dst: stderr}
}

type tee struct{ w, dst io.Writer }

func (t tee) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if err != nil {
		return n, err
	}
	if t.dst == nil {
		return n, nil
	}
	return t.dst.Write(p[:n])
}
