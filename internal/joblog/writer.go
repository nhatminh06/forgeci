package joblog

import (
	"context"
	"io"
	"sync"

	"github.com/nhatminh06/forgeci/internal/store"
)

const MaxChunk = 64 << 10

type Writer struct {
	mu             sync.Mutex
	store          store.JobLogStore
	runID, jobName string
	sequence       int64
	ctx            context.Context
}

func New(s store.JobLogStore, runID, jobName string) *Writer {
	return &Writer{store: s, runID: runID, jobName: jobName, ctx: context.Background()}
}

func NewWithContext(ctx context.Context, s store.JobLogStore, runID, jobName string) *Writer {
	w := New(s, runID, jobName)
	w.ctx = ctx
	return w
}

func (w *Writer) For(stream store.JobLogStream) io.Writer { return streamWriter{w: w, stream: stream} }

type streamWriter struct {
	w      *Writer
	stream store.JobLogStream
}

func (s streamWriter) Write(p []byte) (int, error) { return s.w.Write(s.stream, p) }

func (w *Writer) Write(stream store.JobLogStream, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for offset := 0; offset < len(p); {
		if err := w.ctx.Err(); err != nil {
			return offset, err
		}
		n := len(p) - offset
		if n > MaxChunk {
			n = MaxChunk
		}
		w.sequence++
		err := w.store.AppendJobLog(w.ctx, store.JobLogChunk{RunID: w.runID, JobName: w.jobName, Sequence: w.sequence, Stream: stream, Payload: append([]byte(nil), p[offset:offset+n]...)})
		if err != nil {
			return offset, err
		}
		offset += n
	}
	return len(p), nil
}
