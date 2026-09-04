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
}

func New(s store.JobLogStore, runID, jobName string) *Writer {
	return &Writer{store: s, runID: runID, jobName: jobName}
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
	if len(p) > MaxChunk {
		p = p[:MaxChunk]
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sequence++
	err := w.store.AppendJobLog(context.Background(), store.JobLogChunk{RunID: w.runID, JobName: w.jobName, Sequence: w.sequence, Stream: stream, Payload: append([]byte(nil), p...)})
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
