package joblog

import (
	"context"
	"errors"
	"testing"

	"github.com/nhatminh06/forgeci/internal/store"
)

type memoryLogs struct {
	chunks []store.JobLogChunk
	failAt int
}

func (m *memoryLogs) AppendJobLog(_ context.Context, c store.JobLogChunk) error {
	if m.failAt > 0 && len(m.chunks)+1 == m.failAt {
		return errors.New("append failed")
	}
	m.chunks = append(m.chunks, c)
	return nil
}
func (*memoryLogs) ListJobLogs(context.Context, string, string, int64, int) ([]store.JobLogChunk, error) {
	return nil, nil
}

func TestWriterSplitsAndSharesSequence(t *testing.T) {
	m := &memoryLogs{}
	w := New(m, "run", "job")
	big := make([]byte, MaxChunk*2+3)
	if n, err := w.For(store.JobLogStdout).Write(big); err != nil || n != len(big) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if _, err := w.For(store.JobLogStderr).Write([]byte("err")); err != nil {
		t.Fatal(err)
	}
	if len(m.chunks) != 4 || m.chunks[0].Sequence != 1 || m.chunks[3].Sequence != 4 || m.chunks[3].Stream != store.JobLogStderr {
		t.Fatalf("chunks=%+v", m.chunks)
	}
}

func TestWriterFailureRetainsSequenceForRetry(t *testing.T) {
	m := &memoryLogs{failAt: 2}
	w := New(m, "run", "job")
	big := make([]byte, MaxChunk+1)
	if n, err := w.Write(store.JobLogStdout, big); n != MaxChunk || err == nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if w.Err() == nil {
		t.Fatal("missing retained error")
	}
	m.failAt = 0
	if _, err := w.Write(store.JobLogStdout, big[MaxChunk:]); err == nil {
		t.Fatal("retained failure not surfaced")
	}
}

func TestWriterCancellationStopsWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := NewWithContext(ctx, &memoryLogs{}, "run", "job")
	if n, err := w.Write(store.JobLogStdout, []byte("x")); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
