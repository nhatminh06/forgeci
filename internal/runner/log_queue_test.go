package runner

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/runnerproto"
	"github.com/nhatminh06/forgeci/internal/store"
)

func fillQueue(t *testing.T, q *PendingLogQueue) {
	t.Helper()
	for n := 0; n < MaxPendingLogBytes/runnerproto.MaxLogChunkBytes; n++ {
		if _, err := q.Enqueue(context.Background(), store.JobLogStdout, bytes.Repeat([]byte("x"), runnerproto.MaxLogChunkBytes)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPendingLogQueueCapacityBackpressureAndCancellation(t *testing.T) {
	q := NewPendingLogQueue()
	fillQueue(t, q)
	if q.PendingBytes() != MaxPendingLogBytes {
		t.Fatalf("pending=%d", q.PendingBytes())
	}
	started, done := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		_, err := q.Enqueue(context.Background(), store.JobLogStderr, []byte("next"))
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("enqueue did not block: %v", err)
	default:
	}
	if err := q.Ack(1); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("enqueue did not resume")
	}

	q = NewPendingLogQueue()
	fillQueue(t, q)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done = make(chan error, 1)
	go func() { _, err := q.Enqueue(ctx, store.JobLogStdout, []byte("blocked")); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("enqueue did not block: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not wake writer")
	}
}

func TestPendingLogQueueOrderAndBatches(t *testing.T) {
	q := NewPendingLogQueue()
	for n := 1; n <= 20; n++ {
		if _, err := q.Enqueue(context.Background(), store.JobLogStream(map[bool]string{true: "stdout", false: "stderr"}[n%2 == 0]), []byte{byte(n)}); err != nil {
			t.Fatal(err)
		}
	}
	for expected := int64(1); expected <= 20; {
		batch := q.PeekBatch(runnerproto.MaxLogChunksPerRequest, runnerproto.MaxLogAppendBodyBytes)
		if len(batch) == 0 || len(batch) > runnerproto.MaxLogChunksPerRequest {
			t.Fatalf("batch length=%d", len(batch))
		}
		for _, c := range batch {
			if c.Sequence != expected || len(c.Payload) != 1 || c.Payload[0] != byte(expected) {
				t.Fatalf("chunk=%+v expected=%d", c, expected)
			}
			expected++
		}
		if err := q.Ack(len(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if q.PendingBytes() != 0 {
		t.Fatalf("pending=%d", q.PendingBytes())
	}
}

func TestPendingLogQueueConcurrentWriters(t *testing.T) {
	q := NewPendingLogQueue()
	const writers, each = 8, 64
	var wg sync.WaitGroup
	for n := 0; n < writers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := q.Enqueue(context.Background(), store.JobLogStdout, []byte("x")); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	var all []PendingLogChunk
	for {
		batch := q.PeekBatch(runnerproto.MaxLogChunksPerRequest, runnerproto.MaxLogAppendBodyBytes)
		if len(batch) == 0 {
			break
		}
		all = append(all, batch...)
		if err := q.Ack(len(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if len(all) != writers*each {
		t.Fatalf("count=%d", len(all))
	}
	for i, c := range all {
		if c.Sequence != int64(i+1) {
			t.Fatalf("sequence %d=%d", i, c.Sequence)
		}
	}
}
