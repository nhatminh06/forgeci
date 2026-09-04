package runner

import (
	"context"
	"errors"
	"sync"

	"github.com/nhatminh06/forgeci/internal/runnerproto"
	"github.com/nhatminh06/forgeci/internal/store"
)

// MaxPendingLogBytes bounds payload retained for one remote job. Payload bytes
// dominate request memory; request envelopes are short-lived and bounded by the
// protocol request limits.
const MaxPendingLogBytes = 1 << 20

var ErrLogQueueClosed = errors.New("remote log queue closed")

type PendingLogChunk struct {
	Sequence int64              `json:"sequence"`
	Stream   store.JobLogStream `json:"stream"`
	Payload  []byte             `json:"payload"`
}

type PendingLogQueue struct {
	mu      sync.Mutex
	chunks  []PendingLogChunk
	pending int
	next    int64
	closed  bool
	failure error
	notify  chan struct{}
}

func NewPendingLogQueue() *PendingLogQueue { return &PendingLogQueue{notify: make(chan struct{})} }

func (q *PendingLogQueue) signalLocked() { close(q.notify); q.notify = make(chan struct{}) }

func (q *PendingLogQueue) Enqueue(ctx context.Context, stream store.JobLogStream, payload []byte) (int64, error) {
	if (stream != store.JobLogStdout && stream != store.JobLogStderr) || len(payload) == 0 || len(payload) > runnerproto.MaxLogChunkBytes {
		return 0, errors.New("invalid remote log chunk")
	}
	for {
		q.mu.Lock()
		if q.failure != nil {
			err := q.failure
			q.mu.Unlock()
			return 0, err
		}
		if q.closed {
			q.mu.Unlock()
			return 0, ErrLogQueueClosed
		}
		if q.pending+len(payload) <= MaxPendingLogBytes {
			q.next++
			sequence := q.next
			q.chunks = append(q.chunks, PendingLogChunk{Sequence: sequence, Stream: stream, Payload: append([]byte(nil), payload...)})
			q.pending += len(payload)
			q.signalLocked()
			q.mu.Unlock()
			return sequence, nil
		}
		wake := q.notify
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-wake:
		}
	}
}

func (q *PendingLogQueue) PeekBatch(maxChunks int, maxBodyBytes int) []PendingLogChunk {
	q.mu.Lock()
	defer q.mu.Unlock()
	if maxChunks > runnerproto.MaxLogChunksPerRequest {
		maxChunks = runnerproto.MaxLogChunksPerRequest
	}
	if maxBodyBytes > runnerproto.MaxLogAppendBodyBytes {
		maxBodyBytes = runnerproto.MaxLogAppendBodyBytes
	}
	used := 512
	out := make([]PendingLogChunk, 0, maxChunks)
	for _, c := range q.chunks {
		if len(out) == maxChunks {
			break
		}
		encoded := 4*((len(c.Payload)+2)/3) + 192
		if used+encoded > maxBodyBytes {
			break
		}
		used += encoded
		out = append(out, PendingLogChunk{Sequence: c.Sequence, Stream: c.Stream, Payload: append([]byte(nil), c.Payload...)})
	}
	return out
}

func (q *PendingLogQueue) WaitBatch(ctx context.Context, maxChunks, maxBodyBytes int) ([]PendingLogChunk, error) {
	for {
		if batch := q.PeekBatch(maxChunks, maxBodyBytes); len(batch) > 0 {
			return batch, nil
		}
		q.mu.Lock()
		if q.failure != nil {
			err := q.failure
			q.mu.Unlock()
			return nil, err
		}
		if q.closed {
			q.mu.Unlock()
			return nil, ErrLogQueueClosed
		}
		wake := q.notify
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		}
	}
}

func (q *PendingLogQueue) Ack(count int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if count < 0 || count > len(q.chunks) {
		return errors.New("invalid remote log acknowledgement")
	}
	for _, c := range q.chunks[:count] {
		q.pending -= len(c.Payload)
	}
	q.chunks = append([]PendingLogChunk(nil), q.chunks[count:]...)
	q.signalLocked()
	return nil
}

func (q *PendingLogQueue) Fail(err error) {
	if err == nil {
		return
	}
	q.mu.Lock()
	if q.failure == nil {
		q.failure = err
		q.signalLocked()
	}
	q.mu.Unlock()
}
func (q *PendingLogQueue) Close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.signalLocked()
	}
	q.mu.Unlock()
}
func (q *PendingLogQueue) PendingBytes() int { q.mu.Lock(); defer q.mu.Unlock(); return q.pending }

// WaitForPending is intended for coordination and deterministic tests.
func (q *PendingLogQueue) WaitForPending(ctx context.Context, target int) error {
	for {
		q.mu.Lock()
		if q.pending >= target {
			q.mu.Unlock()
			return nil
		}
		wake := q.notify
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}
