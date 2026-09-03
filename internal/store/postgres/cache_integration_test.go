package postgres

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

func cacheMeta(key, content, blob string) store.CacheMetadata {
	return store.CacheMetadata{Workspace: "/workspace", Key: key, RootName: "cache", RootKind: "directory", ContentSHA256: content, BlobSHA256: blob, Format: "cache-tar-gzip-v1", ArchiveSizeBytes: 10, LogicalSizeBytes: 5, EntryCount: 1, CreatedAt: time.Now().UTC()}
}

func TestCacheCommitIdempotentAndConflict(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	m := cacheMeta("go-v1", strings.Repeat("a", 64), strings.Repeat("b", 64))
	if err := s.CommitCache(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitCache(ctx, m); err != nil {
		t.Fatalf("idempotent commit: %v", err)
	}
	m.ContentSHA256 = strings.Repeat("c", 64)
	if err := s.CommitCache(ctx, m); err != store.ErrConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestCacheConcurrentSameKey(t *testing.T) {
	s := integrationStore(t)
	m := cacheMeta("concurrent-v1", strings.Repeat("a", 64), strings.Repeat("b", 64))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.CommitCache(context.Background(), m) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("same-content concurrent commit: %v", err)
		}
	}
}

func TestCacheLookupDelete(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	m := cacheMeta("delete-v1", strings.Repeat("a", 64), strings.Repeat("b", 64))
	if err := s.CommitCache(ctx, m); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupCache(ctx, m.Workspace, m.Key, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteCache(ctx, m.Workspace, m.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupCache(ctx, m.Workspace, m.Key, time.Now().UTC()); err != store.ErrNotFound {
		t.Fatalf("expected deleted cache miss, got %v", err)
	}
}

func TestCacheLookupUsesExactKey(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	workspace := "/exact-" + strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "-")
	foo := cacheMeta("foo", strings.Repeat("a", 64), strings.Repeat("b", 64))
	foo.Workspace = workspace
	foobar := cacheMeta("foobar", strings.Repeat("c", 64), strings.Repeat("d", 64))
	foobar.Workspace = workspace
	if err := s.CommitCache(ctx, foo); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitCache(ctx, foobar); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupCache(ctx, workspace, "foobar-extra", time.Now().UTC()); err != store.ErrNotFound {
		t.Fatalf("prefix lookup matched: %v", err)
	}
	for _, want := range []store.CacheMetadata{foo, foobar} {
		got, err := s.LookupCache(ctx, workspace, want.Key, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if got.Key != want.Key || got.ContentSHA256 != want.ContentSHA256 || got.BlobSHA256 != want.BlobSHA256 {
			t.Fatalf("wrong metadata: %+v", got)
		}
	}
}
