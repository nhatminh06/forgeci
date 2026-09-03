package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

func lifecycleMeta(key, blob string, size int64, at time.Time) store.CacheMetadata {
	return store.CacheMetadata{Workspace: "/lifecycle", Key: key, RootName: "cache", RootKind: "directory", ContentSHA256: strings.Repeat("a", 64), BlobSHA256: blob, Format: "cache-tar-gzip-v1", ArchiveSizeBytes: size, LogicalSizeBytes: 1, EntryCount: 1, CreatedAt: at}
}

func TestCacheRetentionRefreshAndExpiry(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	s.SetCacheRetention(10 * time.Minute)
	m := lifecycleMeta("retention-"+now.Format("150405.000000"), strings.Repeat("b", 64), 10, now)
	m.Workspace = "/retention-" + now.Format("150405.000000")
	if err := s.CommitCache(ctx, m); err != nil {
		t.Fatal(err)
	}
	t1 := now.Add(time.Minute)
	got, err := s.LookupCache(ctx, m.Workspace, m.Key, t1)
	if err != nil {
		t.Fatal(err)
	}
	if !got.LastAccessedAt.Equal(t1) || !got.ExpiresAt.Equal(t1.Add(10*time.Minute)) {
		t.Fatalf("refresh=%+v", got)
	}
	if _, err = s.ExpireCache(ctx, t1.Add(9*time.Minute), 0); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ExpireCache(ctx, t1.Add(10*time.Minute+time.Second), 0); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LookupCache(ctx, m.Workspace, m.Key, t1.Add(11*time.Minute)); err != store.ErrNotFound {
		t.Fatalf("expected miss, got %v", err)
	}
	if rows, err := s.ListCache(ctx, m.Workspace, 10); err != nil || len(rows) != 0 {
		t.Fatalf("expired list=%v err=%v", rows, err)
	}
}

func TestCacheLRUUniqueBlobsAndSharedReferences(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	s.SetCacheRetention(time.Hour)
	a := lifecycleMeta("lru-a-"+now.Format("150405.000000"), strings.Repeat("c", 64), 10, now)
	a.Workspace = "/lru-" + now.Format("150405.000000")
	b := lifecycleMeta("lru-b-"+now.Format("150405.000000"), strings.Repeat("d", 64), 10, now.Add(time.Second))
	b.Workspace = a.Workspace
	c := lifecycleMeta("lru-c-"+now.Format("150405.000000"), strings.Repeat("e", 64), 10, now.Add(2*time.Second))
	c.Workspace = a.Workspace
	if err := s.CommitCache(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitCache(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitCache(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupCache(ctx, b.Workspace, b.Key, now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupCache(ctx, c.Workspace, c.Key, now.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExpireCache(ctx, now.Add(12*time.Second), 20); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupCache(ctx, a.Workspace, a.Key, now.Add(12*time.Second)); err != store.ErrNotFound {
		t.Fatalf("A should be evicted: %v", err)
	}
	if _, err := s.LookupCache(ctx, b.Workspace, b.Key, now.Add(12*time.Second)); err != nil {
		t.Fatalf("B should remain: %v", err)
	}
	if _, err := s.LookupCache(ctx, c.Workspace, c.Key, now.Add(12*time.Second)); err != nil {
		t.Fatalf("C should remain: %v", err)
	}

	x := lifecycleMeta("shared-x-"+now.Format("150405.000000"), strings.Repeat("f", 64), 10, now)
	x.Workspace = "/shared-" + now.Format("150405.000000")
	y := lifecycleMeta("shared-y-"+now.Format("150405.000000"), x.BlobSHA256, 10, now.Add(time.Second))
	y.Workspace = x.Workspace
	if err := s.CommitCache(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitCache(ctx, y); err != nil {
		t.Fatal(err)
	}
	removed, err := s.ExpireCache(ctx, now.Add(20*time.Second), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupCache(ctx, y.Workspace, y.Key, now.Add(20*time.Second)); err != nil {
		t.Fatalf("shared live reference lost: %v", err)
	}
	for _, blob := range removed {
		if blob == x.BlobSHA256 {
			t.Fatal("shared blob reported unreferenced")
		}
	}
	if err := s.DeleteCache(ctx, x.Workspace, x.Key); err != nil {
		t.Fatal(err)
	}
	removed, err = s.ExpireCache(ctx, now.Add(20*time.Second), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, blob := range removed {
		if blob == x.BlobSHA256 {
			t.Fatal("shared blob reported unreferenced while B is live")
		}
	}
	if err := s.DeleteCache(ctx, y.Workspace, y.Key); err != nil {
		t.Fatal(err)
	}
	removed, err = s.ExpireCache(ctx, now.Add(21*time.Second), 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, blob := range removed {
		found = found || blob == x.BlobSHA256
	}
	if !found {
		t.Fatal("unreferenced shared blob not reported")
	}
}
