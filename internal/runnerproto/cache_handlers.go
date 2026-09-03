package runnerproto

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nhatminh06/forgeci/internal/cache"
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/store"
)

type cacheCommitRequest struct {
	RunnerID                           string `json:"runner_id"`
	RunID                              string `json:"run_id"`
	JobName                            string `json:"job_name"`
	LeaseID                            string `json:"lease_id"`
	Generation                         int    `json:"generation"`
	Key, Path, RootName, RootKind      string
	ContentSHA256, BlobSHA256, Format  string
	ArchiveSizeBytes, LogicalSizeBytes int64
	EntryCount                         int
}

func (h *Handlers) cacheStoreBackend() (store.CacheStore, bool) {
	v, ok := h.store.(store.CacheStore)
	return v, ok
}

func (h *Handlers) cacheOwner(r *http.Request, leaseID, jobName, key, path string) (*store.Run, *store.CacheMetadata, error) {
	runnerID, runID := r.URL.Query().Get("runner_id"), r.URL.Query().Get("run_id")
	generation, err := strconv.Atoi(r.URL.Query().Get("generation"))
	if !validUUID(runnerID) || !validUUID(runID) || !validUUID(leaseID) || jobName == "" || err != nil || generation < 1 {
		return nil, nil, fmt.Errorf("invalid cache ownership")
	}
	run, err := h.store.GetRun(r.Context(), runID)
	if err != nil {
		return nil, nil, err
	}
	owner := store.ArtifactOwnership{RunID: runID, RunnerID: runnerID, LeaseID: leaseID, Generation: generation, JobName: jobName}
	if !validArtifactLease(run, owner, store.JobRunning) {
		return nil, nil, fmt.Errorf("invalid or expired lease")
	}
	var declaration *store.CacheDeclaration
	for _, job := range run.Jobs {
		if job.Name == jobName {
			cfg, parseErr := config.ParseBytes(run.PipelineYAML, "stored pipeline snapshot")
			if parseErr != nil {
				return nil, nil, parseErr
			}
			parsed := cfg.Jobs[jobName]
			for _, item := range parsed.Cache.Restore {
				if item.Key == key && (path == "" || item.Path == path) {
					v := store.CacheDeclaration{Key: key, Path: path}
					declaration = &v
				}
			}
			for _, item := range parsed.Cache.Save {
				if item.Key == key && (path == "" || item.Path == path) {
					v := store.CacheDeclaration{Key: key, Path: path}
					declaration = &v
				}
			}
			break
		}
	}
	if declaration == nil {
		return nil, nil, fmt.Errorf("cache key is not declared for job")
	}
	return run, nil, nil
}

func (h *Handlers) CacheDownload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 6 || parts[0] != "v1" || parts[1] != "runner" || parts[2] != "leases" || parts[4] != "cache" || r.Method != http.MethodGet {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	leaseID, key := parts[3], parts[5]
	jobName, path := r.URL.Query().Get("job_name"), r.URL.Query().Get("path")
	run, _, err := h.cacheOwner(r, leaseID, jobName, key, path)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	backend, ok := h.cacheStoreBackend()
	if !ok || h.cacheStore == nil {
		writeError(w, http.StatusServiceUnavailable, "cache unavailable")
		return
	}
	meta, err := backend.LookupCache(r.Context(), run.Workspace, key, time.Now().UTC())
	if err != nil {
		if err == store.ErrNotFound {
			writeError(w, http.StatusNotFound, "cache miss")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "cache unavailable")
		return
	}
	f, err := h.cacheStore.OpenBlob(meta.BlobSHA256)
	if err != nil {
		writeError(w, http.StatusNotFound, "cache miss")
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/vnd.forgeci.cache+gzip")
	w.Header().Set("Content-Length", strconv.FormatInt(meta.ArchiveSizeBytes, 10))
	w.Header().Set("X-ForgeCI-Cache-Root", meta.RootName)
	w.Header().Set("X-ForgeCI-Cache-Kind", meta.RootKind)
	w.Header().Set("X-ForgeCI-Cache-Content-SHA256", meta.ContentSHA256)
	w.Header().Set("X-ForgeCI-Cache-Blob-SHA256", meta.BlobSHA256)
	w.Header().Set("X-ForgeCI-Cache-Format", meta.Format)
	w.Header().Set("X-ForgeCI-Cache-Logical-Size", strconv.FormatInt(meta.LogicalSizeBytes, 10))
	w.Header().Set("X-ForgeCI-Cache-Entry-Count", strconv.Itoa(meta.EntryCount))
	w.WriteHeader(http.StatusOK)
	_, _ = io.CopyN(w, f, meta.ArchiveSizeBytes)
}

func (h *Handlers) CacheUpload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 8 || parts[0] != "v1" || parts[1] != "runner" || parts[2] != "leases" || parts[4] != "jobs" || parts[6] != "cache-blobs" || r.Method != http.MethodPut {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	leaseID, jobName, digest := parts[3], parts[5], parts[7]
	run, _, err := h.cacheOwner(r, leaseID, jobName, r.URL.Query().Get("key"), r.URL.Query().Get("path"))
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	_ = run
	if !cacheDigest(digest) {
		writeError(w, http.StatusBadRequest, "invalid cache digest")
		return
	}
	if h.cacheStore == nil {
		writeError(w, http.StatusServiceUnavailable, "cache unavailable")
		return
	}
	if r.ContentLength < 0 {
		writeError(w, http.StatusLengthRequired, "content length required")
		return
	}
	if _, err := h.cacheStore.Put(r.Context().Done(), digest, r.ContentLength, r.Body); err != nil {
		writeError(w, http.StatusBadRequest, "cache upload failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func cacheDigest(v string) bool {
	if len(v) != 64 {
		return false
	}
	_, err := url.PathUnescape(v)
	if err != nil {
		return false
	}
	for _, r := range v {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func (h *Handlers) CacheCommit(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 7 || parts[0] != "v1" || parts[1] != "runner" || parts[2] != "leases" || parts[4] != "jobs" || parts[6] != "cache" || r.Method != http.MethodPost {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	var req cacheCommitRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	if req.LeaseID != parts[3] || req.JobName != parts[5] {
		writeError(w, http.StatusBadRequest, "cache ownership mismatch")
		return
	}
	run, _, err := h.cacheOwner(r, req.LeaseID, req.JobName, req.Key, req.Path)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if req.Format != cache.Format {
		writeError(w, http.StatusBadRequest, "invalid cache format")
		return
	}
	backend, ok := h.cacheStoreBackend()
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "cache unavailable")
		return
	}
	err = backend.CommitCache(r.Context(), store.CacheMetadata{Workspace: run.Workspace, Key: req.Key, RootName: req.RootName, RootKind: req.RootKind, ContentSHA256: req.ContentSHA256, BlobSHA256: req.BlobSHA256, Format: req.Format, ArchiveSizeBytes: req.ArchiveSizeBytes, LogicalSizeBytes: req.LogicalSizeBytes, EntryCount: req.EntryCount, CreatedAt: time.Now().UTC()})
	if err != nil {
		if err == store.ErrConflict {
			writeError(w, http.StatusConflict, "cache key conflict")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "cache commit failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
}
