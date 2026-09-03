package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/controlplane"
	"github.com/nhatminh06/forgeci/internal/store"
)

const maxBody = 1 << 20

type Manager interface {
	Ping(context.Context) error
	Submit(context.Context, string, int) (*store.Run, error)
	List(context.Context, int) ([]store.Run, error)
	Get(context.Context, string) (*store.Run, error)
	Cancel(context.Context, string) (store.RunStatus, error)
}

type Store interface {
	Ping(context.Context) error
	ListRunners(context.Context) ([]store.Runner, error)
}

type Server struct {
	Manager      Manager
	Store        Store
	OpenArtifact func(string) (*os.File, error)
	Workspace    string
}

func (s Server) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

func (s Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/healthz" && r.Method == http.MethodGet:
		s.health(w, r)
	case r.URL.Path == "/v1/runs" && r.Method == http.MethodPost:
		s.create(w, r)
	case r.URL.Path == "/v1/runs" && r.Method == http.MethodGet:
		s.list(w, r)
	case r.URL.Path == "/v1/runners" && r.Method == http.MethodGet:
		s.runners(w, r)
	case r.URL.Path == "/v1/cache" && r.Method == http.MethodGet:
		s.cacheList(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/cache/") && r.Method == http.MethodDelete:
		s.cacheDelete(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/runs/"):
		s.run(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s Server) cacheList(w http.ResponseWriter, r *http.Request) {
	b, ok := s.Store.(store.CacheStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "cache unavailable")
		return
	}
	if len(r.URL.Query()) > 1 || (r.URL.Query().Get("limit") == "" && r.URL.RawQuery != "") {
		writeError(w, http.StatusBadRequest, "invalid query parameters")
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = n
	}
	items, err := b.ListCache(r.Context(), s.Workspace, limit)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cache": items})
}
func (s Server) cacheDelete(w http.ResponseWriter, r *http.Request) {
	b, ok := s.Store.(store.CacheStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "cache unavailable")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/v1/cache/")
	if key == "" || strings.ContainsAny(key, "/?#") {
		writeError(w, http.StatusBadRequest, "invalid cache key")
		return
	}
	if err := b.DeleteCache(r.Context(), s.Workspace, key); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "cache key not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s Server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.Manager.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type createRequest struct {
	PipelineFile string `json:"pipeline_file"`
	MaxParallel  int    `json:"max_parallel"`
}

func (s Server) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := decodeStrict(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	run, err := s.Manager.Submit(r.Context(), req.PipelineFile, req.MaxParallel)
	if err != nil {
		var inputError controlplane.InputError
		if errors.As(err, &inputError) {
			writeError(w, http.StatusBadRequest, err.Error())
		} else {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
		}
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"id": run.ID, "status": run.Status})
}
func (s Server) list(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()) > 1 || r.URL.Query().Get("limit") == "" && r.URL.RawQuery != "" {
		writeError(w, http.StatusBadRequest, "invalid query parameters")
		return
	}
	limit := 20
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	runs, err := s.Manager.List(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}
func (s Server) runners(w http.ResponseWriter, r *http.Request) {
	if s.Store == nil {
		writeError(w, http.StatusServiceUnavailable, "runner management unavailable")
		return
	}
	runners, err := s.Store.ListRunners(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	if runners == nil {
		runners = []store.Runner{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runners": runners})
}
func (s Server) run(w http.ResponseWriter, r *http.Request) {
	suffix := strings.TrimPrefix(r.URL.Path, "/v1/runs/")
	parts := strings.Split(suffix, "/")
	id := parts[0]
	if id == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
		status, err := s.Manager.Cancel(r.Context(), id)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, fmt.Sprintf("run %s is already terminal", id))
			return
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "status": status})
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		run, err := s.Manager.Get(r.Context(), id)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, run)
		return
	}
	if len(parts) == 2 && parts[1] == "artifacts" && r.Method == http.MethodGet {
		s.listArtifacts(w, r, id)
		return
	}
	if len(parts) == 4 && parts[1] == "artifacts" && r.Method == http.MethodGet {
		s.downloadArtifact(w, r, id, parts[2], parts[3])
		return
	}
	writeError(w, http.StatusNotFound, "not found")
}

func (s Server) listArtifacts(w http.ResponseWriter, r *http.Request, runID string) {
	backend, ok := s.Store.(store.ArtifactStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "artifact metadata unavailable")
		return
	}
	items, err := backend.ListArtifacts(r.Context(), runID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, items)
}
func (s Server) downloadArtifact(w http.ResponseWriter, r *http.Request, runID, job, name string) {
	backend, ok := s.Store.(store.ArtifactStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "artifact storage unavailable")
		return
	}
	item, err := backend.GetArtifact(r.Context(), runID, job, name)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "artifact not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	if !item.Available {
		writeError(w, http.StatusGone, "artifact expired")
		return
	}
	if s.OpenArtifact == nil {
		writeError(w, http.StatusServiceUnavailable, "artifact storage unavailable")
		return
	}
	f, err := s.OpenArtifact(item.BlobSHA256)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "artifact blob unavailable")
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/vnd.forgeci.artifact+gzip")
	w.Header().Set("Content-Length", strconv.FormatInt(item.ArchiveSizeBytes, 10))
	w.Header().Set("X-ForgeCI-Blob-SHA256", item.BlobSHA256)
	w.WriteHeader(http.StatusOK)
	_, _ = io.CopyN(w, f, item.ArchiveSizeBytes)
}
func decodeStrict(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request must contain exactly one JSON value")
	}
	return nil
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
