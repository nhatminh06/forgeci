package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/artifact"
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/runworkspace"
	"github.com/nhatminh06/forgeci/internal/snapshot"
	"github.com/nhatminh06/forgeci/internal/store"
)

const protocolVersion = 2

type jobLease struct {
	RunID                string              `json:"run_id"`
	JobName              string              `json:"job_name"`
	LeaseID              string              `json:"lease_id"`
	Generation           int                 `json:"generation"`
	Job                  store.JobDefinition `json:"job"`
	ExpiresAt            time.Time           `json:"expires_at"`
	SourceSnapshotSHA256 string              `json:"source_snapshot_sha256"`
	BlobSHA256           string              `json:"blob_sha256"`
	ArchiveSizeBytes     int64               `json:"archive_size_bytes"`
	LogicalSizeBytes     int64               `json:"logical_size_bytes"`
	EntryCount           int                 `json:"entry_count"`
	ArchiveFormat        string              `json:"archive_format"`
}

type activeJob struct {
	lease  jobLease
	cancel context.CancelFunc
}

type Config struct {
	ServerAddr         string
	ServerToken        string
	Name               string
	WorkspaceRoot      string
	Workspace          string // deprecated programmatic alias
	StateDir           string
	MaxParallel        int
	TLSCA              string
	MaxSnapshotArchive int64
	MaxSnapshotLogical int64
	MaxSnapshotEntries int
	MaxArtifactArchive int64
	MaxArtifactLogical int64
	MaxArtifactEntries int
}

type RemoteRunner struct {
	id           string
	config       Config
	httpClient   *http.Client
	leaseMutex   sync.Mutex
	shutdownOnce sync.Once
	shutdownChan chan struct{}
	workers      sync.WaitGroup
	active       map[string]*activeJob
	ctx          context.Context
	cancel       context.CancelFunc
}

func main() {
	homeDir, _ := os.UserHomeDir()
	cfg := &Config{}
	flag.StringVar(&cfg.ServerAddr, "server", "http://localhost:9090", "Control plane runner listener address")
	cfg.ServerToken = os.Getenv("FORGECI_RUNNER_TOKEN")
	flag.StringVar(&cfg.Name, "name", hostname(), "Runner name (default: hostname)")
	flag.StringVar(&cfg.WorkspaceRoot, "workspace-root", "", "Root for isolated run workspaces")
	flag.StringVar(&cfg.WorkspaceRoot, "workspace", "", "Deprecated alias for --workspace-root")
	flag.StringVar(&cfg.StateDir, "state-dir", filepath.Join(homeDir, ".forgeci", "runner"), "State directory for runner ID")
	flag.IntVar(&cfg.MaxParallel, "max-parallel", runtime.NumCPU(), "Maximum parallel jobs")
	flag.StringVar(&cfg.TLSCA, "ca-cert", "", "Path to CA certificate for TLS verification")
	flag.Int64Var(&cfg.MaxSnapshotArchive, "snapshot-max-archive-bytes", 512<<20, "maximum downloaded snapshot bytes")
	flag.Int64Var(&cfg.MaxSnapshotLogical, "snapshot-max-logical-bytes", 1<<30, "maximum extracted source bytes")
	flag.IntVar(&cfg.MaxSnapshotEntries, "snapshot-max-entries", 100000, "maximum extracted source entries")
	flag.Int64Var(&cfg.MaxArtifactArchive, "artifact-max-archive-bytes", 512<<20, "maximum transferred artifact bytes")
	flag.Int64Var(&cfg.MaxArtifactLogical, "artifact-max-logical-bytes", 1<<30, "maximum extracted artifact bytes")
	flag.IntVar(&cfg.MaxArtifactEntries, "artifact-max-entries", 100000, "maximum extracted artifact entries")
	flag.Parse()

	if cfg.ServerToken == "" {
		fmt.Fprintln(os.Stderr, "Error: runner token required (set FORGECI_RUNNER_TOKEN)")
		os.Exit(1)
	}

	if cfg.MaxParallel < 1 {
		fmt.Fprintf(os.Stderr, "Error: max-parallel must be > 0\n")
		os.Exit(1)
	}
	if cfg.WorkspaceRoot == "" {
		fmt.Fprintln(os.Stderr, "Error: workspace-root is required")
		os.Exit(1)
	}
	if cfg.MaxSnapshotArchive < 0 || cfg.MaxSnapshotLogical < 0 || cfg.MaxSnapshotEntries < 0 {
		fmt.Fprintln(os.Stderr, "Error: snapshot limits must not be negative")
		os.Exit(1)
	}
	if cfg.MaxArtifactArchive < 1 || cfg.MaxArtifactLogical < 1 || cfg.MaxArtifactEntries < 1 {
		fmt.Fprintln(os.Stderr, "Error: artifact limits must be greater than zero")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rr, err := newRemoteRunner(ctx, cancel, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create runner: %v\n", err)
		os.Exit(1)
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutdown requested")
		rr.shutdown()
	}()

	if err := rr.run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func hostname() string {
	host, _ := os.Hostname()
	return host
}

func newRemoteRunner(ctx context.Context, cancel context.CancelFunc, cfg *Config) (*RemoteRunner, error) {
	if cfg.MaxSnapshotArchive < 0 || cfg.MaxSnapshotLogical < 0 || cfg.MaxSnapshotEntries < 0 {
		return nil, fmt.Errorf("snapshot limits must not be negative")
	}
	if cfg.WorkspaceRoot == "" {
		cfg.WorkspaceRoot = cfg.Workspace
	}
	root, err := filepath.Abs(cfg.WorkspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace root: %w", err)
	}
	cfg.WorkspaceRoot = root
	state, err := filepath.Abs(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create workspace root: %w", err)
	}
	if err := os.MkdirAll(state, 0700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("canonicalize workspace root: %w", err)
	}
	state, err = filepath.EvalSymlinks(state)
	if err != nil {
		return nil, fmt.Errorf("canonicalize state directory: %w", err)
	}
	if root == state || strings.HasPrefix(root, state+string(filepath.Separator)) || strings.HasPrefix(state, root+string(filepath.Separator)) {
		return nil, fmt.Errorf("workspace root and state directory must not contain one another")
	}
	cfg.WorkspaceRoot = root
	cfg.StateDir = state
	if err := runworkspace.CleanupStale(root); err != nil {
		return nil, fmt.Errorf("clean stale workspaces: %w", err)
	}
	// Load or create runner identity
	id, err := loadOrCreateRunnerID(cfg.StateDir)
	if err != nil {
		return nil, fmt.Errorf("load runner identity: %w", err)
	}

	// Setup HTTP client
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	if cfg.TLSCA != "" {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if cfg.TLSCA != "" {
			pem, err := os.ReadFile(cfg.TLSCA)
			if err != nil {
				return nil, fmt.Errorf("read CA certificate: %w", err)
			}
			pool, err := x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("load system CA pool: %w", err)
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("CA certificate contains no certificates")
			}
			tlsConfig.RootCAs = pool
		}
		httpClient.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	rr := &RemoteRunner{
		id:           id,
		config:       *cfg,
		httpClient:   httpClient,
		shutdownChan: make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
		active:       make(map[string]*activeJob),
	}

	return rr, nil
}

func loadOrCreateRunnerID(stateDir string) (string, error) {
	stateFile := filepath.Join(stateDir, "runner-id")

	// Try to load existing
	if data, err := os.ReadFile(stateFile); err == nil {
		id := strings.TrimSpace(string(data))
		if _, err := uuid.Parse(id); err != nil {
			return "", fmt.Errorf("malformed persisted runner ID: %w", err)
		}
		return id, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}

	// Create new
	id := uuid.New().String()

	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return "", err
	}

	temporary, err := os.CreateTemp(stateDir, ".runner-id-*")
	if err != nil {
		return "", err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return "", err
	}
	if _, err := temporary.WriteString(id + "\n"); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryName, stateFile); err != nil {
		return "", err
	}

	return id, nil
}

func (rr *RemoteRunner) run() error {
	// Register with control plane
	if err := rr.register(); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	fmt.Printf("Runner %s registered with control plane\n", rr.config.Name)

	// Start heartbeat loop
	for _, worker := range []func(){rr.heartbeatLoop, rr.leaseDeadlineLoop, rr.workLoop} {
		rr.workers.Add(1)
		go func() { defer rr.workers.Done(); worker() }()
	}

	// Wait for shutdown
	<-rr.shutdownChan
	rr.workers.Wait()
	return nil
}

func (rr *RemoteRunner) register() error {
	reqBody := fmt.Sprintf(`{
		"id": "%s",
		"name": "%s",
		"protocol_version": %d,
		"os": "%s",
		"arch": "%s",
		"docker": %v,
		"max_parallel": %d
	}`, rr.id, rr.config.Name, protocolVersion, runtime.GOOS, runtime.GOARCH, hasDocker(), rr.config.MaxParallel)

	req, _ := http.NewRequest("POST", rr.config.ServerAddr+"/v1/runner/register", bytes.NewReader([]byte(reqBody)))
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", rr.config.ServerToken))
	req.Header.Set("Content-Type", "application/json")

	resp, err := rr.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register failed: %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (rr *RemoteRunner) heartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rr.sendHeartbeat()
		case <-rr.shutdownChan:
			return
		}
	}
}

func (rr *RemoteRunner) leaseDeadlineLoop() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			rr.leaseMutex.Lock()
			for _, job := range rr.active {
				if !job.lease.ExpiresAt.IsZero() && !now.Before(job.lease.ExpiresAt) {
					job.cancel()
				}
			}
			rr.leaseMutex.Unlock()
		case <-rr.shutdownChan:
			return
		}
	}
}

func (rr *RemoteRunner) sendHeartbeat() {
	rr.leaseMutex.Lock()
	leases := make([]store.LeaseHeartbeat, 0, len(rr.active))
	for _, job := range rr.active {
		leases = append(leases, store.LeaseHeartbeat{RunID: job.lease.RunID, JobName: job.lease.JobName, LeaseID: job.lease.LeaseID, Generation: job.lease.Generation})
	}
	rr.leaseMutex.Unlock()
	reqData := map[string]interface{}{"runner_id": rr.id, "leases": leases}
	reqBody, _ := json.Marshal(reqData)

	req, _ := http.NewRequest("POST", rr.config.ServerAddr+"/v1/runner/heartbeat", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", rr.config.ServerToken))
	req.Header.Set("Content-Type", "application/json")

	resp, err := rr.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var hb struct {
			Leases         []store.LeaseHeartbeatResult `json:"leases"`
			ServerShutdown bool                         `json:"server_shutdown"`
		}
		if json.NewDecoder(resp.Body).Decode(&hb) != nil {
			rr.cancelActive()
			return
		}
		rr.leaseMutex.Lock()
		seen := map[string]store.LeaseHeartbeatResult{}
		for _, result := range hb.Leases {
			seen[result.LeaseID] = result
		}
		for id, job := range rr.active {
			result, ok := seen[id]
			if hb.ServerShutdown || !ok || !result.Valid || result.CancelRequested {
				job.cancel()
				continue
			}
			if result.ExpiresAt != nil {
				job.lease.ExpiresAt = *result.ExpiresAt
			}
		}
		rr.leaseMutex.Unlock()
	}
}

func (rr *RemoteRunner) workLoop() {
	for {
		select {
		case <-rr.shutdownChan:
			return
		default:
			rr.requestLease()
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (rr *RemoteRunner) requestLease() {
	rr.leaseMutex.Lock()
	if len(rr.active) >= rr.config.MaxParallel {
		rr.leaseMutex.Unlock()
		return // Already have active run
	}
	rr.leaseMutex.Unlock()

	reqData := map[string]string{"runner_id": rr.id}
	reqBody, _ := json.Marshal(reqData)
	req, _ := http.NewRequest("POST", rr.config.ServerAddr+"/v1/runner/lease", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", rr.config.ServerToken))
	req.Header.Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(rr.ctx, 30*time.Second)
	req = req.WithContext(ctx)
	defer cancel()

	resp, err := rr.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	var lease jobLease
	if err := json.NewDecoder(resp.Body).Decode(&lease); err != nil {
		return
	}
	if lease.RunID == "" || lease.JobName == "" {
		return
	}
	if lease.ArchiveFormat != snapshot.Format || lease.SourceSnapshotSHA256 == "" || lease.BlobSHA256 == "" || lease.ArchiveSizeBytes < 1 || lease.EntryCount < 0 || lease.LogicalSizeBytes < 0 {
		message := "invalid snapshot lease metadata"
		_ = rr.sendJobComplete(lease, "ERROR", &message)
		return
	}
	limits := rr.snapshotLimits()
	if lease.ArchiveSizeBytes > limits.MaxArchiveBytes || lease.LogicalSizeBytes > limits.MaxLogicalBytes || lease.EntryCount > limits.MaxEntries {
		message := "snapshot exceeds runner limits"
		_ = rr.sendJobComplete(lease, "ERROR", &message)
		return
	}

	executionCtx, executionCancel := context.WithCancel(rr.ctx)
	rr.leaseMutex.Lock()
	if len(rr.active) >= rr.config.MaxParallel {
		rr.leaseMutex.Unlock()
		executionCancel()
		return
	}
	rr.active[lease.LeaseID] = &activeJob{lease: lease, cancel: executionCancel}
	rr.leaseMutex.Unlock()
	rr.workers.Add(1)
	go func() {
		defer rr.workers.Done()
		rr.executeLeasedJob(executionCtx, lease)
		rr.leaseMutex.Lock()
		delete(rr.active, lease.LeaseID)
		rr.leaseMutex.Unlock()
	}()
}

func (rr *RemoteRunner) executeLeasedJob(ctx context.Context, lease jobLease) {
	meta := snapshot.Metadata{SourceDigest: lease.SourceSnapshotSHA256, BlobDigest: lease.BlobSHA256, Format: lease.ArchiveFormat, ArchiveSizeBytes: lease.ArchiveSizeBytes, LogicalSizeBytes: lease.LogicalSizeBytes, EntryCount: lease.EntryCount}
	marker := runworkspace.Marker{RunID: lease.RunID, JobName: lease.JobName, LeaseID: lease.LeaseID, Generation: lease.Generation, SourceDigest: meta.SourceDigest}
	workspace, err := runworkspace.Create(rr.config.WorkspaceRoot, marker)
	if err != nil {
		rr.completeJobError(lease, err)
		return
	}
	defer func() {
		if err := runworkspace.Remove(rr.config.WorkspaceRoot, workspace, marker); err != nil {
			fmt.Fprintf(os.Stderr, "workspace cleanup failed: %v\n", err)
		}
	}()
	sourceDir := filepath.Join(workspace, "source")
	if err = os.Mkdir(sourceDir, 0700); err != nil {
		rr.completeJobError(lease, err)
		return
	}
	archive, err := os.CreateTemp(workspace, ".source-*.tar.gz")
	if err != nil {
		rr.completeJobError(lease, err)
		return
	}
	archivePath := archive.Name()
	if err = rr.downloadJobSnapshot(ctx, lease, meta, archive); err != nil {
		archive.Close()
		os.Remove(archivePath)
		rr.completeJobError(lease, err)
		return
	}
	if err = archive.Close(); err != nil {
		os.Remove(archivePath)
		rr.completeJobError(lease, err)
		return
	}
	if err = snapshot.Extract(archivePath, sourceDir, meta, rr.snapshotLimits()); err != nil {
		os.Remove(archivePath)
		rr.completeJobError(lease, err)
		return
	}
	if err = os.Remove(archivePath); err != nil {
		rr.completeJobError(lease, err)
		return
	}
	uploads := make([]config.ArtifactUpload, len(lease.Job.Uploads))
	for i, u := range lease.Job.Uploads {
		uploads[i] = config.ArtifactUpload{Name: u.Name, Path: u.Path}
	}
	downloads := make([]config.ArtifactDownload, len(lease.Job.Downloads))
	for i, d := range lease.Job.Downloads {
		downloads[i] = config.ArtifactDownload{From: d.From, Name: d.Name, Into: d.Into}
	}
	transport := runnerArtifactTransport{rr: rr, runID: lease.RunID, leaseID: lease.LeaseID, generation: lease.Generation}
	artifacts, err := artifact.NewRemoteSession(sourceDir, filepath.Join(workspace, "artifact-tmp"), transport, rr.artifactLimits())
	if err != nil {
		rr.completeJobError(lease, err)
		return
	}
	if err = artifacts.Restore(ctx, lease.JobName, downloads); err != nil {
		rr.completeArtifactFailure(lease, ctx, err)
		return
	}
	job := config.Job{Image: lease.Job.Image, Artifacts: config.Artifacts{Upload: uploads, Download: downloads}}
	for _, command := range lease.Job.Steps {
		job.Steps = append(job.Steps, config.Step{Run: command})
	}
	local := executor.Local{Directory: sourceDir}
	var docker *executor.Docker
	if job.Image != nil {
		docker, err = executor.NewDocker(sourceDir)
		if err != nil {
			rr.completeJobError(lease, err)
			return
		}
	}
	result := (executor.Job{Local: local, Docker: docker}).RunJob(ctx, lease.JobName, job, os.Stdout, os.Stderr)
	if result.Err != nil {
		status := "FAILED"
		if ctx.Err() != nil {
			status = "CANCELED"
		} else if result.ExitCode < 0 {
			status = "ERROR"
		}
		message := result.Err.Error()
		_ = rr.sendJobComplete(lease, status, &message)
		return
	}
	if err = artifacts.Publish(ctx, lease.JobName, uploads); err != nil {
		rr.completeArtifactFailure(lease, ctx, err)
		return
	}
	_ = rr.sendJobComplete(lease, "PASSED", nil)
}

func (rr *RemoteRunner) completeArtifactFailure(lease jobLease, ctx context.Context, err error) {
	status := "ERROR"
	if ctx.Err() != nil {
		status = "CANCELED"
	} else if artifact.IsPipelineError(err) {
		status = "FAILED"
	}
	message := err.Error()
	_ = rr.sendJobComplete(lease, status, &message)
}
func (rr *RemoteRunner) completeJobError(lease jobLease, err error) {
	message := err.Error()
	_ = rr.sendJobComplete(lease, "ERROR", &message)
}

func (rr *RemoteRunner) downloadJobSnapshot(ctx context.Context, lease jobLease, meta snapshot.Metadata, destination io.Writer) error {
	values := url.Values{}
	values.Set("runner_id", rr.id)
	values.Set("run_id", lease.RunID)
	values.Set("job_name", lease.JobName)
	values.Set("generation", strconv.Itoa(lease.Generation))
	endpoint := fmt.Sprintf("%s/v1/runner/leases/%s/source?%s", rr.config.ServerAddr, lease.LeaseID, values.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", rr.config.ServerToken))
	resp, err := rr.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("download source snapshot: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("download source snapshot: %d: %s", resp.StatusCode, body)
	}
	return snapshot.CopyVerified(destination, resp.Body, meta.ArchiveSizeBytes, meta.BlobDigest, rr.snapshotLimits().MaxArchiveBytes)
}

func (rr *RemoteRunner) artifactLimits() artifact.Limits {
	limits := artifact.DefaultLimits()
	limits.MaxArchiveBytes = rr.config.MaxArtifactArchive
	limits.MaxLogicalBytes = rr.config.MaxArtifactLogical
	limits.MaxFileBytes = rr.config.MaxArtifactLogical
	limits.MaxEntries = rr.config.MaxArtifactEntries
	return limits
}

type runnerArtifactTransport struct {
	rr             *RemoteRunner
	runID, leaseID string
	generation     int
}

func (t runnerArtifactTransport) query(job string) string {
	values := url.Values{}
	values.Set("runner_id", t.rr.id)
	values.Set("run_id", t.runID)
	values.Set("generation", strconv.Itoa(t.generation))
	if job != "" {
		values.Set("consumer_job", job)
	}
	return values.Encode()
}
func (t runnerArtifactTransport) Upload(ctx context.Context, job string, meta artifact.Metadata, body io.Reader) error {
	endpoint := fmt.Sprintf("%s/v1/runner/leases/%s/jobs/%s/artifact-blobs/%s?%s", t.rr.config.ServerAddr, t.leaseID, url.PathEscape(job), meta.BlobSHA256, t.query(""))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.rr.config.ServerToken))
	req.ContentLength = meta.ArchiveSizeBytes
	resp, err := t.rr.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("upload artifact: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("upload artifact: %d: %s", resp.StatusCode, data)
	}
	return nil
}
func (t runnerArtifactTransport) Commit(ctx context.Context, job string, metas []artifact.Metadata) error {
	items := make([]map[string]any, len(metas))
	for i, m := range metas {
		items[i] = map[string]any{"name": m.Name, "root_name": m.RootName, "root_kind": m.RootKind, "content_sha256": m.ContentSHA256, "blob_sha256": m.BlobSHA256, "format": m.Format, "archive_size_bytes": m.ArchiveSizeBytes, "logical_size_bytes": m.LogicalSizeBytes, "entry_count": m.EntryCount}
	}
	payload := map[string]any{"runner_id": t.rr.id, "run_id": t.runID, "lease_id": t.leaseID, "generation": t.generation, "job_name": job, "artifacts": items}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/v1/runner/leases/%s/jobs/%s/artifacts/commit", t.rr.config.ServerAddr, t.leaseID, url.PathEscape(job))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.rr.config.ServerToken))
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.rr.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("commit artifacts: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("commit artifacts: %d: %s", resp.StatusCode, body)
	}
	return nil
}
func (t runnerArtifactTransport) Download(ctx context.Context, producer, name, consumer string, destination io.Writer) (artifact.Metadata, error) {
	endpoint := fmt.Sprintf("%s/v1/runner/leases/%s/artifacts/%s/%s?%s", t.rr.config.ServerAddr, t.leaseID, url.PathEscape(producer), url.PathEscape(name), t.query(consumer))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return artifact.Metadata{}, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.rr.config.ServerToken))
	resp, err := t.rr.httpClient.Do(req)
	if err != nil {
		return artifact.Metadata{}, fmt.Errorf("download artifact: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return artifact.Metadata{}, fmt.Errorf("download artifact: %d: %s", resp.StatusCode, body)
	}
	archiveSize, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return artifact.Metadata{}, fmt.Errorf("invalid artifact size")
	}
	logical, err := strconv.ParseInt(resp.Header.Get("X-ForgeCI-Logical-Size"), 10, 64)
	if err != nil {
		return artifact.Metadata{}, fmt.Errorf("invalid artifact logical size")
	}
	entries, err := strconv.Atoi(resp.Header.Get("X-ForgeCI-Entry-Count"))
	if err != nil {
		return artifact.Metadata{}, fmt.Errorf("invalid artifact entry count")
	}
	meta := artifact.Metadata{Name: name, RootName: resp.Header.Get("X-ForgeCI-Root-Name"), RootKind: resp.Header.Get("X-ForgeCI-Root-Kind"), ContentSHA256: resp.Header.Get("X-ForgeCI-Content-SHA256"), BlobSHA256: resp.Header.Get("X-ForgeCI-Blob-SHA256"), Format: artifact.Format, ArchiveSizeBytes: archiveSize, LogicalSizeBytes: logical, EntryCount: entries}
	if err := artifact.CopyVerified(destination, resp.Body, archiveSize, meta.BlobSHA256, t.rr.artifactLimits().MaxArchiveBytes); err != nil {
		return artifact.Metadata{}, err
	}
	return meta, nil
}

func (rr *RemoteRunner) cancelActive() {
	rr.leaseMutex.Lock()
	cancels := make([]context.CancelFunc, 0, len(rr.active))
	for _, job := range rr.active {
		cancels = append(cancels, job.cancel)
	}
	rr.leaseMutex.Unlock()
	for _, stop := range cancels {
		stop()
	}
}

func (rr *RemoteRunner) sendJobComplete(lease jobLease, status string, message *string) error {
	payload := map[string]any{"runner_id": rr.id, "run_id": lease.RunID, "job_name": lease.JobName, "lease_id": lease.LeaseID, "generation": lease.Generation, "status": status}
	if message != nil {
		payload["error"] = *message
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(rr.ctx, http.MethodPost, fmt.Sprintf("%s/v1/runner/leases/%s/complete", rr.config.ServerAddr, lease.LeaseID), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", rr.config.ServerToken))
	req.Header.Set("Content-Type", "application/json")
	resp, err := rr.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("complete job: %d: %s", resp.StatusCode, data)
	}
	return nil
}

func (rr *RemoteRunner) snapshotLimits() snapshot.Limits {
	limits := snapshot.DefaultLimits()
	if rr.config.MaxSnapshotArchive > 0 {
		limits.MaxArchiveBytes = rr.config.MaxSnapshotArchive
	}
	if rr.config.MaxSnapshotLogical > 0 {
		limits.MaxLogicalBytes = rr.config.MaxSnapshotLogical
		limits.MaxFileBytes = rr.config.MaxSnapshotLogical
	}
	if rr.config.MaxSnapshotEntries > 0 {
		limits.MaxEntries = rr.config.MaxSnapshotEntries
	}
	return limits
}

func (rr *RemoteRunner) shutdown() {
	rr.shutdownOnce.Do(func() {
		rr.cancelActive()
		rr.cancel()
		close(rr.shutdownChan)
	})
}

func hasDocker() bool {
	// Check if Docker daemon is available
	err := exec.Command("docker", "version").Run()
	return err == nil
}
