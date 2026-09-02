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
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/pipeline"
	runnerpkg "github.com/nhatminh06/forgeci/internal/runner"
	"github.com/nhatminh06/forgeci/internal/runworkspace"
	"github.com/nhatminh06/forgeci/internal/snapshot"
)

const protocolVersion = 1

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
}

type RemoteRunner struct {
	id              string
	config          Config
	httpClient      *http.Client
	currentLeaseID  string
	currentRunID    string
	currentPipeline []byte
	currentParallel int
	currentSnapshot snapshot.Metadata
	leaseExpiresAt  time.Time
	activeCancel    context.CancelFunc
	generation      int
	leaseMutex      sync.Mutex
	shutdownOnce    sync.Once
	shutdownChan    chan struct{}
	workers         sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
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
	for _, worker := range []func(){rr.heartbeatLoop, rr.leaseDeadlineLoop, rr.workLoop, rr.executionLoop} {
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
			rr.leaseMutex.Lock()
			expired := rr.currentRunID != "" && !rr.leaseExpiresAt.IsZero() && !time.Now().Before(rr.leaseExpiresAt)
			rr.leaseMutex.Unlock()
			if expired {
				rr.cancelActive()
			}
		case <-rr.shutdownChan:
			return
		}
	}
}

func (rr *RemoteRunner) sendHeartbeat() {
	rr.leaseMutex.Lock()
	leaseID := rr.currentLeaseID
	runID := rr.currentRunID
	generation := rr.generation
	rr.leaseMutex.Unlock()

	var leaseIDPtr *string
	if leaseID != "" {
		leaseIDPtr = &leaseID
	}

	reqData := map[string]interface{}{
		"runner_id":       rr.id,
		"active_lease_id": leaseIDPtr,
		"generation":      generation,
	}
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
			LeaseValid      bool       `json:"lease_valid"`
			CancelRequested bool       `json:"cancel_requested"`
			ServerShutdown  bool       `json:"server_shutdown"`
			LeaseExpiresAt  *time.Time `json:"lease_expires_at"`
		}
		if json.NewDecoder(resp.Body).Decode(&hb) != nil {
			rr.cancelActive()
			return
		}
		cancelReq, leaseValid := hb.CancelRequested, hb.LeaseValid
		if hb.LeaseExpiresAt != nil {
			rr.leaseMutex.Lock()
			rr.leaseExpiresAt = *hb.LeaseExpiresAt
			rr.leaseMutex.Unlock()
		}
		if runID != "" && (cancelReq || !leaseValid) {
			rr.cancelActive()
		}
		if hb.ServerShutdown {
			rr.cancelActive()
		}
	}
	rr.leaseMutex.Lock()
	expired := rr.currentRunID != "" && !rr.leaseExpiresAt.IsZero() && !time.Now().Before(rr.leaseExpiresAt)
	rr.leaseMutex.Unlock()
	if expired {
		rr.cancelActive()
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
	if rr.currentRunID != "" {
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

	var lease struct {
		RunID                string    `json:"run_id"`
		LeaseID              string    `json:"lease_id"`
		Generation           int       `json:"generation"`
		PipelineYAML         []byte    `json:"pipeline_yaml"`
		EffectiveParallel    int       `json:"effective_parallel"`
		ExpiresAt            time.Time `json:"expires_at"`
		SourceSnapshotSHA256 string    `json:"source_snapshot_sha256"`
		BlobSHA256           string    `json:"blob_sha256"`
		ArchiveSizeBytes     int64     `json:"archive_size_bytes"`
		LogicalSizeBytes     int64     `json:"logical_size_bytes"`
		EntryCount           int       `json:"entry_count"`
		ArchiveFormat        string    `json:"archive_format"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&lease); err != nil {
		return
	}
	if lease.RunID == "" {
		return
	}
	if lease.ArchiveFormat != snapshot.Format || lease.SourceSnapshotSHA256 == "" || lease.BlobSHA256 == "" || lease.ArchiveSizeBytes < 1 || lease.EntryCount < 0 || lease.LogicalSizeBytes < 0 {
		message := "invalid snapshot lease metadata"
		_ = rr.sendComplete(lease.RunID, lease.LeaseID, lease.Generation, "ERROR", &message)
		return
	}
	limits := rr.snapshotLimits()
	if lease.ArchiveSizeBytes > limits.MaxArchiveBytes || lease.LogicalSizeBytes > limits.MaxLogicalBytes || lease.EntryCount > limits.MaxEntries {
		message := "snapshot exceeds runner limits"
		_ = rr.sendComplete(lease.RunID, lease.LeaseID, lease.Generation, "ERROR", &message)
		return
	}

	rr.leaseMutex.Lock()
	rr.currentRunID = lease.RunID
	rr.currentLeaseID = lease.LeaseID
	rr.generation = lease.Generation
	rr.currentPipeline = append([]byte(nil), lease.PipelineYAML...)
	rr.currentParallel = lease.EffectiveParallel
	rr.leaseExpiresAt = lease.ExpiresAt
	rr.currentSnapshot = snapshot.Metadata{SourceDigest: lease.SourceSnapshotSHA256, BlobDigest: lease.BlobSHA256, Format: lease.ArchiveFormat, ArchiveSizeBytes: lease.ArchiveSizeBytes, LogicalSizeBytes: lease.LogicalSizeBytes, EntryCount: lease.EntryCount}
	rr.leaseMutex.Unlock()
}

func (rr *RemoteRunner) executionLoop() {
	for {
		select {
		case <-rr.shutdownChan:
			return
		default:
			rr.processCurrentRun()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (rr *RemoteRunner) processCurrentRun() {
	rr.leaseMutex.Lock()
	runID := rr.currentRunID
	leaseID := rr.currentLeaseID
	generation := rr.generation
	pipelineYAML := append([]byte(nil), rr.currentPipeline...)
	snapshotMeta := rr.currentSnapshot
	rr.leaseMutex.Unlock()

	if runID == "" || leaseID == "" || len(pipelineYAML) == 0 {
		return
	}
	executionCtx, cancel := context.WithCancel(rr.ctx)
	rr.leaseMutex.Lock()
	rr.activeCancel = cancel
	parallel := rr.currentParallel
	rr.leaseMutex.Unlock()
	defer cancel()
	marker := runworkspace.Marker{RunID: runID, LeaseID: leaseID, Generation: generation, SourceDigest: snapshotMeta.SourceDigest}
	workspace, err := runworkspace.Create(rr.config.WorkspaceRoot, marker)
	if err != nil {
		rr.completeError(runID, leaseID, generation, err)
		return
	}
	defer func() {
		if err := runworkspace.Remove(rr.config.WorkspaceRoot, workspace, marker); err != nil {
			fmt.Fprintf(os.Stderr, "workspace cleanup failed: %v\n", err)
		}
	}()
	sourceDir := filepath.Join(workspace, "source")
	if err := os.Mkdir(sourceDir, 0700); err != nil {
		rr.completeError(runID, leaseID, generation, err)
		return
	}
	archive, err := os.CreateTemp(workspace, ".source-*.tar.gz")
	if err != nil {
		rr.completeError(runID, leaseID, generation, err)
		return
	}
	archivePath := archive.Name()
	if err := rr.downloadSnapshot(executionCtx, runID, leaseID, generation, snapshotMeta, archive); err != nil {
		archive.Close()
		os.Remove(archivePath)
		rr.completeError(runID, leaseID, generation, err)
		return
	}
	if err := archive.Close(); err != nil {
		os.Remove(archivePath)
		rr.completeError(runID, leaseID, generation, err)
		return
	}
	if err := snapshot.Extract(archivePath, sourceDir, snapshotMeta, rr.snapshotLimits()); err != nil {
		os.Remove(archivePath)
		rr.completeError(runID, leaseID, generation, err)
		return
	}
	if err := os.Remove(archivePath); err != nil {
		rr.completeError(runID, leaseID, generation, err)
		return
	}

	cfg, err := config.ParseBytes(pipelineYAML, fmt.Sprintf("remote-runner:%s", runID))
	if err != nil {
		message := err.Error()
		_ = rr.sendComplete(runID, leaseID, generation, "ERROR", &message)
		rr.clearLease()
		return
	}

	graph, err := pipeline.Compile(cfg)
	if err != nil {
		message := err.Error()
		_ = rr.sendComplete(runID, leaseID, generation, "ERROR", &message)
		rr.clearLease()
		return
	}

	if parallel < 1 || parallel > rr.config.MaxParallel {
		parallel = rr.config.MaxParallel
	}
	local := executor.Local{Directory: sourceDir}
	var docker *executor.Docker
	for _, job := range cfg.Jobs {
		if job.Image != nil {
			docker, err = executor.NewDocker(sourceDir)
			break
		}
	}
	if err != nil {
		message := err.Error()
		_ = rr.sendComplete(runID, leaseID, generation, "ERROR", &message)
		rr.clearLease()
		return
	}
	jobObserver := remoteJobObserver{runner: rr, runID: runID, leaseID: leaseID, generation: generation}
	r := runnerpkg.Runner{
		Executor:    executor.Job{Local: local, Docker: docker},
		Output:      os.Stdout,
		ErrorOutput: os.Stderr,
		MaxParallel: parallel,
		Observer:    jobObserver,
	}
	result := r.Run(executionCtx, graph)

	status := "FAILED"
	switch {
	case result.Interrupted:
		status = "CANCELED"
	case result.InternalError:
		status = "ERROR"
	case result.Succeeded():
		status = "PASSED"
	}
	_ = rr.sendComplete(runID, leaseID, generation, status, nil)

	rr.clearLease()
}

func (rr *RemoteRunner) cancelActive() {
	rr.leaseMutex.Lock()
	cancel := rr.activeCancel
	rr.leaseMutex.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (rr *RemoteRunner) clearLease() {
	rr.leaseMutex.Lock()
	rr.currentRunID = ""
	rr.currentLeaseID = ""
	rr.generation = 0
	rr.currentPipeline = nil
	rr.currentParallel = 0
	rr.currentSnapshot = snapshot.Metadata{}
	rr.leaseExpiresAt = time.Time{}
	rr.activeCancel = nil
	rr.leaseMutex.Unlock()
}

func (rr *RemoteRunner) completeError(runID, leaseID string, generation int, err error) {
	message := err.Error()
	_ = rr.sendComplete(runID, leaseID, generation, "ERROR", &message)
	rr.clearLease()
}

func (rr *RemoteRunner) downloadSnapshot(ctx context.Context, runID, leaseID string, generation int, meta snapshot.Metadata, destination io.Writer) error {
	url := fmt.Sprintf("%s/v1/runner/leases/%s/source?runner_id=%s&run_id=%s&generation=%d", rr.config.ServerAddr, leaseID, rr.id, runID, generation)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
		return fmt.Errorf("download source snapshot: %d: %s", resp.StatusCode, string(body))
	}
	if resp.Header.Get("Content-Type") != "application/vnd.forgeci.source+gzip" {
		return fmt.Errorf("unexpected snapshot content type")
	}
	if value := resp.Header.Get("Content-Length"); value != "" {
		size, err := strconv.ParseInt(value, 10, 64)
		if err != nil || size != meta.ArchiveSizeBytes {
			return fmt.Errorf("snapshot content length mismatch")
		}
	}
	return snapshot.CopyVerified(destination, resp.Body, meta.ArchiveSizeBytes, meta.BlobDigest, rr.snapshotLimits().MaxArchiveBytes)
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

type remoteJobObserver struct {
	runner     *RemoteRunner
	runID      string
	leaseID    string
	generation int
}

func (o remoteJobObserver) OnJobState(name string, oldState, newState runnerpkg.State) {
	if newState == runnerpkg.Pending {
		return
	}
	status := map[string]string{
		string(runnerpkg.Running):  "RUNNING",
		string(runnerpkg.Passed):   "PASSED",
		string(runnerpkg.Failed):   "FAILED",
		string(runnerpkg.Blocked):  "BLOCKED",
		string(runnerpkg.Canceled): "CANCELED",
	}[string(newState)]
	if status == "" {
		return
	}
	_ = o.runner.sendJobEvent(o.runID, o.leaseID, o.generation, name, status)
}

func (rr *RemoteRunner) sendJobEvent(runID, leaseID string, generation int, jobName, status string) error {
	payload := map[string]any{
		"runner_id":  rr.id,
		"run_id":     runID,
		"lease_id":   leaseID,
		"generation": generation,
		"job_name":   jobName,
		"status":     status,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, rr.config.ServerAddr+fmt.Sprintf("/v1/runner/leases/%s/events", leaseID), bytes.NewReader(body))
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
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("job event failed: %d: %s", resp.StatusCode, string(payload))
	}
	return nil
}

func (rr *RemoteRunner) sendComplete(runID, leaseID string, generation int, status string, message *string) error {
	payload := map[string]any{
		"runner_id":  rr.id,
		"run_id":     runID,
		"lease_id":   leaseID,
		"generation": generation,
		"status":     status,
	}
	if message != nil {
		payload["error"] = *message
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, rr.config.ServerAddr+fmt.Sprintf("/v1/runner/leases/%s/complete", leaseID), bytes.NewReader(body))
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
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("complete failed: %d: %s", resp.StatusCode, string(payload))
	}
	return nil
}

func hasDocker() bool {
	// Check if Docker daemon is available
	err := exec.Command("docker", "version").Run()
	return err == nil
}
