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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/pipeline"
	runnerpkg "github.com/nhatminh06/forgeci/internal/runner"
)

const protocolVersion = 1

type Config struct {
	ServerAddr  string
	ServerToken string
	Name        string
	Workspace   string
	StateDir    string
	MaxParallel int
	TLSCA       string
	Insecure    bool
}

type RemoteRunner struct {
	id              string
	config          Config
	httpClient      *http.Client
	currentLeaseID  string
	currentRunID    string
	currentPipeline []byte
	currentParallel int
	leaseExpiresAt  time.Time
	activeCancel    context.CancelFunc
	generation      int
	leaseMutex      sync.Mutex
	shutdownOnce    sync.Once
	shutdownChan    chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc
}

func main() {
	homeDir, _ := os.UserHomeDir()
	cfg := &Config{}
	flag.StringVar(&cfg.ServerAddr, "server", "http://localhost:9090", "Control plane runner listener address")
	flag.StringVar(&cfg.ServerToken, "token", os.Getenv("FORGECI_RUNNER_TOKEN"), "Bearer token for authentication")
	flag.StringVar(&cfg.Name, "name", hostname(), "Runner name (default: hostname)")
	flag.StringVar(&cfg.Workspace, "workspace", ".", "Pipeline workspace directory")
	flag.StringVar(&cfg.StateDir, "state-dir", filepath.Join(homeDir, ".forgeci", "runner"), "State directory for runner ID")
	flag.IntVar(&cfg.MaxParallel, "max-parallel", runtime.NumCPU(), "Maximum parallel jobs")
	flag.StringVar(&cfg.TLSCA, "ca-cert", "", "Path to CA certificate for TLS verification")
	flag.BoolVar(&cfg.Insecure, "insecure", false, "Skip TLS verification (development only)")
	flag.Parse()

	if cfg.ServerToken == "" {
		fmt.Fprintf(os.Stderr, "Error: runner token required (set --token or FORGECI_RUNNER_TOKEN)\n")
		os.Exit(1)
	}

	if cfg.MaxParallel < 1 {
		fmt.Fprintf(os.Stderr, "Error: max-parallel must be > 0\n")
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
	// Load or create runner identity
	id, err := loadOrCreateRunnerID(cfg.StateDir)
	if err != nil {
		return nil, fmt.Errorf("load runner identity: %w", err)
	}

	// Setup HTTP client
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	if cfg.TLSCA != "" || cfg.Insecure {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: cfg.Insecure,
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
	go rr.heartbeatLoop()
	go rr.leaseDeadlineLoop()

	// Start work acquisition loop
	go rr.workLoop()

	// Start execution loop
	go rr.executionLoop()

	// Wait for shutdown
	<-rr.shutdownChan
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
		RunID             string    `json:"run_id"`
		LeaseID           string    `json:"lease_id"`
		Generation        int       `json:"generation"`
		PipelineYAML      []byte    `json:"pipeline_yaml"`
		EffectiveParallel int       `json:"effective_parallel"`
		ExpiresAt         time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&lease); err != nil {
		return
	}
	if lease.RunID == "" {
		return
	}

	rr.leaseMutex.Lock()
	rr.currentRunID = lease.RunID
	rr.currentLeaseID = lease.LeaseID
	rr.generation = lease.Generation
	rr.currentPipeline = append([]byte(nil), lease.PipelineYAML...)
	rr.currentParallel = lease.EffectiveParallel
	rr.leaseExpiresAt = lease.ExpiresAt
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
	rr.leaseMutex.Unlock()

	if runID == "" || leaseID == "" || len(pipelineYAML) == 0 {
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

	executionCtx, cancel := context.WithCancel(rr.ctx)
	rr.leaseMutex.Lock()
	rr.activeCancel = cancel
	parallel := rr.currentParallel
	rr.leaseMutex.Unlock()
	defer cancel()
	if parallel < 1 || parallel > rr.config.MaxParallel {
		parallel = rr.config.MaxParallel
	}
	local := executor.Local{Directory: rr.config.Workspace}
	var docker *executor.Docker
	for _, job := range cfg.Jobs {
		if job.Image != nil {
			docker, err = executor.NewDocker(rr.config.Workspace)
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
	rr.leaseExpiresAt = time.Time{}
	rr.activeCancel = nil
	rr.leaseMutex.Unlock()
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
