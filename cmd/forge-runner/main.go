package main

import (
	"bytes"
	"context"
	"crypto/tls"
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
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
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
	id             string
	config         Config
	httpClient     *http.Client
	currentLeaseID string
	currentRunID   string
	generation     int
	leaseMutex     sync.Mutex
	shutdownOnce   sync.Once
	shutdownChan   chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
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
		return string(data), nil
	}

	// Create new
	id := uuid.New().String()

	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return "", err
	}

	if err := os.WriteFile(stateFile, []byte(id), 0600); err != nil {
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
		var hb map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&hb)
		if cancelReq, ok := hb["cancel_requested"].(bool); ok && cancelReq && runID != "" {
			// Signal cancellation - in full implementation would cancel execution
		}
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

	if resp.StatusCode == http.StatusOK {
		var lease map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&lease)
		if runID, ok := lease["run_id"].(string); ok {
			rr.leaseMutex.Lock()
			rr.currentRunID = runID
			if leaseID, ok := lease["lease_id"].(string); ok {
				rr.currentLeaseID = leaseID
			}
			if gen, ok := lease["generation"].(float64); ok {
				rr.generation = int(gen)
			}
			rr.leaseMutex.Unlock()
		}
	}
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
	rr.leaseMutex.Unlock()

	if runID == "" {
		return // No active run
	}

	// In full implementation, would execute pipeline here
	// For now, just a placeholder
}

func (rr *RemoteRunner) shutdown() {
	rr.shutdownOnce.Do(func() {
		rr.cancel()
		close(rr.shutdownChan)
	})
}

func hasDocker() bool {
	// Check if Docker daemon is available
	err := exec.Command("docker", "version").Run()
	return err == nil
}
