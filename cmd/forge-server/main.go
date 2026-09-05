package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nhatminh06/forgeci/internal/api"
	"github.com/nhatminh06/forgeci/internal/artifact"
	"github.com/nhatminh06/forgeci/internal/cache"
	"github.com/nhatminh06/forgeci/internal/controlplane"
	"github.com/nhatminh06/forgeci/internal/runnerproto"
	"github.com/nhatminh06/forgeci/internal/snapshot"
	"github.com/nhatminh06/forgeci/internal/store/postgres"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
func run() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	flags := flag.NewFlagSet("forge-server", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:8080", "loopback listen address")
	workspace := flags.String("workspace", cwd, "repository workspace")
	snapshotDir := flags.String("snapshot-dir", "", "source snapshot store directory (required, outside workspace)")
	artifactDir := flags.String("artifact-dir", "", "durable artifact store directory (required, outside workspace)")
	cacheDir := flags.String("cache-dir", "", "reusable build cache store directory (required, outside workspace)")
	cacheRetention := flags.Duration("cache-retention", 168*time.Hour, "idle cache retention")
	cacheMaxBytes := flags.Int64("cache-max-bytes", 10<<30, "maximum unique live cache blob bytes")
	cacheGCInterval := flags.Duration("cache-gc-interval", time.Minute, "cache garbage collection interval")
	cacheOrphanGrace := flags.Duration("cache-orphan-grace", time.Hour, "minimum age before removing orphan cache blobs")
	artifactRetention := flags.Duration("artifact-retention", 168*time.Hour, "artifact retention after a run becomes terminal")
	artifactGCInterval := flags.Duration("artifact-gc-interval", time.Minute, "artifact garbage collection interval")
	artifactOrphanGrace := flags.Duration("artifact-orphan-grace", time.Hour, "minimum age before removing orphan artifact blobs")
	maxArtifactEntries := flags.Int("artifact-max-entries", 100000, "maximum artifact entries")
	maxArtifactLogical := flags.Int64("artifact-max-logical-bytes", 1<<30, "maximum logical artifact bytes")
	maxArtifactArchive := flags.Int64("artifact-max-archive-bytes", 512<<20, "maximum compressed artifact bytes")
	maxSnapshotEntries := flags.Int("snapshot-max-entries", 100000, "maximum source snapshot entries")
	maxSnapshotLogical := flags.Int64("snapshot-max-logical-bytes", 1<<30, "maximum logical source bytes")
	maxSnapshotArchive := flags.Int64("snapshot-max-archive-bytes", 512<<20, "maximum compressed snapshot bytes")
	databaseURL := flags.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL")
	executionMode := flags.String("execution-mode", "local", "execution mode: local or remote")
	runnerListen := flags.String("runner-listen", "127.0.0.1:9090", "runner protocol listener address")
	runnerTokenFile := flags.String("runner-token-file", "", "file containing the runner bearer token")
	runnerTLSCert := flags.String("runner-tls-cert", "", "runner listener TLS certificate")
	runnerTLSKey := flags.String("runner-tls-key", "", "runner listener TLS private key")
	githubWebhookSecretFile := flags.String("github-webhook-secret-file", "", "file containing the GitHub webhook secret")

	if err := flags.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	if *databaseURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if *snapshotDir == "" {
		return fmt.Errorf("snapshot directory is required")
	}
	if *artifactDir == "" {
		return fmt.Errorf("artifact directory is required")
	}
	if *cacheDir == "" {
		*cacheDir = filepath.Join(filepath.Dir(*artifactDir), "cache")
	}
	if *artifactRetention <= 0 || *artifactGCInterval <= 0 || *artifactOrphanGrace <= 0 {
		return fmt.Errorf("artifact retention and GC durations must be greater than zero")
	}
	if *cacheRetention <= 0 || *cacheGCInterval <= 0 || *cacheOrphanGrace <= 0 || *cacheMaxBytes < 0 {
		return fmt.Errorf("cache retention, GC durations, and size must be valid")
	}

	// Validate execution mode
	if *executionMode != "local" && *executionMode != "remote" {
		return fmt.Errorf("invalid execution-mode: %q (must be 'local' or 'remote')", *executionMode)
	}

	runnerToken := os.Getenv("FORGECI_RUNNER_TOKEN")
	if *runnerTokenFile != "" {
		data, err := os.ReadFile(*runnerTokenFile)
		if err != nil {
			return fmt.Errorf("read runner token file: %w", err)
		}
		runnerToken = strings.TrimSpace(string(data))
	}
	if *executionMode == "remote" && runnerToken == "" {
		return fmt.Errorf("runner token required for remote execution mode")
	}
	githubWebhookSecret, err := readSecretFile(*githubWebhookSecretFile)
	if err != nil {
		return fmt.Errorf("read GitHub webhook secret file: %w", err)
	}

	if err := validateLoopback(*listen); err != nil {
		return err
	}

	// Validate runner listener
	if *executionMode == "remote" {
		if err := validateRunnerListener(*runnerListen, *runnerTLSCert, *runnerTLSKey); err != nil {
			return err
		}
	}

	absolute, err := filepath.Abs(*workspace)
	if err != nil {
		return err
	}
	limits := snapshot.DefaultLimits()
	limits.MaxEntries = *maxSnapshotEntries
	limits.MaxLogicalBytes = *maxSnapshotLogical
	limits.MaxArchiveBytes = *maxSnapshotArchive
	if limits.MaxEntries < 1 || limits.MaxLogicalBytes < 1 || limits.MaxArchiveBytes < 1 {
		return fmt.Errorf("snapshot limits must be greater than zero")
	}
	snapshotStore, err := snapshot.Open(*snapshotDir, absolute, limits)
	if err != nil {
		return err
	}
	artifactLimits := artifact.DefaultLimits()
	artifactLimits.MaxEntries = *maxArtifactEntries
	artifactLimits.MaxLogicalBytes = *maxArtifactLogical
	artifactLimits.MaxFileBytes = *maxArtifactLogical
	artifactLimits.MaxArchiveBytes = *maxArtifactArchive
	artifactStore, err := artifact.Open(*artifactDir, artifactLimits)
	if err != nil {
		return err
	}
	cacheStore, err := cache.Open(*cacheDir, artifactLimits)
	if err != nil {
		return err
	}
	workspaceRoot, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return err
	}
	if pathsOverlap(workspaceRoot, artifactStore.Root()) || pathsOverlap(snapshotStore.Root(), artifactStore.Root()) || pathsOverlap(workspaceRoot, cacheStore.Root()) || pathsOverlap(snapshotStore.Root(), cacheStore.Root()) || pathsOverlap(artifactStore.Root(), cacheStore.Root()) {
		return fmt.Errorf("artifact directory must be separate from workspace and snapshot directory")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	persistence, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer persistence.Close()
	persistence.SetArtifactRetention(*artifactRetention)
	persistence.SetCacheRetention(*cacheRetention)
	persistence.SetCacheMaxBytes(*cacheMaxBytes)
	manager, err := controlplane.New(ctx, persistence, absolute, os.Stdout, snapshotStore)
	if err != nil {
		return err
	}
	manager.SetArtifactStore(artifactStore)
	manager.SetCacheStore(cacheStore)

	// Only start manager in local execution mode
	if *executionMode == "local" {
		if err := manager.Start(ctx); err != nil {
			return err
		}
	} else if err := persistence.RecoverRemoteJobs(ctx); err != nil {
		return fmt.Errorf("recover remote jobs: %w", err)
	}
	defer manager.Close()

	server := &http.Server{Addr: *listen, Handler: (api.Server{Manager: manager, Store: persistence, OpenArtifact: artifactStore.OpenBlob, Workspace: workspaceRoot, GitHubWebhookSecret: githubWebhookSecret}).Handler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	go func() {
		ticker := time.NewTicker(*artifactGCInterval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				digests, err := persistence.ExpireArtifacts(ctx, now.UTC())
				if err != nil {
					fmt.Fprintf(os.Stderr, "artifact GC metadata error: %v\n", err)
					continue
				}
				for _, digest := range digests {
					if err := artifactStore.RemoveBlob(digest); err != nil {
						fmt.Fprintf(os.Stderr, "artifact GC blob error: %v\n", err)
					}
				}
				live, err := persistence.LiveArtifactBlobs(ctx)
				if err == nil {
					_ = artifactStore.CleanupOrphans(now.UTC(), *artifactOrphanGrace, live)
				}
				_ = artifactStore.CleanupTemps(now.UTC(), *artifactOrphanGrace)
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(*cacheGCInterval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				digests, err := persistence.ExpireCache(ctx, now.UTC(), *cacheMaxBytes)
				if err == nil {
					for _, digest := range digests {
						_ = cacheStore.RemoveBlob(digest)
					}
					if live, liveErr := persistence.LiveCacheBlobs(ctx); liveErr == nil {
						_ = cacheStore.CleanupOrphans(now.UTC(), *cacheOrphanGrace, live)
					}
					_ = cacheStore.CleanupTemps(now.UTC(), *cacheOrphanGrace)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Setup runner protocol server in remote mode
	var runnerServer *http.Server
	if *executionMode == "remote" {
		handlers := runnerproto.NewHandlers(persistence, runnerToken, manager.WorkAvailable)
		handlers.SetSnapshotOpener(snapshotStore.OpenBlob)
		handlers.SetArtifactStore(artifactStore)
		handlers.SetCacheStore(cacheStore)
		handlers.SetNotifier(manager.Notify)

		// Start lease expiration sweeper
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if persistence.ExpireJobLeases(ctx, time.Now().UTC()) == nil {
						manager.Notify()
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		// Setup runner routes with auth
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/runner/register", handlers.AuthMiddleware(http.HandlerFunc(handlers.Register)).ServeHTTP)
		mux.HandleFunc("/v1/runner/heartbeat", handlers.AuthMiddleware(http.HandlerFunc(handlers.Heartbeat)).ServeHTTP)
		mux.HandleFunc("/v1/runner/lease", handlers.AuthMiddleware(http.HandlerFunc(handlers.Lease)).ServeHTTP)
		mux.HandleFunc("/v1/runner/leases/", handlers.AuthMiddleware(http.HandlerFunc(handlers.HandleLeaseRoute)).ServeHTTP)

		runnerServer = &http.Server{Addr: *runnerListen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		runnerServeErr := make(chan error, 1)
		go func() {
			if *runnerTLSCert != "" {
				runnerServeErr <- runnerServer.ListenAndServeTLS(*runnerTLSCert, *runnerTLSKey)
			} else {
				runnerServeErr <- runnerServer.ListenAndServe()
			}
		}()

		go func() {
			select {
			case err := <-runnerServeErr:
				if !errors.Is(err, http.ErrServerClosed) {
					serveErr <- err
				}
			case <-ctx.Done():
			}
		}()
	}

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		if runnerServer != nil {
			if err := runnerServer.Shutdown(shutdownCtx); err != nil {
				return err
			}
		}
		if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}

func readSecretFile(name string) ([]byte, error) {
	if name == "" {
		return nil, nil
	}
	info, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("permissions must not allow group or other access")
	}
	if info.Size() < 1 || info.Size() > 64<<10 {
		return nil, fmt.Errorf("invalid file size")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	secret := []byte(strings.TrimSpace(string(data)))
	if len(secret) == 0 {
		return nil, fmt.Errorf("secret is empty")
	}
	return secret, nil
}

func pathsOverlap(a, b string) bool {
	rel, err := filepath.Rel(a, b)
	if err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return true
	}
	rel, err = filepath.Rel(b, a)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func validateLoopback(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("listen address must use a loopback host")
	}
	return nil
}

func validateRunnerListener(address, certFile, keyFile string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid runner listener address: %w", err)
	}
	if (certFile == "") != (keyFile == "") {
		return fmt.Errorf("runner TLS certificate and key must be configured together")
	}
	if certFile != "" {
		if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
			return fmt.Errorf("load runner TLS certificate: %w", err)
		}
		return nil
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return nil
	}
	if ip == nil && host != "" {
		addresses, err := net.LookupIP(host)
		if err == nil && len(addresses) > 0 {
			all := true
			for _, address := range addresses {
				all = all && address.IsLoopback()
			}
			if all {
				return nil
			}
		}
	}
	return fmt.Errorf("non-loopback runner listener requires TLS")
}
