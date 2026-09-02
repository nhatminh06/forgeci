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
	"github.com/nhatminh06/forgeci/internal/controlplane"
	"github.com/nhatminh06/forgeci/internal/runnerproto"
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
	databaseURL := flags.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL")
	executionMode := flags.String("execution-mode", "local", "execution mode: local or remote")
	runnerListen := flags.String("runner-listen", "127.0.0.1:9090", "runner protocol listener address")
	runnerTokenFile := flags.String("runner-token-file", "", "file containing the runner bearer token")
	runnerTLSCert := flags.String("runner-tls-cert", "", "runner listener TLS certificate")
	runnerTLSKey := flags.String("runner-tls-key", "", "runner listener TLS private key")

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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	persistence, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer persistence.Close()
	manager, err := controlplane.New(ctx, persistence, absolute, os.Stdout)
	if err != nil {
		return err
	}

	// Only start manager in local execution mode
	if *executionMode == "local" {
		if err := manager.Start(ctx); err != nil {
			return err
		}
	}
	defer manager.Close()

	server := &http.Server{Addr: *listen, Handler: (api.Server{Manager: manager, Store: persistence}).Handler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()

	// Setup runner protocol server in remote mode
	var runnerServer *http.Server
	if *executionMode == "remote" {
		handlers := runnerproto.NewHandlers(persistence, runnerToken)

		// Start lease expiration sweeper
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					persistence.ExpireLeases(ctx, time.Now().UTC())
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
