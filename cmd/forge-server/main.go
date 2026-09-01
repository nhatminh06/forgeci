package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nhatminh06/forgeci/internal/api"
	"github.com/nhatminh06/forgeci/internal/controlplane"
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
	if err := validateLoopback(*listen); err != nil {
		return err
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
	if err := manager.Start(ctx); err != nil {
		return err
	}
	defer manager.Close()
	server := &http.Server{Addr: *listen, Handler: (api.Server{Manager: manager}).Handler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
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
