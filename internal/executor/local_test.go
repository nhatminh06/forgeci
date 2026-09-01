package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalRunStreamsOutput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	result := (Local{Directory: t.TempDir()}).Run(
		context.Background(), "echo visible; echo warning >&2", &stdout, &stderr,
	)
	if result.Err != nil || result.ExitCode != 0 {
		t.Fatalf("Run() = %+v", result)
	}
	if !strings.Contains(stdout.String(), "visible") || !strings.Contains(stderr.String(), "warning") {
		t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestLocalRunUsesConfiguredDirectory(t *testing.T) {
	directory := t.TempDir()
	var stdout bytes.Buffer
	result := (Local{Directory: directory}).Run(context.Background(), "pwd", &stdout, io.Discard)
	if result.Err != nil {
		t.Fatalf("Run() error = %v", result.Err)
	}
	if got := strings.TrimSpace(stdout.String()); got != directory {
		t.Fatalf("pwd = %q, want %q", got, directory)
	}
}

func TestLocalRunDistinguishesStartupFailure(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "missing")
	result := (Local{Directory: directory}).Run(context.Background(), "exit 7", io.Discard, io.Discard)
	if result.Err == nil || result.ExitCode != -1 {
		t.Fatalf("Run() = %+v, want startup failure with exit code -1", result)
	}
	var pathErr *os.PathError
	if !strings.Contains(result.Err.Error(), "start shell process") || !errors.As(result.Err, &pathErr) {
		t.Fatalf("Run() error = %v, want wrapped process-start path error", result.Err)
	}
}

func TestLocalRunReturnsExitCode(t *testing.T) {
	result := (Local{Directory: t.TempDir()}).Run(context.Background(), "exit 7", io.Discard, io.Discard)
	if result.Err == nil || result.ExitCode != 7 {
		t.Fatalf("Run() = %+v, want exit code 7", result)
	}
}
