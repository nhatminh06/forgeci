package executor

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestLocalRunStreamsOutput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	result := (Local{Directory: t.TempDir(), Stdout: &stdout, Stderr: &stderr}).Run(
		context.Background(), "echo visible; echo warning >&2",
	)
	if result.Err != nil || result.ExitCode != 0 {
		t.Fatalf("Run() = %+v", result)
	}
	if !strings.Contains(stdout.String(), "visible") || !strings.Contains(stderr.String(), "warning") {
		t.Fatalf("stdout = %q, stderr = %q", stdout.String(), stderr.String())
	}
}

func TestLocalRunReturnsExitCode(t *testing.T) {
	result := (Local{Directory: t.TempDir()}).Run(context.Background(), "exit 7")
	if result.Err == nil || result.ExitCode != 7 {
		t.Fatalf("Run() = %+v, want exit code 7", result)
	}
}
