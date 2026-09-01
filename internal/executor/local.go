package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
)

type Result struct {
	ExitCode int
	Err      error
}

type Local struct {
	Directory string
	Stdout    io.Writer
	Stderr    io.Writer
}

func (l Local) Run(ctx context.Context, command string) Result {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Dir = l.Directory
	cmd.Stdout = l.Stdout
	cmd.Stderr = l.Stderr
	err := cmd.Run()
	if err == nil {
		return Result{ExitCode: 0}
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return Result{ExitCode: exitErr.ExitCode(), Err: err}
	}
	return Result{ExitCode: -1, Err: fmt.Errorf("start shell process: %w", err)}
}
