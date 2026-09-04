package executor

import (
	"context"
	"fmt"
	"io"

	"github.com/nhatminh06/forgeci/internal/config"
)

// Job routes an entire job to exactly one execution environment.
type Job struct {
	Local  CommandRunner
	Docker *Docker
}

type CommandRunner interface {
	Run(context.Context, string, io.Writer, io.Writer) Result
}

// Run preserves the command-executor contract for runner compatibility. Job
// execution itself uses RunJob so routing is decided once per job.
func (e Job) Run(ctx context.Context, command string, stdout, stderr io.Writer) Result {
	return e.Local.Run(ctx, command, stdout, stderr)
}

func (e Job) RunJob(ctx context.Context, name string, job config.Job, stdout, stderr io.Writer) Result {
	if job.Image != nil {
		if e.Docker == nil {
			return Result{ExitCode: -1, Err: fmt.Errorf("Docker executor is unavailable")}
		}
		return e.Docker.RunJob(ctx, name, job, stdout, stderr)
	}
	for _, step := range job.Steps {
		result := e.Local.Run(ctx, step.Run, stdout, stderr)
		if result.Err != nil {
			return result
		}
	}
	return Result{ExitCode: 0}
}
