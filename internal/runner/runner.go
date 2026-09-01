package runner

import (
	"context"
	"fmt"
	"io"

	"github.com/forgeci/forgeci/internal/executor"
	"github.com/forgeci/forgeci/internal/pipeline"
)

type State string

const (
	Pending State = "PENDING"
	Running State = "RUNNING"
	Passed  State = "PASSED"
	Failed  State = "FAILED"
	Blocked State = "BLOCKED"
)

type CommandExecutor interface {
	Run(context.Context, string) executor.Result
}

type Result struct {
	States      map[string]State
	Interrupted bool
}

func (r Result) Succeeded() bool {
	if r.Interrupted {
		return false
	}
	for _, state := range r.States {
		if state != Passed {
			return false
		}
	}
	return true
}

type Runner struct {
	Executor CommandExecutor
	Output   io.Writer
}

func (r Runner) Run(ctx context.Context, graph *pipeline.Graph) Result {
	result := Result{States: make(map[string]State, len(graph.Nodes))}
	for name := range graph.Nodes {
		result.States[name] = Pending
	}
	for _, name := range graph.Order {
		if ctx.Err() != nil {
			result.Interrupted = true
			break
		}
		node := graph.Nodes[name]
		blocked := false
		for _, dependency := range node.Needs {
			if result.States[dependency] != Passed {
				blocked = true
				break
			}
		}
		if blocked {
			result.States[name] = Blocked
			fmt.Fprintf(r.Output, "[%s] blocked by failed dependency\n\n", name)
			continue
		}

		result.States[name] = Running
		fmt.Fprintf(r.Output, "[%s]\n", name)
		for _, step := range node.Job.Steps {
			fmt.Fprintf(r.Output, "$ %s\n", step.Run)
			execution := r.Executor.Run(ctx, step.Run)
			if execution.Err != nil {
				result.States[name] = Failed
				if ctx.Err() != nil {
					result.Interrupted = true
					fmt.Fprintf(r.Output, "x %s interrupted\n\n", name)
				} else if execution.ExitCode >= 0 {
					fmt.Fprintf(r.Output, "x %s (exit %d)\n\n", name, execution.ExitCode)
				} else {
					fmt.Fprintf(r.Output, "x %s (%v)\n\n", name, execution.Err)
				}
				break
			}
		}
		if result.States[name] == Running {
			result.States[name] = Passed
			fmt.Fprintf(r.Output, "✓ %s\n\n", name)
		}
	}
	return result
}

func PrintSummary(w io.Writer, graph *pipeline.Graph, result Result) {
	fmt.Fprintln(w, "Pipeline summary")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "JOB\tSTATUS")
	for _, name := range graph.Order {
		fmt.Fprintf(w, "%s\t%s\n", name, result.States[name])
	}
	fmt.Fprintln(w)
	if result.Succeeded() {
		fmt.Fprintln(w, "Pipeline succeeded")
	} else {
		fmt.Fprintln(w, "Pipeline failed")
	}
}
