package runner

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/pipeline"
)

type State string

const (
	Pending  State = "PENDING"
	Running  State = "RUNNING"
	Passed   State = "PASSED"
	Failed   State = "FAILED"
	Blocked  State = "BLOCKED"
	Canceled State = "CANCELED"
)

type CommandExecutor interface {
	Run(context.Context, string, io.Writer, io.Writer) executor.Result
}

type JobExecutor interface {
	RunJob(context.Context, string, config.Job, io.Writer, io.Writer) executor.Result
}

type JobStateObserver interface {
	OnJobState(string, State, State)
}

type Result struct {
	States        map[string]State
	Interrupted   bool
	InternalError bool
}

func (r Result) Succeeded() bool {
	if r.Interrupted || r.InternalError {
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
	Executor    CommandExecutor
	Output      io.Writer
	ErrorOutput io.Writer
	MaxParallel int
	Observer    JobStateObserver
}

type completion struct {
	name          string
	state         State
	internalError bool
}

func (r Runner) Run(ctx context.Context, graph *pipeline.Graph) Result {
	maxParallel := r.MaxParallel
	if maxParallel < 1 {
		maxParallel = 1
	}
	output := newSynchronizedOutput(r.Output, r.ErrorOutput)
	result := Result{States: make(map[string]State, len(graph.Nodes))}
	for name := range graph.Nodes {
		result.States[name] = Pending
	}

	completions := make(chan completion, len(graph.Nodes))
	active, terminal := 0, 0
	canceling := false
	for terminal < len(graph.Nodes) {
		if !canceling && ctx.Err() != nil {
			canceling = true
			result.Interrupted = true
		}

		if !canceling {
			for _, name := range graph.Order {
				if result.States[name] == Pending && hasFailedDependency(graph, result.States, name) {
					result.States[name] = Blocked
					r.observe(name, Pending, Blocked)
					terminal++
					fmt.Fprintf(output.stdout, "[%s] blocked by failed dependency\n\n", name)
				}
			}
			for active < maxParallel {
				name := nextReadyJob(graph, result.States)
				if name == "" {
					break
				}
				result.States[name] = Running
				r.observe(name, Pending, Running)
				active++
				fmt.Fprintf(output.stdout, "[%s]\n", name)
				go func(name string) {
					state, internalError := r.executeJob(ctx, graph.Nodes[name], output)
					completions <- completion{name: name, state: state, internalError: internalError}
				}(name)
			}
		}

		if active == 0 {
			if canceling {
				for _, name := range graph.Order {
					if result.States[name] == Pending {
						result.States[name] = Canceled
						r.observe(name, Pending, Canceled)
						terminal++
					}
				}
			}
			if terminal == len(graph.Nodes) {
				break
			}
			result.InternalError = true
			for _, name := range graph.Order {
				if result.States[name] == Pending {
					result.States[name] = Failed
					r.observe(name, Pending, Failed)
					terminal++
				}
			}
			break
		}

		var completed completion
		if canceling {
			completed = <-completions
		} else {
			select {
			case completed = <-completions:
			case <-ctx.Done():
				continue
			}
		}
		oldState := result.States[completed.name]
		result.States[completed.name] = completed.state
		r.observe(completed.name, oldState, completed.state)
		result.InternalError = result.InternalError || completed.internalError
		active--
		terminal++
	}
	return result
}

func (r Runner) observe(name string, oldState, newState State) {
	if r.Observer != nil {
		r.Observer.OnJobState(name, oldState, newState)
	}
}

func nextReadyJob(graph *pipeline.Graph, states map[string]State) string {
	for _, name := range graph.Order {
		if states[name] != Pending {
			continue
		}
		ready := true
		for _, dependency := range graph.Nodes[name].Needs {
			if states[dependency] != Passed {
				ready = false
				break
			}
		}
		if ready {
			return name
		}
	}
	return ""
}

func hasFailedDependency(graph *pipeline.Graph, states map[string]State, name string) bool {
	for _, dependency := range graph.Nodes[name].Needs {
		if states[dependency] == Failed || states[dependency] == Blocked {
			return true
		}
	}
	return false
}

func (r Runner) executeJob(ctx context.Context, node *pipeline.Node, output synchronizedOutput) (State, bool) {
	if jobExecutor, ok := r.Executor.(JobExecutor); ok {
		execution := jobExecutor.RunJob(ctx, node.Name, node.Job, output.stdout, output.stderr)
		return finishJob(ctx, node.Name, execution, output.stdout)
	}
	for _, step := range node.Job.Steps {
		fmt.Fprintf(output.stdout, "$ %s\n", step.Run)
		execution := r.Executor.Run(ctx, step.Run, output.stdout, output.stderr)
		if execution.Err == nil {
			continue
		}
		if ctx.Err() != nil {
			fmt.Fprintf(output.stdout, "x %s canceled\n\n", node.Name)
			return Canceled, false
		}
		if execution.ExitCode >= 0 {
			fmt.Fprintf(output.stdout, "x %s (exit %d)\n\n", node.Name, execution.ExitCode)
			return Failed, false
		}
		fmt.Fprintf(output.stdout, "x %s (%v)\n\n", node.Name, execution.Err)
		return Failed, true
	}
	fmt.Fprintf(output.stdout, "✓ %s\n\n", node.Name)
	return Passed, false
}

func finishJob(ctx context.Context, name string, execution executor.Result, output io.Writer) (State, bool) {
	if execution.Err == nil {
		fmt.Fprintf(output, "✓ %s\n\n", name)
		return Passed, false
	}
	if ctx.Err() != nil {
		fmt.Fprintf(output, "x %s canceled\n\n", name)
		return Canceled, false
	}
	if execution.ExitCode >= 0 {
		fmt.Fprintf(output, "x %s (exit %d)\n\n", name, execution.ExitCode)
		return Failed, false
	}
	fmt.Fprintf(output, "x %s (%v)\n\n", name, execution.Err)
	return Failed, true
}

type synchronizedOutput struct {
	stdout io.Writer
	stderr io.Writer
}
type lockedWriter struct {
	mu     *sync.Mutex
	target io.Writer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.target.Write(p)
}

func newSynchronizedOutput(stdout, stderr io.Writer) synchronizedOutput {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = stdout
	}
	mu := &sync.Mutex{}
	return synchronizedOutput{stdout: lockedWriter{mu: mu, target: stdout}, stderr: lockedWriter{mu: mu, target: stderr}}
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
