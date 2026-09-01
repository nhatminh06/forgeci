package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/executor"
	"github.com/nhatminh06/forgeci/internal/pipeline"
)

func graphFor(t *testing.T, jobs map[string]config.Job) *pipeline.Graph {
	t.Helper()
	graph, err := pipeline.Compile(&config.Pipeline{Version: 1, Jobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	return graph
}

func TestRunSuccessAndMultipleSteps(t *testing.T) {
	dir := t.TempDir()
	var output bytes.Buffer
	local := executor.Local{Directory: dir}
	graph := graphFor(t, map[string]config.Job{
		"test": {Steps: []config.Step{{Run: "echo one"}, {Run: "echo two"}}},
	})
	result := (Runner{Executor: local, Output: &output, ErrorOutput: &output}).Run(context.Background(), graph)
	if !result.Succeeded() || result.States["test"] != Passed {
		t.Fatalf("result = %+v", result)
	}
	if text := output.String(); !strings.Contains(text, "one") || !strings.Contains(text, "two") {
		t.Fatalf("output = %q", text)
	}
}

type recordingObserver struct {
	mu     sync.Mutex
	events []string
}

func (o *recordingObserver) OnJobState(name string, oldState, newState State) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, fmt.Sprintf("%s:%s>%s", name, oldState, newState))
}

func TestRunPublishesLiveJobTransitions(t *testing.T) {
	observer := &recordingObserver{}
	graph := graphFor(t, map[string]config.Job{"test": {Steps: []config.Step{{Run: "true"}}}})
	result := (Runner{Executor: executor.Local{Directory: t.TempDir()}, Observer: observer}).Run(context.Background(), graph)
	if !result.Succeeded() || !reflect.DeepEqual(observer.events, []string{"test:PENDING>RUNNING", "test:RUNNING>PASSED"}) {
		t.Fatalf("result=%+v events=%v", result, observer.events)
	}
}

func TestRunFailureBlocksDependentsAndContinuesIndependentJobs(t *testing.T) {
	dir := t.TempDir()
	var output bytes.Buffer
	local := executor.Local{Directory: dir}
	graph := graphFor(t, map[string]config.Job{
		"build":  {Steps: []config.Step{{Run: "exit 9"}, {Run: "touch " + filepath.Join(dir, "remaining")}}},
		"test":   {Needs: []string{"build"}, Steps: []config.Step{{Run: "touch " + filepath.Join(dir, "blocked")}}},
		"deploy": {Needs: []string{"test"}, Steps: []config.Step{{Run: "touch " + filepath.Join(dir, "transitive")}}},
		"lint":   {Steps: []config.Step{{Run: "touch " + filepath.Join(dir, "independent")}}},
	})
	result := (Runner{Executor: local, Output: &output, ErrorOutput: &output}).Run(context.Background(), graph)
	if result.Succeeded() || result.States["build"] != Failed || result.States["test"] != Blocked || result.States["deploy"] != Blocked || result.States["lint"] != Passed {
		t.Fatalf("states = %+v", result.States)
	}
	for _, name := range []string{"remaining", "blocked", "transitive"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s command unexpectedly executed", name)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "independent")); err != nil {
		t.Fatalf("independent command did not execute: %v", err)
	}
}

type startupFailureExecutor struct{}

func (startupFailureExecutor) Run(context.Context, string, io.Writer, io.Writer) executor.Result {
	return executor.Result{ExitCode: -1, Err: errors.New("start shell process: unavailable")}
}

func TestRunReportsInternalExecutionError(t *testing.T) {
	graph := graphFor(t, map[string]config.Job{
		"test": {Steps: []config.Step{{Run: "true"}}},
	})
	var output bytes.Buffer
	result := (Runner{Executor: startupFailureExecutor{}, Output: &output}).Run(context.Background(), graph)
	if !result.InternalError || result.States["test"] != Failed || result.Succeeded() {
		t.Fatalf("result = %+v, want failed internal execution error", result)
	}
}

type controlledExecutor struct {
	mu           sync.Mutex
	active, peak int
	started      []string
	starts       chan string
	releases     map[string]chan executor.Result
}

func newControlledExecutor(names ...string) *controlledExecutor {
	releases := make(map[string]chan executor.Result, len(names))
	for _, name := range names {
		releases[name] = make(chan executor.Result, 1)
	}
	return &controlledExecutor{starts: make(chan string, len(names)), releases: releases}
}

func (e *controlledExecutor) Run(ctx context.Context, command string, stdout, stderr io.Writer) executor.Result {
	e.mu.Lock()
	e.active++
	if e.active > e.peak {
		e.peak = e.active
	}
	e.started = append(e.started, command)
	e.mu.Unlock()
	e.starts <- command
	var result executor.Result
	select {
	case result = <-e.releases[command]:
	case <-ctx.Done():
		result = executor.Result{ExitCode: -1, Err: ctx.Err()}
	}
	e.mu.Lock()
	e.active--
	e.mu.Unlock()
	return result
}

func (e *controlledExecutor) release(name string, result executor.Result) { e.releases[name] <- result }
func (e *controlledExecutor) snapshot() ([]string, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.started...), e.peak
}

func awaitStart(t *testing.T, starts <-chan string) string {
	t.Helper()
	select {
	case name := <-starts:
		return name
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for job start")
		return ""
	}
}
func awaitResult(t *testing.T, results <-chan Result) Result {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pipeline result")
		return Result{}
	}
}
func runControlled(ctx context.Context, graph *pipeline.Graph, exec *controlledExecutor, max int, output io.Writer) <-chan Result {
	results := make(chan Result, 1)
	go func() {
		results <- (Runner{Executor: exec, Output: output, ErrorOutput: output, MaxParallel: max}).Run(ctx, graph)
	}()
	return results
}
func independentJobs(count int) map[string]config.Job {
	jobs := make(map[string]config.Job, count)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("job-%02d", i)
		jobs[name] = config.Job{Steps: []config.Step{{Run: name}}}
	}
	return jobs
}

func TestRunExecutesIndependentJobsConcurrently(t *testing.T) {
	exec := newControlledExecutor("alpha", "beta")
	graph := graphFor(t, map[string]config.Job{"alpha": {Steps: []config.Step{{Run: "alpha"}}}, "beta": {Steps: []config.Step{{Run: "beta"}}}})
	results := runControlled(context.Background(), graph, exec, 2, io.Discard)
	awaitStart(t, exec.starts)
	awaitStart(t, exec.starts)
	_, peak := exec.snapshot()
	if peak != 2 {
		t.Fatalf("peak concurrency = %d, want 2", peak)
	}
	exec.release("alpha", executor.Result{})
	exec.release("beta", executor.Result{})
	if result := awaitResult(t, results); !result.Succeeded() {
		t.Fatalf("result = %+v", result)
	}
}

func TestRunEnforcesConcurrencyBounds(t *testing.T) {
	for _, tc := range []struct{ jobs, limit, wantPeak int }{{10, 3, 3}, {4, 1, 1}, {2, 5, 2}} {
		t.Run(fmt.Sprintf("jobs=%d/limit=%d", tc.jobs, tc.limit), func(t *testing.T) {
			jobs := independentJobs(tc.jobs)
			names := make([]string, 0, tc.jobs)
			for name := range jobs {
				names = append(names, name)
			}
			exec := newControlledExecutor(names...)
			results := runControlled(context.Background(), graphFor(t, jobs), exec, tc.limit, io.Discard)
			running := make([]string, 0, tc.wantPeak)
			for i := 0; i < tc.wantPeak; i++ {
				running = append(running, awaitStart(t, exec.starts))
			}
			for completed := 0; completed < tc.jobs; completed++ {
				name := running[0]
				running = running[1:]
				exec.release(name, executor.Result{})
				if completed+len(running)+1 < tc.jobs {
					running = append(running, awaitStart(t, exec.starts))
				}
			}
			result := awaitResult(t, results)
			_, peak := exec.snapshot()
			if !result.Succeeded() || peak != tc.wantPeak {
				t.Fatalf("peak = %d, result = %+v; want peak %d", peak, result, tc.wantPeak)
			}
		})
	}
}

func TestRunAdmitsReadyJobsDeterministically(t *testing.T) {
	exec := newControlledExecutor("alpha", "beta", "charlie", "delta")
	jobs := map[string]config.Job{}
	for _, name := range []string{"delta", "charlie", "beta", "alpha"} {
		jobs[name] = config.Job{Steps: []config.Step{{Run: name}}}
	}
	var output bytes.Buffer
	results := runControlled(context.Background(), graphFor(t, jobs), exec, 2, &output)
	awaitStart(t, exec.starts)
	awaitStart(t, exec.starts)
	text := output.String()
	if !(strings.Index(text, "[alpha]") < strings.Index(text, "[beta]")) || strings.Contains(text, "[charlie]") {
		t.Fatalf("initial admissions = %q", text)
	}
	exec.release("alpha", executor.Result{})
	if got := awaitStart(t, exec.starts); got != "charlie" {
		t.Fatalf("next admitted = %q", got)
	}
	exec.release("beta", executor.Result{})
	exec.release("charlie", executor.Result{})
	if got := awaitStart(t, exec.starts); got != "delta" {
		t.Fatalf("next admitted = %q", got)
	}
	exec.release("delta", executor.Result{})
	if result := awaitResult(t, results); !result.Succeeded() {
		t.Fatalf("result = %+v", result)
	}
}

func TestRunGatesLinearDependency(t *testing.T) {
	exec := newControlledExecutor("A", "B")
	graph := graphFor(t, map[string]config.Job{"A": {Steps: []config.Step{{Run: "A"}}}, "B": {Needs: []string{"A"}, Steps: []config.Step{{Run: "B"}}}})
	var output bytes.Buffer
	results := runControlled(context.Background(), graph, exec, 2, &output)
	if got := awaitStart(t, exec.starts); got != "A" {
		t.Fatalf("started = %q", got)
	}
	if started, _ := exec.snapshot(); !reflect.DeepEqual(started, []string{"A"}) {
		t.Fatalf("started = %v", started)
	}
	if strings.Contains(output.String(), "[B]") {
		t.Fatalf("B admitted before A completed: %q", output.String())
	}
	exec.release("A", executor.Result{})
	if got := awaitStart(t, exec.starts); got != "B" {
		t.Fatalf("started = %q", got)
	}
	exec.release("B", executor.Result{})
	if result := awaitResult(t, results); !result.Succeeded() {
		t.Fatalf("result = %+v", result)
	}
}

func TestRunGatesFanOutAndFanIn(t *testing.T) {
	exec := newControlledExecutor("A", "B", "C", "D")
	graph := graphFor(t, map[string]config.Job{
		"A": {Steps: []config.Step{{Run: "A"}}}, "B": {Needs: []string{"A"}, Steps: []config.Step{{Run: "B"}}}, "C": {Needs: []string{"A"}, Steps: []config.Step{{Run: "C"}}}, "D": {Needs: []string{"B", "C"}, Steps: []config.Step{{Run: "D"}}}})
	results := runControlled(context.Background(), graph, exec, 2, io.Discard)
	awaitStart(t, exec.starts)
	if started, _ := exec.snapshot(); len(started) != 1 {
		t.Fatalf("before A completion = %v", started)
	}
	exec.release("A", executor.Result{})
	first, second := awaitStart(t, exec.starts), awaitStart(t, exec.starts)
	got := map[string]bool{first: true, second: true}
	if !got["B"] || !got["C"] {
		t.Fatalf("fan-out = %v", got)
	}
	exec.release("B", executor.Result{})
	if started, _ := exec.snapshot(); len(started) != 3 {
		t.Fatalf("D started early: %v", started)
	}
	exec.release("C", executor.Result{})
	if got := awaitStart(t, exec.starts); got != "D" {
		t.Fatalf("started = %q", got)
	}
	exec.release("D", executor.Result{})
	if result := awaitResult(t, results); !result.Succeeded() {
		t.Fatalf("result=%+v", result)
	}
}

func TestRunPreservesIndependentBranchAfterFailure(t *testing.T) {
	exec := newControlledExecutor("A", "B", "C", "D", "E")
	graph := graphFor(t, map[string]config.Job{
		"A": {Steps: []config.Step{{Run: "A"}}}, "C": {Needs: []string{"A"}, Steps: []config.Step{{Run: "C"}}}, "E": {Needs: []string{"C"}, Steps: []config.Step{{Run: "E"}}}, "B": {Steps: []config.Step{{Run: "B"}}}, "D": {Needs: []string{"B"}, Steps: []config.Step{{Run: "D"}}}})
	results := runControlled(context.Background(), graph, exec, 2, io.Discard)
	awaitStart(t, exec.starts)
	awaitStart(t, exec.starts)
	exec.release("A", executor.Result{ExitCode: 9, Err: errors.New("exit 9")})
	exec.release("B", executor.Result{})
	if got := awaitStart(t, exec.starts); got != "D" {
		t.Fatalf("started=%q", got)
	}
	exec.release("D", executor.Result{})
	result := awaitResult(t, results)
	if result.States["A"] != Failed || result.States["B"] != Passed || result.States["C"] != Blocked || result.States["E"] != Blocked || result.States["D"] != Passed {
		t.Fatalf("states=%v", result.States)
	}
	if started, _ := exec.snapshot(); slices.Contains(started, "C") || slices.Contains(started, "E") {
		t.Fatalf("blocked jobs started: %v", started)
	}
}

func TestRunCancellationStopsAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	exec := newControlledExecutor("A", "B", "C")
	graph := graphFor(t, map[string]config.Job{"A": {Steps: []config.Step{{Run: "A"}}}, "B": {Steps: []config.Step{{Run: "B"}}}, "C": {Steps: []config.Step{{Run: "C"}}}})
	results := runControlled(ctx, graph, exec, 2, io.Discard)
	awaitStart(t, exec.starts)
	awaitStart(t, exec.starts)
	cancel()
	result := awaitResult(t, results)
	if !result.Interrupted || result.Succeeded() || result.States["A"] != Canceled || result.States["B"] != Canceled || result.States["C"] != Canceled {
		t.Fatalf("result=%+v", result)
	}
	if started, _ := exec.snapshot(); slices.Contains(started, "C") {
		t.Fatalf("C started: %v", started)
	}
}

type recordingExecutor struct {
	mu       sync.Mutex
	commands []string
}

type outputExecutor struct{}

func (outputExecutor) Run(_ context.Context, command string, stdout, stderr io.Writer) executor.Result {
	fmt.Fprintln(stdout, "stdout-"+command)
	fmt.Fprintln(stderr, "stderr-"+command)
	return executor.Result{}
}

func TestRunSynchronizesConcurrentOutput(t *testing.T) {
	jobs := independentJobs(20)
	var output bytes.Buffer
	result := (Runner{Executor: outputExecutor{}, Output: &output, ErrorOutput: &output, MaxParallel: 5}).Run(context.Background(), graphFor(t, jobs))
	if !result.Succeeded() {
		t.Fatalf("result = %+v", result)
	}
	for name := range jobs {
		if !strings.Contains(output.String(), "stdout-"+name) || !strings.Contains(output.String(), "stderr-"+name) {
			t.Fatalf("missing output for %s", name)
		}
	}
}

func (e *recordingExecutor) Run(_ context.Context, command string, stdout, stderr io.Writer) executor.Result {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.commands = append(e.commands, command)
	return executor.Result{}
}
func TestRunKeepsStepsSequentialWithinJob(t *testing.T) {
	exec := &recordingExecutor{}
	graph := graphFor(t, map[string]config.Job{"job": {Steps: []config.Step{{Run: "one"}, {Run: "two"}, {Run: "three"}}}})
	result := (Runner{Executor: exec, MaxParallel: 3}).Run(context.Background(), graph)
	if !result.Succeeded() || !reflect.DeepEqual(exec.commands, []string{"one", "two", "three"}) {
		t.Fatalf("commands=%v result=%+v", exec.commands, result)
	}
}
