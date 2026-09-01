package runner

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	local := executor.Local{Directory: dir, Stdout: &output, Stderr: &output}
	graph := graphFor(t, map[string]config.Job{
		"test": {Steps: []config.Step{{Run: "echo one"}, {Run: "echo two"}}},
	})
	result := (Runner{Executor: local, Output: &output}).Run(context.Background(), graph)
	if !result.Succeeded() || result.States["test"] != Passed {
		t.Fatalf("result = %+v", result)
	}
	if text := output.String(); !strings.Contains(text, "one") || !strings.Contains(text, "two") {
		t.Fatalf("output = %q", text)
	}
}

func TestRunFailureBlocksDependentsAndContinuesIndependentJobs(t *testing.T) {
	dir := t.TempDir()
	var output bytes.Buffer
	local := executor.Local{Directory: dir, Stdout: &output, Stderr: &output}
	graph := graphFor(t, map[string]config.Job{
		"build":  {Steps: []config.Step{{Run: "exit 9"}, {Run: "touch " + filepath.Join(dir, "remaining")}}},
		"test":   {Needs: []string{"build"}, Steps: []config.Step{{Run: "touch " + filepath.Join(dir, "blocked")}}},
		"deploy": {Needs: []string{"test"}, Steps: []config.Step{{Run: "touch " + filepath.Join(dir, "transitive")}}},
		"lint":   {Steps: []config.Step{{Run: "touch " + filepath.Join(dir, "independent")}}},
	})
	result := (Runner{Executor: local, Output: &output}).Run(context.Background(), graph)
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

func (startupFailureExecutor) Run(context.Context, string) executor.Result {
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
