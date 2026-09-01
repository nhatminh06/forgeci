package runner

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/forgeci/forgeci/internal/config"
	"github.com/forgeci/forgeci/internal/executor"
	"github.com/forgeci/forgeci/internal/pipeline"
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
		"build": {Steps: []config.Step{{Run: "exit 9"}, {Run: "touch " + filepath.Join(dir, "remaining")}}},
		"test":  {Needs: []string{"build"}, Steps: []config.Step{{Run: "touch " + filepath.Join(dir, "blocked")}}},
		"lint":  {Steps: []config.Step{{Run: "touch " + filepath.Join(dir, "independent")}}},
	})
	result := (Runner{Executor: local, Output: &output}).Run(context.Background(), graph)
	if result.Succeeded() || result.States["build"] != Failed || result.States["test"] != Blocked || result.States["lint"] != Passed {
		t.Fatalf("states = %+v", result.States)
	}
	for _, name := range []string{"remaining", "blocked"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s command unexpectedly executed", name)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "independent")); err != nil {
		t.Fatalf("independent command did not execute: %v", err)
	}
}
