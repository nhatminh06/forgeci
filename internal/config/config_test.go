package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writePipeline(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "forge.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValidPipelines(t *testing.T) {
	tests := map[string]string{
		"single job": `version: 1
jobs:
  test:
    steps:
      - run: echo test
`,
		"independent and multiple steps": `version: 1
jobs:
  build:
    steps:
      - run: echo one
      - run: echo two
  lint:
    steps:
      - run: echo lint
`,
		"fan in and fan out": `version: 1
jobs:
  root:
    steps: [{run: echo root}]
  left:
    needs: [root]
    steps: [{run: echo left}]
  right:
    needs: [root]
    steps: [{run: echo right}]
  final:
    needs: [left, right]
    steps: [{run: echo final}]
`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writePipeline(t, body)); err != nil {
				t.Fatalf("Load() error = %v", err)
			}
		})
	}
}

func TestLoadInvalidPipelines(t *testing.T) {
	tests := []struct {
		name, body, want string
	}{
		{"malformed YAML", "version: [", "parse pipeline"},
		{"unsupported version", "version: 2\njobs:\n  a:\n    steps: [{run: ok}]\n", "unsupported version"},
		{"missing jobs", "version: 1\n", "jobs must contain"},
		{"empty jobs", "version: 1\njobs: {}\n", "jobs must contain"},
		{"invalid job name", "version: 1\njobs:\n  bad name:\n    steps: [{run: ok}]\n", "invalid job name"},
		{"no steps", "version: 1\njobs:\n  test: {}\n", "at least one step"},
		{"empty command", "version: 1\njobs:\n  test:\n    steps: [{run: ''}]\n", "non-empty run"},
		{"whitespace command", "version: 1\njobs:\n  test:\n    steps: [{run: '   '}]\n", "non-empty run"},
		{"unknown dependency", "version: 1\njobs:\n  test:\n    needs: [build]\n    steps: [{run: ok}]\n", "unknown job"},
		{"self dependency", "version: 1\njobs:\n  test:\n    needs: [test]\n    steps: [{run: ok}]\n", "cannot depend on itself"},
		{"duplicate dependency", "version: 1\njobs:\n  a:\n    steps: [{run: ok}]\n  b:\n    needs: [a, a]\n    steps: [{run: ok}]\n", "duplicate dependency"},
		{"unknown field", "version: 1\njobs:\n  test:\n    magic-option: true\n    steps: [{run: ok}]\n", "field magic-option not found"},
		{"malformed needs", "version: 1\njobs:\n  test:\n    needs: build\n    steps: [{run: ok}]\n", "cannot unmarshal"},
		{"malformed steps", "version: 1\njobs:\n  test:\n    steps: nope\n", "cannot unmarshal"},
		{"unknown step field", "version: 1\njobs:\n  test:\n    steps: [{run: ok, uses: thing}]\n", "field uses not found"},
		{"multiple documents", "version: 1\njobs:\n  a:\n    steps: [{run: ok}]\n---\nversion: 1\n", "multiple YAML documents"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writePipeline(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil || !strings.Contains(err.Error(), "open pipeline") {
		t.Fatalf("Load() error = %v, want open pipeline error", err)
	}
}
