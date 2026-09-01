package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMainExitCodes(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name, body string
		wantCode   int
		wantOutput string
	}{
		{"success", "version: 1\njobs:\n  hello:\n    steps: [{run: echo hello}]\n", 0, "Pipeline succeeded"},
		{"job failure", "version: 1\njobs:\n  fail:\n    steps: [{run: exit 3}]\n", 1, "Pipeline failed"},
		{"configuration error", "version: 9\njobs: {}\n", 2, "unsupported version"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "-")+".yaml")
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := Main(context.Background(), []string{"run", "--file", path}, dir, &stdout, &stderr)
			combined := stdout.String() + stderr.String()
			if code != tc.wantCode || !strings.Contains(combined, tc.wantOutput) {
				t.Fatalf("code = %d, output = %q; want code %d containing %q", code, combined, tc.wantCode, tc.wantOutput)
			}
		})
	}
}

func TestHelp(t *testing.T) {
	var output bytes.Buffer
	if code := Main(context.Background(), []string{"--help"}, t.TempDir(), &output, &output); code != 0 {
		t.Fatalf("code = %d", code)
	}
	if text := output.String(); !strings.Contains(text, "repository-local") || !strings.Contains(text, "forge.yaml") || !strings.Contains(text, "--jobs") {
		t.Fatalf("help = %q", text)
	}
}

func TestJobsValidationPreventsExecution(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "executed")
	path := filepath.Join(dir, "forge.yaml")
	body := "version: 1\njobs:\n  test:\n    steps:\n      - run: touch " + marker + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"0", "-1", "invalid"} {
		t.Run(value, func(t *testing.T) {
			var output bytes.Buffer
			code := Main(context.Background(), []string{"run", "--jobs", value, "--file", path}, dir, &output, &output)
			if code != 2 {
				t.Fatalf("code = %d, output = %q", code, output.String())
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("pipeline command executed for --jobs %s", value)
			}
		})
	}
}
