package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestForgeBinary(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "forge")
	build := exec.Command("go", "build", "-o", binary, "./cmd/forge")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build binary: %v\n%s", err, output)
	}

	tests := []struct {
		name, pipeline string
		wantSuccess    bool
		contains       []string
		absent         string
	}{
		{"success", "version: 1\njobs:\n  hello:\n    steps: [{run: echo hello}]\n", true, []string{"hello", "PASSED", "Pipeline succeeded"}, ""},
		{"dependency order", "version: 1\njobs:\n  second:\n    needs: [first]\n    steps: [{run: echo second-marker}]\n  first:\n    steps: [{run: echo first-marker}]\n", true, []string{"first-marker", "second-marker"}, ""},
		{"failure propagation", "version: 1\njobs:\n  build:\n    steps: [{run: exit 9}]\n  test:\n    needs: [build]\n    steps: [{run: echo SHOULD_NOT_RUN}]\n", false, []string{"FAILED", "BLOCKED", "Pipeline failed"}, "SHOULD_NOT_RUN"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "forge.yaml"), []byte(tc.pipeline), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "run")
			cmd.Dir = dir
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.wantSuccess {
				t.Fatalf("success = %v, output = %q", err == nil, output)
			}
			text := string(output)
			for _, want := range tc.contains {
				if !strings.Contains(text, want) {
					t.Fatalf("output = %q, want containing %q", text, want)
				}
			}
			if tc.absent != "" && strings.Contains(text, tc.absent) {
				t.Fatalf("output unexpectedly contains %q: %q", tc.absent, text)
			}
			if tc.name == "dependency order" && strings.Index(text, "first-marker") >= strings.Index(text, "second-marker") {
				t.Fatalf("dependency order incorrect: %q", text)
			}
		})
	}
}
