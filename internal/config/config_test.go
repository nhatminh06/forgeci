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

func TestArtifactDeclarations(t *testing.T) {
	valid := `version: 1
jobs:
  build:
    steps:
      - run: mkdir -p dist
    artifacts:
      upload:
        - name: application
          path: dist/app
  test:
    needs: [build]
    steps:
      - run: test -f inputs/app
    artifacts:
      download:
        - from: build
          name: application
          into: inputs
`
	if _, err := ParseBytes([]byte(valid), "valid"); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"unknown field":         strings.Replace(valid, "path: dist/app", "paths: [dist/app]", 1),
		"bad name":              strings.Replace(valid, "name: application", "name: ../bad", 1),
		"absolute upload":       strings.Replace(valid, "path: dist/app", "path: /tmp/app", 1),
		"upload traversal":      strings.Replace(valid, "path: dist/app", "path: ../app", 1),
		"download traversal":    strings.Replace(valid, "into: inputs", "into: ../inputs", 1),
		"not direct dependency": strings.Replace(valid, "needs: [build]", "needs: []", 1),
		"undeclared artifact":   strings.Replace(valid, "name: application\n          into: inputs", "name: other\n          into: inputs", 1),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseBytes([]byte(data), name); err == nil {
				t.Fatal("invalid artifact pipeline accepted")
			}
		})
	}
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
		{"unknown top-level field", "version: 1\nunknown: true\njobs:\n  test:\n    steps: [{run: ok}]\n", "field unknown not found"},
		{"empty image", "version: 1\njobs:\n  test:\n    image: ''\n    steps: [{run: ok}]\n", "image must be"},
		{"whitespace image", "version: 1\njobs:\n  test:\n    image: ' '\n    steps: [{run: ok}]\n", "image must be"},
		{"leading image whitespace", "version: 1\njobs:\n  test:\n    image: ' alpine:3.22'\n    steps: [{run: ok}]\n", "image must be"},
		{"trailing image whitespace", "version: 1\njobs:\n  test:\n    image: 'alpine:3.22 '\n    steps: [{run: ok}]\n", "image must be"},
		{"embedded image space", "version: 1\njobs:\n  test:\n    image: 'alpine :3.22'\n    steps: [{run: ok}]\n", "image must be"},
		{"image tab", "version: 1\njobs:\n  test:\n    image: \"alpine\\t:3.22\"\n    steps: [{run: ok}]\n", "image must be"},
		{"image CR", "version: 1\njobs:\n  test:\n    image: \"alpine\\r:3.22\"\n    steps: [{run: ok}]\n", "image must be"},
		{"image LF", "version: 1\njobs:\n  test:\n    image: \"alpine\\n:3.22\"\n    steps: [{run: ok}]\n", "image must be"},
		{"image NUL", "version: 1\njobs:\n  test:\n    image: \"alpine\\0:3.22\"\n    steps: [{run: ok}]\n", "image must be"},
		{"image control", "version: 1\njobs:\n  test:\n    image: \"alpine\\x01:3.22\"\n    steps: [{run: ok}]\n", "image must be"},
		{"wrong image type", "version: 1\njobs:\n  test:\n    image: [alpine]\n    steps: [{run: ok}]\n", "cannot unmarshal"},
		{"malformed needs", "version: 1\njobs:\n  test:\n    needs: build\n    steps: [{run: ok}]\n", "cannot unmarshal"},
		{"malformed steps", "version: 1\njobs:\n  test:\n    steps: nope\n", "cannot unmarshal"},
		{"unknown step field", "version: 1\njobs:\n  test:\n    steps: [{run: ok, uses: thing}]\n", "field uses not found"},
		{"unknown shell field", "version: 1\njobs:\n  test:\n    steps: [{run: ok, shell: bash}]\n", "field shell not found"},
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

func TestLoadImageReferences(t *testing.T) {
	images := []string{"alpine:3.22", "golang:1.27", "registry.example.com/team/image:v1", "alpine@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	for _, image := range images {
		t.Run(image, func(t *testing.T) {
			cfg, err := Load(writePipeline(t, "version: 1\njobs:\n  test:\n    image: "+image+"\n    steps: [{run: true}]\n"))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Jobs["test"].Image == nil || *cfg.Jobs["test"].Image != image {
				t.Fatalf("image = %#v, want %q", cfg.Jobs["test"].Image, image)
			}
		})
	}
}

func TestJobNamePolicy(t *testing.T) {
	accepted := []string{"build", "test-1", "lint_job", "A", "0build"}
	for _, name := range accepted {
		t.Run("accept "+name, func(t *testing.T) {
			cfg := &Pipeline{Version: 1, Jobs: map[string]Job{name: {Steps: []Step{{Run: "true"}}}}}
			if err := Validate(cfg); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}

	rejected := []string{"", " leading", "trailing ", "contains space", "build/test", `build\test`, "build:test", "build;rm", "new\nline", "has\ttab", "control\x01byte"}
	for _, name := range rejected {
		t.Run("reject "+name, func(t *testing.T) {
			cfg := &Pipeline{Version: 1, Jobs: map[string]Job{name: {Steps: []Step{{Run: "true"}}}}}
			err := Validate(cfg)
			if err == nil || !strings.Contains(err.Error(), "invalid job name") {
				t.Fatalf("Validate() error = %v, want invalid job name", err)
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
