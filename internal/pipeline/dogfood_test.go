package pipeline

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/nhatminh06/forgeci/internal/config"
)

func TestRepositoryDogfoodPipeline(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller unavailable")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	p, err := config.Load(filepath.Join(root, "forge.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	g, err := Compile(p)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{"format": true, "vet": true, "unit": true, "race": true, "build": true, "binary-smoke": true, "docker-smoke": true}
	if len(g.Nodes) != len(expected) {
		t.Fatalf("jobs=%d", len(g.Nodes))
	}
	for name := range expected {
		if g.Nodes[name] == nil {
			t.Fatalf("missing %s", name)
		}
	}
	build := g.Nodes["build"]
	for _, name := range []string{"format", "vet", "unit"} {
		if !contains(build.Needs, name) {
			t.Fatalf("build missing %s", name)
		}
	}
	if !contains(g.Nodes["race"].Needs, "unit") || !contains(g.Nodes["binary-smoke"].Needs, "build") || !contains(g.Nodes["docker-smoke"].Needs, "build") {
		t.Fatal("dogfood dependencies invalid")
	}
	if g.Nodes["docker-smoke"].Job.Image == nil {
		t.Fatal("docker image missing")
	}
	if len(build.Job.Artifacts.Upload) != 1 || build.Job.Artifacts.Upload[0].Name != "self-binaries" {
		t.Fatal("build artifact missing")
	}
	for _, name := range []string{"binary-smoke", "docker-smoke"} {
		if len(g.Nodes[name].Job.Artifacts.Download) != 1 || g.Nodes[name].Job.Artifacts.Download[0].From != "build" {
			t.Fatalf("%s artifact download invalid", name)
		}
	}
	if g.Nodes["binary-smoke"].Job.Artifacts.Download[0].Into != "binary-input" || g.Nodes["docker-smoke"].Job.Artifacts.Download[0].Into != "docker-input" {
		t.Fatal("smoke artifact destinations must be isolated")
	}
	if len(g.Nodes["unit"].Job.Cache.Restore) != 1 || len(g.Nodes["unit"].Job.Cache.Save) != 1 {
		t.Fatal("unit cache missing")
	}
	if _, err := os.Stat(filepath.Join(root, "forge.yaml")); err != nil {
		t.Fatal(err)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
