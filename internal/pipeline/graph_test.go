package pipeline

import (
	"reflect"
	"strings"
	"testing"

	"github.com/forgeci/forgeci/internal/config"
)

func job(needs ...string) config.Job {
	return config.Job{Needs: needs, Steps: []config.Step{{Run: "true"}}}
}

func TestCompileDeterministicGraph(t *testing.T) {
	cfg := &config.Pipeline{Version: 1, Jobs: map[string]config.Job{
		"D": job("B", "C"), "C": job("A"), "B": job("A"), "A": job(), "Z": job(),
	}}
	graph, err := Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"A", "B", "C", "D", "Z"}; !reflect.DeepEqual(graph.Order, want) {
		t.Fatalf("Order = %v, want %v", graph.Order, want)
	}
	if want := []string{"A", "Z"}; !reflect.DeepEqual(graph.InitialJobs(), want) {
		t.Fatalf("InitialJobs = %v, want %v", graph.InitialJobs(), want)
	}
	if want := []string{"B", "C"}; !reflect.DeepEqual(graph.Nodes["A"].Dependents, want) {
		t.Fatalf("A dependents = %v, want %v", graph.Nodes["A"].Dependents, want)
	}
	if want := []string{"B", "C"}; !reflect.DeepEqual(graph.Nodes["D"].Needs, want) {
		t.Fatalf("D needs = %v, want %v", graph.Nodes["D"].Needs, want)
	}
}

func TestCompileFanOut(t *testing.T) {
	graph, err := Compile(&config.Pipeline{Version: 1, Jobs: map[string]config.Job{
		"root": job(), "left": job("root"), "right": job("root"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"left", "right"}; !reflect.DeepEqual(graph.Nodes["root"].Dependents, want) {
		t.Fatalf("dependents = %v, want %v", graph.Nodes["root"].Dependents, want)
	}
}

func TestCompileRejectsCycles(t *testing.T) {
	tests := map[string]map[string]config.Job{
		"two jobs":   {"A": job("B"), "B": job("A")},
		"three jobs": {"A": job("C"), "B": job("A"), "C": job("B")},
	}
	for name, jobs := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Compile(&config.Pipeline{Version: 1, Jobs: jobs})
			if err == nil || !strings.Contains(err.Error(), "dependency cycle") || !strings.Contains(err.Error(), "A") {
				t.Fatalf("Compile() error = %v, want useful cycle error", err)
			}
		})
	}
}
