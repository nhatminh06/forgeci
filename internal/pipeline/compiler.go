package pipeline

import (
	"fmt"
	"sort"
	"strings"

	"github.com/nhatminh06/forgeci/internal/config"
)

func Compile(cfg *config.Pipeline) (*Graph, error) {
	graph := &Graph{Nodes: make(map[string]*Node, len(cfg.Jobs))}
	for name, job := range cfg.Jobs {
		needs := append([]string(nil), job.Needs...)
		sort.Strings(needs)
		graph.Nodes[name] = &Node{Name: name, Job: job, Needs: needs}
	}
	for name, node := range graph.Nodes {
		for _, dependency := range node.Needs {
			graph.Nodes[dependency].Dependents = append(graph.Nodes[dependency].Dependents, name)
		}
	}
	for _, node := range graph.Nodes {
		sort.Strings(node.Dependents)
	}

	indegree := make(map[string]int, len(graph.Nodes))
	ready := make([]string, 0)
	for name, node := range graph.Nodes {
		indegree[name] = len(node.Needs)
		if len(node.Needs) == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		graph.Order = append(graph.Order, name)
		for _, dependent := range graph.Nodes[name].Dependents {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	if len(graph.Order) != len(graph.Nodes) {
		cyclic := make([]string, 0)
		for name, degree := range indegree {
			if degree > 0 {
				cyclic = append(cyclic, name)
			}
		}
		sort.Strings(cyclic)
		return nil, fmt.Errorf("dependency cycle involves jobs: %s", strings.Join(cyclic, ", "))
	}
	return graph, nil
}
