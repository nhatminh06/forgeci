package pipeline

import "github.com/forgeci/forgeci/internal/config"

type Node struct {
	Name       string
	Job        config.Job
	Needs      []string
	Dependents []string
}

type Graph struct {
	Nodes map[string]*Node
	Order []string
}

func (g *Graph) InitialJobs() []string {
	initial := make([]string, 0)
	for _, name := range g.Order {
		if len(g.Nodes[name].Needs) == 0 {
			initial = append(initial, name)
		}
	}
	return initial
}
