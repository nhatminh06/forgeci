package config

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var jobNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)

func Validate(cfg *Pipeline) error {
	if cfg.Version != 1 {
		return fmt.Errorf("unsupported version %d: only version 1 is supported", cfg.Version)
	}
	if len(cfg.Jobs) == 0 {
		return fmt.Errorf("jobs must contain at least one job")
	}

	names := make([]string, 0, len(cfg.Jobs))
	for name := range cfg.Jobs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		job := cfg.Jobs[name]
		if !jobNamePattern.MatchString(name) {
			return fmt.Errorf("invalid job name %q: must match [A-Za-z0-9][A-Za-z0-9_-]*", name)
		}
		if len(job.Steps) == 0 {
			return fmt.Errorf("job %q must contain at least one step", name)
		}
		for i, step := range job.Steps {
			if strings.TrimSpace(step.Run) == "" {
				return fmt.Errorf("job %q step %d must contain a non-empty run command", name, i+1)
			}
		}
		seen := make(map[string]struct{}, len(job.Needs))
		for _, dependency := range job.Needs {
			if dependency == name {
				return fmt.Errorf("job %q cannot depend on itself", name)
			}
			if _, ok := cfg.Jobs[dependency]; !ok {
				return fmt.Errorf("job %q depends on unknown job %q", name, dependency)
			}
			if _, ok := seen[dependency]; ok {
				return fmt.Errorf("job %q has duplicate dependency %q", name, dependency)
			}
			seen[dependency] = struct{}{}
		}
	}
	return nil
}
