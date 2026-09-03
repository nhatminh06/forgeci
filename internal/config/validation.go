package config

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

var jobNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)
var artifactNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var cacheKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

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
		if job.Image != nil {
			image := *job.Image
			if image == "" || strings.TrimSpace(image) != image || strings.IndexFunc(image, func(r rune) bool {
				return unicode.IsSpace(r) || unicode.IsControl(r)
			}) >= 0 {
				return fmt.Errorf("job %q image must be a non-empty reference without whitespace or control characters", name)
			}
		}
		for i, step := range job.Steps {
			if strings.TrimSpace(step.Run) == "" {
				return fmt.Errorf("job %q step %d must contain a non-empty run command", name, i+1)
			}
		}
		if err := validateArtifacts(name, job, cfg.Jobs); err != nil {
			return err
		}
		if err := validateCache(name, job); err != nil {
			return err
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

func validateCache(jobName string, job Job) error {
	restore := make(map[string]string, len(job.Cache.Restore))
	for _, item := range job.Cache.Restore {
		if !cacheKeyPattern.MatchString(item.Key) {
			return fmt.Errorf("job %q has invalid cache restore key %q", jobName, item.Key)
		}
		if !safeCachePath(item.Path) {
			return fmt.Errorf("job %q cache restore key %q has unsafe path %q", jobName, item.Key, item.Path)
		}
		if _, exists := restore[item.Key]; exists {
			return fmt.Errorf("job %q has duplicate cache restore key %q", jobName, item.Key)
		}
		restore[item.Key] = item.Path
	}
	save := make(map[string]string, len(job.Cache.Save))
	for _, item := range job.Cache.Save {
		if !cacheKeyPattern.MatchString(item.Key) {
			return fmt.Errorf("job %q has invalid cache save key %q", jobName, item.Key)
		}
		if !safeCachePath(item.Path) {
			return fmt.Errorf("job %q cache save key %q has unsafe path %q", jobName, item.Key, item.Path)
		}
		if _, exists := save[item.Key]; exists {
			return fmt.Errorf("job %q has duplicate cache save key %q", jobName, item.Key)
		}
		save[item.Key] = item.Path
		if restorePath, exists := restore[item.Key]; exists && restorePath != item.Path {
			return fmt.Errorf("job %q cache key %q has conflicting restore/save paths", jobName, item.Key)
		}
	}
	for _, item := range job.Cache.Restore {
		for _, artifact := range job.Artifacts.Download {
			if cachePathsConflict(item.Path, artifact.Into) {
				return fmt.Errorf("job %q cache restore path %q conflicts with artifact download path %q", jobName, item.Path, artifact.Into)
			}
		}
	}
	for _, item := range job.Cache.Restore {
		for _, artifact := range job.Artifacts.Upload {
			if cachePathsConflict(item.Path, artifact.Path) {
				return fmt.Errorf("job %q cache restore path %q conflicts with artifact upload path %q", jobName, item.Path, artifact.Path)
			}
		}
	}
	return nil
}

func safeCachePath(value string) bool {
	if value == "" || value == "." || path.IsAbs(value) || strings.ContainsRune(value, 0) || strings.Contains(value, `\`) || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return false
	}
	clean := path.Clean(value)
	return clean == value && clean != ".." && !strings.HasPrefix(clean, "../")
}

func cachePathsConflict(a, b string) bool {
	a = path.Clean(a)
	b = path.Clean(b)
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func validateArtifacts(jobName string, job Job, jobs map[string]Job) error {
	uploads := make(map[string]struct{}, len(job.Artifacts.Upload))
	for _, item := range job.Artifacts.Upload {
		if !artifactNamePattern.MatchString(item.Name) || item.Name == ".." {
			return fmt.Errorf("job %q has invalid artifact name %q", jobName, item.Name)
		}
		if _, exists := uploads[item.Name]; exists {
			return fmt.Errorf("job %q has duplicate artifact upload %q", jobName, item.Name)
		}
		uploads[item.Name] = struct{}{}
		if !safeArtifactPath(item.Path) {
			return fmt.Errorf("job %q artifact %q has unsafe upload path %q", jobName, item.Name, item.Path)
		}
	}
	needs := make(map[string]struct{}, len(job.Needs))
	for _, dependency := range job.Needs {
		needs[dependency] = struct{}{}
	}
	seenRefs := make(map[string]struct{}, len(job.Artifacts.Download))
	seenTargets := make(map[string]struct{}, len(job.Artifacts.Download))
	for _, item := range job.Artifacts.Download {
		if !jobNamePattern.MatchString(item.From) {
			return fmt.Errorf("job %q artifact download has invalid producer %q", jobName, item.From)
		}
		if !artifactNamePattern.MatchString(item.Name) || item.Name == ".." {
			return fmt.Errorf("job %q has invalid artifact name %q", jobName, item.Name)
		}
		if !safeArtifactPath(item.Into) {
			return fmt.Errorf("job %q artifact %q has unsafe download destination %q", jobName, item.Name, item.Into)
		}
		producer, exists := jobs[item.From]
		if !exists {
			return fmt.Errorf("job %q downloads artifact %q from unknown job %q", jobName, item.Name, item.From)
		}
		if _, exists := needs[item.From]; !exists {
			return fmt.Errorf("job %q downloads artifact %q from %q which is not a direct dependency", jobName, item.Name, item.From)
		}
		declared := false
		root := ""
		for _, upload := range producer.Artifacts.Upload {
			if upload.Name == item.Name {
				declared = true
				root = path.Base(upload.Path)
				break
			}
		}
		if !declared {
			return fmt.Errorf("job %q downloads undeclared artifact %q from %q", jobName, item.Name, item.From)
		}
		ref := item.From + "\x00" + item.Name
		if _, exists := seenRefs[ref]; exists {
			return fmt.Errorf("job %q has duplicate artifact download %q from %q", jobName, item.Name, item.From)
		}
		seenRefs[ref] = struct{}{}
		target := path.Join(item.Into, root)
		if _, exists := seenTargets[target]; exists {
			return fmt.Errorf("job %q has conflicting artifact destination %q", jobName, target)
		}
		seenTargets[target] = struct{}{}
	}
	return nil
}

func safeArtifactPath(value string) bool {
	if value == "" || value == "." || path.IsAbs(value) || strings.ContainsRune(value, 0) || strings.Contains(value, `\`) || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return false
	}
	clean := path.Clean(value)
	return clean == value && clean != ".." && !strings.HasPrefix(clean, "../")
}
