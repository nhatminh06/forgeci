package runworkspace

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const markerName = ".forgeci-workspace.json"

type Marker struct {
	RunID, JobName, LeaseID, SourceDigest string
	Generation                            int
}

func Create(root string, marker Marker) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if !validComponent(marker.RunID) || !validComponent(marker.LeaseID) {
		return "", fmt.Errorf("invalid workspace identity")
	}
	if err := os.MkdirAll(abs, 0700); err != nil {
		return "", err
	}
	dir := workspacePath(abs, marker)
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		return "", err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return "", err
	}
	b, _ := json.Marshal(marker)
	if err := os.WriteFile(filepath.Join(dir, markerName), append(b, '\n'), 0600); err != nil {
		_ = os.Remove(dir)
		return "", err
	}
	return dir, nil
}

func workspacePath(root string, marker Marker) string {
	if marker.JobName != "" {
		return filepath.Join(root, "jobs", marker.RunID, marker.JobName, marker.LeaseID)
	}
	return filepath.Join(root, "runs", marker.RunID, marker.LeaseID)
}

func validComponent(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, "/\\\x00\r\n")
}

func Remove(root, dir string, expected Marker) error {
	if !validComponent(expected.RunID) || !validComponent(expected.LeaseID) || expected.JobName != "" && !validComponent(expected.JobName) {
		return fmt.Errorf("invalid workspace identity")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absRoot, absDir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("workspace is outside configured root")
	}
	expectedRel := filepath.Join("runs", expected.RunID, expected.LeaseID)
	if expected.JobName != "" {
		expectedRel = filepath.Join("jobs", expected.RunID, expected.JobName, expected.LeaseID)
	}
	if filepath.Clean(rel) != expectedRel {
		return fmt.Errorf("workspace path does not match identity")
	}
	candidates := []string{filepath.Join(absRoot, "runs"), filepath.Join(absRoot, "runs", expected.RunID), absDir}
	if expected.JobName != "" {
		candidates = []string{filepath.Join(absRoot, "jobs"), filepath.Join(absRoot, "jobs", expected.RunID), filepath.Join(absRoot, "jobs", expected.RunID, expected.JobName), absDir}
	}
	for _, candidate := range candidates {
		info, err := os.Lstat(candidate)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("workspace path contains unsafe link")
		}
	}
	b, err := os.ReadFile(filepath.Join(absDir, markerName))
	if err != nil {
		return fmt.Errorf("workspace marker: %w", err)
	}
	var actual Marker
	if json.Unmarshal(b, &actual) != nil || actual != expected {
		return fmt.Errorf("workspace marker identity mismatch")
	}
	if err := os.RemoveAll(absDir); err != nil {
		return err
	}
	_ = os.Remove(filepath.Dir(absDir))
	return nil
}

func CleanupStale(root string) error {
	if err := cleanupTree(root, "jobs", true); err != nil {
		return err
	}
	return cleanupTree(root, "runs", false)
}

func cleanupTree(root, kind string, jobs bool) error {
	if jobs {
		runs := filepath.Join(root, kind)
		runDirs, err := os.ReadDir(runs)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, run := range runDirs {
			if !run.IsDir() {
				continue
			}
			jobDirs, err := os.ReadDir(filepath.Join(runs, run.Name()))
			if err != nil {
				return err
			}
			for _, job := range jobDirs {
				if !job.IsDir() {
					continue
				}
				leases, err := os.ReadDir(filepath.Join(runs, run.Name(), job.Name()))
				if err != nil {
					return err
				}
				for _, lease := range leases {
					if !lease.IsDir() {
						continue
					}
					dir := filepath.Join(runs, run.Name(), job.Name(), lease.Name())
					b, err := os.ReadFile(filepath.Join(dir, markerName))
					if err != nil {
						continue
					}
					var marker Marker
					if json.Unmarshal(b, &marker) != nil || marker.RunID != run.Name() || marker.JobName != job.Name() || marker.LeaseID != lease.Name() {
						continue
					}
					if err := Remove(root, dir, marker); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	runs := filepath.Join(root, "runs")
	runDirs, err := os.ReadDir(runs)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, run := range runDirs {
		if !run.IsDir() {
			continue
		}
		leases, err := os.ReadDir(filepath.Join(runs, run.Name()))
		if err != nil {
			return err
		}
		for _, lease := range leases {
			if !lease.IsDir() {
				continue
			}
			dir := filepath.Join(runs, run.Name(), lease.Name())
			b, err := os.ReadFile(filepath.Join(dir, markerName))
			if err != nil {
				continue
			}
			var marker Marker
			if json.Unmarshal(b, &marker) != nil || marker.RunID != run.Name() || marker.LeaseID != lease.Name() {
				continue
			}
			if err := Remove(root, dir, marker); err != nil {
				return err
			}
		}
	}
	return nil
}
