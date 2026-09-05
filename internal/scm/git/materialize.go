// Package git materializes exact, immutable SCM source revisions.
package git

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nhatminh06/forgeci/internal/config"
	"github.com/nhatminh06/forgeci/internal/pipeline"
	"github.com/nhatminh06/forgeci/internal/scm"
	"github.com/nhatminh06/forgeci/internal/snapshot"
)

type Request struct {
	Provider                                 scm.Provider
	Repository, PipelinePath, Ref, CommitSHA string
	PullRequestNumber                        *int
	CloneBase, Token                         string
}
type Prepared struct {
	Provider                                    scm.Provider
	Repository, GitCommitSHA, Ref, PipelinePath string
	PullRequestNumber                           *int
	PipelineYAML                                []byte
	Snapshot                                    snapshot.Metadata
	Pipeline                                    *pipeline.Graph
}

var (
	captureSnapshot = func(store *snapshot.Store, root string) (snapshot.Metadata, error) {
		return store.Capture(root)
	}
	observeCommand = func(string, string, []string) {}
	commandStarted = func(*exec.Cmd) {}
	beforeCleanup  = func(string) {}
)

func Prepare(ctx context.Context, snapshots *snapshot.Store, in Request) (Prepared, error) {
	if snapshots == nil || in.Provider != scm.GitHub || !validSHA(in.CommitSHA) {
		return Prepared{}, scm.Permanent(fmt.Errorf("invalid source request"))
	}
	repo, err := scm.NormalizeRepository(in.Provider, in.Repository)
	if err != nil {
		return Prepared{}, scm.Permanent(err)
	}
	pipelinePath, err := scm.ValidatePipelinePath(in.PipelinePath)
	if err != nil {
		return Prepared{}, scm.Permanent(err)
	}
	remote, err := Remote(in.CloneBase, repo)
	if err != nil {
		return Prepared{}, scm.Permanent(err)
	}
	dir, err := os.MkdirTemp("", "forgeci-scm-")
	if err != nil {
		return Prepared{}, err
	}
	defer func() {
		beforeCleanup(dir)
		_ = os.RemoveAll(dir)
	}()
	if err := run(ctx, dir, in.Token, "init"); err != nil {
		return Prepared{}, err
	}
	if err := run(ctx, dir, in.Token, "remote", "add", "origin", remote); err != nil {
		return Prepared{}, err
	}
	ref := in.Ref
	if in.PullRequestNumber != nil {
		ref = fmt.Sprintf("refs/pull/%d/head", *in.PullRequestNumber)
	}
	if ref == "" {
		return Prepared{}, scm.Permanent(fmt.Errorf("missing source ref"))
	}
	if err := run(ctx, dir, in.Token, "fetch", "--no-tags", "origin", ref); err != nil {
		return Prepared{}, err
	}
	if err := run(ctx, dir, in.Token, "checkout", "--detach", in.CommitSHA); err != nil {
		return Prepared{}, scm.Permanent(fmt.Errorf("requested source revision unavailable: %w", err))
	}
	head, err := output(ctx, dir, "rev-parse", "HEAD")
	if err != nil || !strings.EqualFold(strings.TrimSpace(head), in.CommitSHA) {
		return Prepared{}, scm.Permanent(fmt.Errorf("requested source revision unavailable"))
	}
	path, err := safePath(dir, pipelinePath)
	if err != nil {
		return Prepared{}, scm.Permanent(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Prepared{}, scm.Permanent(fmt.Errorf("read pipeline: %w", err))
	}
	cfg, err := config.ParseBytes(data, pipelinePath)
	if err != nil {
		return Prepared{}, scm.Permanent(err)
	}
	graph, err := pipeline.Compile(cfg)
	if err != nil {
		return Prepared{}, scm.Permanent(fmt.Errorf("compile pipeline: %w", err))
	}
	meta, err := captureSnapshot(snapshots, dir)
	if err != nil {
		return Prepared{}, fmt.Errorf("capture source snapshot: %w", err)
	}
	return Prepared{Provider: in.Provider, Repository: repo, GitCommitSHA: strings.ToLower(in.CommitSHA), Ref: ref, PullRequestNumber: in.PullRequestNumber, PipelinePath: pipelinePath, PipelineYAML: append([]byte(nil), data...), Snapshot: meta, Pipeline: graph}, nil
}
func Remote(base, repo string) (string, error) {
	if _, err := scm.NormalizeRepository(scm.GitHub, repo); err != nil {
		return "", err
	}
	if !strings.HasPrefix(base, "https://") && !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "file://") {
		return "", fmt.Errorf("invalid clone base")
	}
	return strings.TrimSuffix(base, "/") + "/" + repo + ".git", nil
}
func safePath(root, name string) (string, error) {
	p := filepath.Join(root, name)
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, r)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("pipeline path escapes checkout")
	}
	return r, nil
}
func run(ctx context.Context, dir, token string, args ...string) error {
	_, err := command(ctx, dir, token, args...)
	return err
}
func output(ctx context.Context, dir string, args ...string) (string, error) {
	return command(ctx, dir, "", args...)
}
func command(ctx context.Context, dir, token string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	var askpass string
	if token != "" {
		file, err := os.CreateTemp(dir, ".forgeci-askpass-")
		if err != nil {
			return "", fmt.Errorf("create Git credential helper: %w", err)
		}
		askpass = file.Name()
		if _, err := file.WriteString("#!/bin/sh\ncase \"$1\" in *Username*) printf '%s\\n' x-access-token ;; *) printf '%s\\n' \"$FORGECI_GIT_TOKEN\" ;; esac\n"); err != nil {
			file.Close()
			os.Remove(askpass)
			return "", err
		}
		if err := file.Close(); err != nil {
			os.Remove(askpass)
			return "", err
		}
		if err := os.Chmod(askpass, 0o700); err != nil {
			os.Remove(askpass)
			return "", err
		}
		defer os.Remove(askpass)
		cmd.Env = append(cmd.Env, "GIT_ASKPASS="+askpass, "FORGECI_GIT_TOKEN="+token)
	}
	observeCommand(dir, askpass, append([]string(nil), cmd.Args...))
	var out limitedOutput
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("git command failed: %s", strings.TrimSpace(out.String()))
	}
	commandStarted(cmd)
	err := cmd.Wait()
	if err != nil {
		return "", fmt.Errorf("git command failed: %s", strings.TrimSpace(out.String()))
	}
	return out.String(), nil
}

type limitedOutput struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *limitedOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	const max = 4096
	if w.buf.Len() < max {
		n := max - w.buf.Len()
		if n > len(p) {
			n = len(p)
		}
		_, _ = w.buf.Write(p[:n])
	}
	return len(p), nil
}

func (w *limitedOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}
func validSHA(v string) bool {
	if len(v) != 40 && len(v) != 64 {
		return false
	}
	for _, r := range v {
		if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
			return false
		}
	}
	return true
}
