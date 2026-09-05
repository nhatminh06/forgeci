package git

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nhatminh06/forgeci/internal/scm"
	"github.com/nhatminh06/forgeci/internal/snapshot"
)

func resetMaterializeHooks(t *testing.T) {
	t.Helper()
	oldCapture, oldObserve, oldStarted, oldCleanup := captureSnapshot, observeCommand, commandStarted, beforeCleanup
	t.Cleanup(func() {
		captureSnapshot, observeCommand, commandStarted, beforeCleanup = oldCapture, oldObserve, oldStarted, oldCleanup
	})
}

func gitRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", args...)
	c.Dir = dir
	b, e := c.CombinedOutput()
	if e != nil {
		t.Fatalf("git %v: %v: %s", args, e, b)
	}
	return strings.TrimSpace(string(b))
}
func TestPrepareExactBranchRevision(t *testing.T) {
	root := t.TempDir()
	bare := filepath.Join(root, "owner", "repo.git")
	if err := os.MkdirAll(filepath.Dir(bare), 0700); err != nil {
		t.Fatal(err)
	}
	gitRun(t, root, "init", "--bare", bare)
	work := filepath.Join(root, "work")
	gitRun(t, root, "init", work)
	gitRun(t, work, "config", "user.email", "test@example.invalid")
	gitRun(t, work, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(work, "forge.yaml"), []byte("version: 1\njobs:\n  test:\n    steps:\n      - run: echo ok\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "marker.txt"), []byte("FROM_COMMIT_A"), 0600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, work, "add", ".")
	gitRun(t, work, "commit", "-m", "a")
	a := gitRun(t, work, "rev-parse", "HEAD")
	gitRun(t, work, "remote", "add", "origin", bare)
	gitRun(t, work, "push", "origin", "HEAD:refs/heads/main")
	if err := os.WriteFile(filepath.Join(work, "marker.txt"), []byte("FROM_COMMIT_B"), 0600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, work, "add", ".")
	gitRun(t, work, "commit", "-m", "b")
	b := gitRun(t, work, "rev-parse", "HEAD")
	gitRun(t, work, "push", "origin", "HEAD:refs/heads/main")
	gitRun(t, work, "push", "origin", a+":refs/pull/1/head")
	snap, err := snapshot.Open(filepath.Join(root, "snapshots"), work, snapshot.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	got, err := Prepare(context.Background(), snap, Request{Provider: scm.GitHub, Repository: "owner/repo", PipelinePath: "forge.yaml", Ref: "refs/heads/main", CommitSHA: a, CloneBase: "file://" + root})
	if err != nil {
		t.Fatal(err)
	}
	if got.GitCommitSHA != a || got.Snapshot.SourceDigest == "" {
		t.Fatalf("got=%+v", got)
	}
	if got.GitCommitSHA == got.Snapshot.SourceDigest {
		t.Fatal("Git and snapshot identities were conflated")
	}
	blob, err := snap.OpenBlob(got.Snapshot.SourceDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer blob.Close()
	restored := t.TempDir()
	if err := snapshot.Extract(blob.Name(), restored, got.Snapshot, snapshot.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(restored, "marker.txt"))
	if err != nil || string(marker) != "FROM_COMMIT_A" {
		t.Fatalf("marker=%q err=%v", marker, err)
	}
	pr, err := Prepare(context.Background(), snap, Request{Provider: scm.GitHub, Repository: "owner/repo", PipelinePath: "forge.yaml", CommitSHA: a, PullRequestNumber: ptr(1), CloneBase: "file://" + root})
	if err != nil || pr.GitCommitSHA != a {
		t.Fatalf("PR got=%+v err=%v", pr, err)
	}
	if _, err := Prepare(context.Background(), snap, Request{Provider: scm.GitHub, Repository: "owner/repo", PipelinePath: "forge.yaml", CommitSHA: b, PullRequestNumber: ptr(1), CloneBase: "file://" + root}); err == nil {
		t.Fatal("accepted mismatched PR revision")
	}
}

func ptr(value int) *int { return &value }

func TestSafePathRejectsSymlinkEscape(t *testing.T) {
	root, outside := t.TempDir(), filepath.Join(t.TempDir(), "forge.yaml")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "ci"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "ci", "forge.yaml")); err != nil {
		t.Fatal(err)
	}
	if _, err := safePath(root, "ci/forge.yaml"); err == nil {
		t.Fatal("accepted symlink escape")
	}
}

func TestRemoteTrustedIdentity(t *testing.T) {
	got, err := Remote("https://github.example.test", "owner/repo")
	if err != nil || got != "https://github.example.test/owner/repo.git" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	for _, value := range []string{"https://attacker.example/repo", "owner/repo/../../evil", "/foo/bar", "owner@attacker/repo"} {
		if _, err := Remote("https://github.example.test", value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestTokenAbsentFromFetchError(t *testing.T) {
	const token = "FORGECI_SECRET_TOKEN_TEST"
	root, source := t.TempDir(), t.TempDir()
	_, err := Prepare(context.Background(), mustSnapshot(t, root, source), Request{Provider: scm.GitHub, Repository: "owner/repo", PipelinePath: "forge.yaml", Ref: "refs/heads/main", CommitSHA: strings.Repeat("a", 40), CloneBase: "file://" + root, Token: token})
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("err=%v", err)
	}
}

func TestPrepareKeepsTokenOutOfArgvRemoteAndConfigAndCleansResources(t *testing.T) {
	resetMaterializeHooks(t)
	root, work, sha := sourceFixture(t, validPipeline, "marker.txt", "FROM_COMMIT_A")
	snap := mustSnapshot(t, root, work)
	const token = "FORGECI_SECRET_TOKEN_TEST"
	var argv [][]string
	var helpers []string
	var checkout string
	observeCommand = func(_ string, askpass string, args []string) {
		argv = append(argv, args)
		if askpass != "" {
			helpers = append(helpers, askpass)
			info, err := os.Stat(askpass)
			if err != nil || info.Mode().Perm() != 0o700 {
				t.Fatalf("askpass mode=%v err=%v", info.Mode().Perm(), err)
			}
		}
	}
	beforeCleanup = func(dir string) {
		checkout = dir
		remote := gitRun(t, dir, "remote", "-v")
		config, err := os.ReadFile(filepath.Join(dir, ".git", "config"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(remote, token) || strings.Contains(string(config), token) {
			t.Fatalf("token persisted: remote=%q config=%q", remote, config)
		}
	}
	got, err := Prepare(context.Background(), snap, Request{Provider: scm.GitHub, Repository: "owner/repo", PipelinePath: "forge.yaml", Ref: "refs/heads/main", CommitSHA: sha, CloneBase: "file://" + root, Token: token})
	if err != nil || got.Snapshot.SourceDigest == "" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	for _, args := range argv {
		for _, arg := range args {
			if strings.Contains(arg, token) {
				t.Fatalf("token present in argv: %q", args)
			}
		}
	}
	if checkout == "" || len(helpers) == 0 {
		t.Fatalf("checkout=%q helpers=%v", checkout, helpers)
	}
	assertRemoved(t, append(helpers, checkout)...)
}

func TestPrepareCleansCheckoutAndAskpassOnFailures(t *testing.T) {
	root, work, a := sourceFixture(t, validPipeline, "marker.txt", "FROM_COMMIT_A")
	invalid := commitAndPush(t, work, root, "forge.yaml", "not: a pipeline\n", "refs/heads/invalid")
	symlinkTarget := filepath.Join(t.TempDir(), "forge.yaml")
	if err := os.WriteFile(symlinkTarget, []byte(validPipeline), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(work, "forge.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(symlinkTarget, filepath.Join(work, "forge.yaml")); err != nil {
		t.Fatal(err)
	}
	gitRun(t, work, "add", "forge.yaml")
	gitRun(t, work, "commit", "-m", "symlink")
	symlinkSHA := gitRun(t, work, "rev-parse", "HEAD")
	gitRun(t, work, "push", "origin", "HEAD:refs/heads/symlink")
	gitRun(t, work, "push", "origin", a+":refs/pull/1/head")

	base := Request{Provider: scm.GitHub, Repository: "owner/repo", PipelinePath: "forge.yaml", Ref: "refs/heads/main", CommitSHA: a, CloneBase: "file://" + root, Token: "FORGECI_SECRET_TOKEN_TEST"}
	cases := []struct {
		name string
		edit func(*Request)
	}{
		{"git failure", func(r *Request) { r.Ref = "refs/heads/missing" }},
		{"wrong SHA", func(r *Request) { r.CommitSHA = strings.Repeat("f", 40) }},
		{"unavailable SHA", func(r *Request) { r.CommitSHA = strings.Repeat("e", 40) }},
		{"PR mismatch", func(r *Request) { r.PullRequestNumber = ptr(1); r.CommitSHA = invalid }},
		{"missing pipeline", func(r *Request) { r.PipelinePath = "ci/forge.yaml" }},
		{"invalid pipeline", func(r *Request) { r.Ref = "refs/heads/invalid"; r.CommitSHA = invalid }},
		{"symlink escape", func(r *Request) { r.Ref = "refs/heads/symlink"; r.CommitSHA = symlinkSHA }},
		{"snapshot failure", func(*Request) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMaterializeHooks(t)
			request := base
			tc.edit(&request)
			var paths []string
			observeCommand = func(_ string, askpass string, _ []string) {
				if askpass != "" {
					paths = append(paths, askpass)
				}
			}
			beforeCleanup = func(dir string) { paths = append(paths, dir) }
			if tc.name == "snapshot failure" {
				captureSnapshot = func(*snapshot.Store, string) (snapshot.Metadata, error) {
					return snapshot.Metadata{}, errors.New("injected snapshot failure")
				}
			}
			got, err := Prepare(context.Background(), mustSnapshot(t, filepath.Join(root, tc.name), work), request)
			if err == nil || got.Snapshot.SourceDigest != "" {
				t.Fatalf("got=%+v err=%v", got, err)
			}
			assertRemoved(t, paths...)
		})
	}
}

func TestPrepareCancellationStopsGitAndCleansResources(t *testing.T) {
	resetMaterializeHooks(t)
	bin := t.TempDir()
	script := filepath.Join(bin, "git")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do :; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan *exec.Cmd, 1)
	commandStarted = func(cmd *exec.Cmd) { started <- cmd; cancel() }
	var paths []string
	observeCommand = func(dir, askpass string, _ []string) {
		if askpass != "" {
			paths = append(paths, askpass)
		}
		if len(paths) == 0 || paths[len(paths)-1] != dir {
			paths = append(paths, dir)
		}
	}
	beforeCleanup = func(dir string) { paths = append(paths, dir) }
	result := make(chan error, 1)
	snap := mustSnapshot(t, t.TempDir(), t.TempDir())
	go func() {
		_, err := Prepare(ctx, snap, Request{Provider: scm.GitHub, Repository: "owner/repo", PipelinePath: "forge.yaml", Ref: "refs/heads/main", CommitSHA: strings.Repeat("a", 40), CloneBase: "file:///tmp", Token: "FORGECI_SECRET_TOKEN_TEST"})
		result <- err
	}()
	var child *exec.Cmd
	select {
	case child = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Git subprocess did not start")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled materialization succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Materialize did not return after cancellation")
	}
	if child.ProcessState == nil {
		t.Fatalf("child did not exit: %+v", child.ProcessState)
	}
	assertRemoved(t, paths...)
}

func TestGitDiagnosticsBoundedDuringCapture(t *testing.T) {
	bin := t.TempDir()
	script := filepath.Join(bin, "git")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ni=0; while [ $i -lt 20000 ]; do printf x; printf y >&2; i=$((i+1)); done\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	_, err := command(context.Background(), t.TempDir(), "", "fetch")
	if err == nil || len(err.Error()) > 4096+len("git command failed: ") {
		t.Fatalf("diagnostic length=%d err=%v", len(err.Error()), err)
	}
}

func mustSnapshot(t *testing.T, root, source string) *snapshot.Store {
	t.Helper()
	s, err := snapshot.Open(filepath.Join(root, "snapshots"), source, snapshot.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const validPipeline = "version: 1\njobs:\n  test:\n    steps:\n      - run: echo ok\n"

func sourceFixture(t *testing.T, pipelineData, markerName, markerData string) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	bare := filepath.Join(root, "owner", "repo.git")
	if err := os.MkdirAll(filepath.Dir(bare), 0o700); err != nil {
		t.Fatal(err)
	}
	gitRun(t, root, "init", "--bare", bare)
	work := filepath.Join(root, "work")
	gitRun(t, root, "init", work)
	gitRun(t, work, "config", "user.email", "test@example.invalid")
	gitRun(t, work, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(work, "forge.yaml"), []byte(pipelineData), 0o600); err != nil {
		t.Fatal(err)
	}
	if markerName != "" {
		if err := os.WriteFile(filepath.Join(work, markerName), []byte(markerData), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gitRun(t, work, "add", ".")
	gitRun(t, work, "commit", "-m", "fixture")
	sha := gitRun(t, work, "rev-parse", "HEAD")
	gitRun(t, work, "remote", "add", "origin", bare)
	gitRun(t, work, "push", "origin", "HEAD:refs/heads/main")
	return root, work, sha
}

func commitAndPush(t *testing.T, work, root, name, data, ref string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(work, name), []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, work, "add", name)
	gitRun(t, work, "commit", "-m", ref)
	sha := gitRun(t, work, "rev-parse", "HEAD")
	gitRun(t, work, "push", filepath.Join(root, "owner", "repo.git"), "HEAD:"+ref)
	return sha
}

func assertRemoved(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Errorf("temporary resource remains at %q: %v", path, err)
		}
	}
}

func TestPrepareRejectsMissingAndInvalidPipeline(t *testing.T) {
	root := t.TempDir()
	bare := filepath.Join(root, "owner", "repo.git")
	if err := os.MkdirAll(filepath.Dir(bare), 0700); err != nil {
		t.Fatal(err)
	}
	gitRun(t, root, "init", "--bare", bare)
	work := filepath.Join(root, "work")
	gitRun(t, root, "init", work)
	gitRun(t, work, "config", "user.email", "test@example.invalid")
	gitRun(t, work, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(work, "forge.yaml"), []byte("not: a pipeline"), 0600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, work, "add", ".")
	gitRun(t, work, "commit", "-m", "invalid")
	sha := gitRun(t, work, "rev-parse", "HEAD")
	gitRun(t, work, "remote", "add", "origin", bare)
	gitRun(t, work, "push", "origin", "HEAD:refs/heads/main")
	snap, err := snapshot.Open(filepath.Join(root, "snapshots"), work, snapshot.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	base := Request{Provider: scm.GitHub, Repository: "owner/repo", Ref: "refs/heads/main", CommitSHA: sha, CloneBase: "file://" + root}
	invalid := base
	invalid.PipelinePath = "forge.yaml"
	if _, err := Prepare(context.Background(), snap, invalid); err == nil {
		t.Fatal("accepted invalid pipeline")
	}
	missing := base
	missing.PipelinePath = "ci/forge.yaml"
	if _, err := Prepare(context.Background(), snap, missing); err == nil {
		t.Fatal("accepted missing pipeline")
	}
}
