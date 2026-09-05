package github

import (
	"context"
	"errors"
	"testing"

	"github.com/nhatminh06/forgeci/internal/scm"
	gittransport "github.com/nhatminh06/forgeci/internal/scm/git"
	"github.com/nhatminh06/forgeci/internal/snapshot"
)

type countingTokens struct {
	calls int
	err   error
}

func (c *countingTokens) InstallationToken(context.Context, string) (Token, error) {
	c.calls++
	return Token{Value: "token"}, c.err
}

func withMaterializer(t *testing.T, fn func(context.Context, *snapshot.Store, gittransport.Request) (gittransport.Prepared, error)) {
	t.Helper()
	old := materialize
	materialize = fn
	t.Cleanup(func() { materialize = old })
}

func TestPrepareSourceRejectsInvalidInstallationBeforeToken(t *testing.T) {
	tokens := &countingTokens{}
	calls := 0
	withMaterializer(t, func(context.Context, *snapshot.Store, gittransport.Request) (gittransport.Prepared, error) {
		calls++
		return gittransport.Prepared{}, nil
	})
	_, err := PrepareSource(context.Background(), tokens, nil, "https://github.com", scm.Repository{ID: "repo", Provider: scm.GitHub}, scm.Delivery{RepositoryID: "repo", InstallationID: "0"})
	if err == nil || tokens.calls != 0 || calls != 0 {
		t.Fatalf("err=%v token calls=%d materializer calls=%d", err, tokens.calls, calls)
	}
}

func TestPrepareSourceTokenFailureDoesNotMaterialize(t *testing.T) {
	want := errors.New("temporary token failure")
	tokens := &countingTokens{err: want}
	calls := 0
	withMaterializer(t, func(context.Context, *snapshot.Store, gittransport.Request) (gittransport.Prepared, error) {
		calls++
		return gittransport.Prepared{}, nil
	})
	_, err := PrepareSource(context.Background(), tokens, nil, "https://github.com", scm.Repository{ID: "repo", Provider: scm.GitHub}, scm.Delivery{RepositoryID: "repo", InstallationID: "7"})
	if !errors.Is(err, want) || tokens.calls != 1 || calls != 0 {
		t.Fatalf("err=%v token calls=%d materializer calls=%d", err, tokens.calls, calls)
	}
}

func TestPrepareSourceObtainsOneTokenAndMaterializesOnce(t *testing.T) {
	tokens := &countingTokens{}
	calls := 0
	withMaterializer(t, func(_ context.Context, _ *snapshot.Store, in gittransport.Request) (gittransport.Prepared, error) {
		calls++
		if in.Token != "token" || in.Repository != "owner/repo" || in.CloneBase != "https://github.example.test" {
			t.Fatalf("request=%+v", in)
		}
		return gittransport.Prepared{Repository: in.Repository}, nil
	})
	repo := scm.Repository{ID: "repo", Provider: scm.GitHub, FullName: "owner/repo", PipelinePath: "forge.yaml"}
	delivery := scm.Delivery{RepositoryID: "repo", InstallationID: "7", CommitSHA: "0123456789012345678901234567890123456789"}
	got, err := PrepareSource(context.Background(), tokens, nil, "https://github.example.test", repo, delivery)
	if err != nil || got.Repository != repo.FullName || tokens.calls != 1 || calls != 1 {
		t.Fatalf("got=%+v err=%v token calls=%d materializer calls=%d", got, err, tokens.calls, calls)
	}
}

func TestPrepareSourceRejectsRepositoryMismatchBeforeToken(t *testing.T) {
	tokens := &countingTokens{}
	calls := 0
	withMaterializer(t, func(context.Context, *snapshot.Store, gittransport.Request) (gittransport.Prepared, error) {
		calls++
		return gittransport.Prepared{}, nil
	})
	_, err := PrepareSource(context.Background(), tokens, nil, "https://github.com", scm.Repository{ID: "repo", Provider: scm.GitHub}, scm.Delivery{RepositoryID: "other", InstallationID: "7"})
	if err == nil || tokens.calls != 0 || calls != 0 {
		t.Fatalf("err=%v token calls=%d materializer calls=%d", err, tokens.calls, calls)
	}
}
