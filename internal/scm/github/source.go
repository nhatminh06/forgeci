package github

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/nhatminh06/forgeci/internal/scm"
	gittransport "github.com/nhatminh06/forgeci/internal/scm/git"
	"github.com/nhatminh06/forgeci/internal/snapshot"
)

// TokenProvider is the narrow boundary used by source preparation.
type TokenProvider interface {
	InstallationToken(context.Context, string) (Token, error)
}

var materialize = gittransport.Prepare

// PrepareSource obtains one installation token then materializes the exact registered source.
func PrepareSource(ctx context.Context, tokens TokenProvider, snapshots *snapshot.Store, cloneBase string, repository scm.Repository, delivery scm.Delivery) (gittransport.Prepared, error) {
	if tokens == nil || repository.Provider != scm.GitHub || repository.ID == "" || delivery.RepositoryID != repository.ID {
		return gittransport.Prepared{}, scm.Permanent(fmt.Errorf("invalid GitHub source request"))
	}
	installation, err := strconv.ParseInt(delivery.InstallationID, 10, 64)
	if err != nil || installation < 1 {
		return gittransport.Prepared{}, scm.Permanent(fmt.Errorf("invalid GitHub installation ID"))
	}
	token, err := tokens.InstallationToken(ctx, delivery.InstallationID)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && !apiErr.Transient {
			return gittransport.Prepared{}, scm.Permanent(err)
		}
		return gittransport.Prepared{}, scm.Transient(err)
	}
	request := gittransport.Request{Provider: scm.GitHub, Repository: repository.FullName, PipelinePath: repository.PipelinePath, Ref: delivery.Ref, CommitSHA: delivery.CommitSHA, PullRequestNumber: delivery.PullRequestNumber, CloneBase: cloneBase, Token: token.Value}
	return materialize(ctx, snapshots, request)
}
