package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/nhatminh06/forgeci/internal/scm"
)

const (
	CheckName         = "ForgeCI"
	maxCheckResponse  = 64 << 10
	maxCheckListPages = 3
	githubAPIVersion  = "2022-11-28"
)

type CheckRequest struct {
	Repository, InstallationID, CommitSHA, ExternalID, CheckRunID string
	Status                                                        string
	Conclusion                                                    *string
}

func (c *Client) ReconcileCheck(ctx context.Context, in CheckRequest) (string, error) {
	if _, err := scm.NormalizeRepository(scm.GitHub, in.Repository); err != nil || in.CommitSHA == "" || in.ExternalID == "" {
		return "", errors.New("invalid GitHub Check request")
	}
	token, err := c.InstallationToken(ctx, in.InstallationID)
	if err != nil {
		return "", err
	}
	checkID := in.CheckRunID
	if checkID == "" {
		checkID, err = c.findCheck(ctx, token.Value, in)
		if err != nil {
			return "", err
		}
	}
	if checkID == "" {
		return c.createCheck(ctx, token.Value, in)
	}
	return checkID, c.updateCheck(ctx, token.Value, checkID, in)
}

func (c *Client) findCheck(ctx context.Context, token string, in CheckRequest) (string, error) {
	for page := 1; page <= maxCheckListPages; page++ {
		var out struct {
			CheckRuns []struct {
				ID         int64  `json:"id"`
				ExternalID string `json:"external_id"`
			} `json:"check_runs"`
		}
		endpoint := c.repoURL(in.Repository, "/commits/"+url.PathEscape(in.CommitSHA)+"/check-runs")
		query := endpoint.Query()
		query.Set("check_name", CheckName)
		query.Set("per_page", "100")
		query.Set("page", strconv.Itoa(page))
		endpoint.RawQuery = query.Encode()
		if err := c.checkJSON(ctx, http.MethodGet, endpoint, token, nil, &out); err != nil {
			return "", err
		}
		for _, check := range out.CheckRuns {
			if check.ExternalID == in.ExternalID {
				return strconv.FormatInt(check.ID, 10), nil
			}
		}
		if len(out.CheckRuns) < 100 {
			break
		}
	}
	return "", nil
}

func (c *Client) createCheck(ctx context.Context, token string, in CheckRequest) (string, error) {
	body := map[string]any{"name": CheckName, "head_sha": in.CommitSHA, "external_id": in.ExternalID, "status": in.Status}
	if in.Conclusion != nil {
		body["conclusion"] = *in.Conclusion
	}
	var out struct {
		ID int64 `json:"id"`
	}
	if err := c.checkJSON(ctx, http.MethodPost, c.repoURL(in.Repository, "/check-runs"), token, body, &out); err != nil {
		return "", err
	}
	if out.ID < 1 {
		return "", errors.New("invalid GitHub Check response")
	}
	return strconv.FormatInt(out.ID, 10), nil
}

func (c *Client) updateCheck(ctx context.Context, token, id string, in CheckRequest) error {
	body := map[string]any{"name": CheckName, "status": in.Status}
	if in.Conclusion != nil {
		body["conclusion"] = *in.Conclusion
	}
	return c.checkJSON(ctx, http.MethodPatch, c.repoURL(in.Repository, "/check-runs/"+url.PathEscape(id)), token, body, nil)
}

func (c *Client) repoURL(repository, suffix string) *url.URL {
	parts := strings.Split(repository, "/")
	u := *c.base
	u.Path = strings.TrimSuffix(u.Path, "/") + "/repos/" + url.PathEscape(parts[0]) + "/" + url.PathEscape(parts[1]) + suffix
	return &u
}

func (c *Client) checkJSON(ctx context.Context, method string, endpoint *url.URL, token string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return &APIError{Status: resp.StatusCode, Transient: resp.StatusCode == 429 || resp.StatusCode >= 500}
	}
	limited := io.LimitReader(resp.Body, maxCheckResponse+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(data) > maxCheckResponse {
		return errors.New("GitHub Check response too large")
	}
	if output != nil {
		if err := json.Unmarshal(data, output); err != nil {
			return fmt.Errorf("invalid GitHub Check response")
		}
	}
	return nil
}
