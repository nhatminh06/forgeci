package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nhatminh06/forgeci/internal/store"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(base string) *Client {
	return &Client{BaseURL: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 10 * time.Second}}
}
func (c *Client) do(ctx context.Context, method, path string, body any, target any) error {
	var content *bytes.Reader
	if body == nil {
		content = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		content = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, content)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("control-plane request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Error == "" {
			e.Error = resp.Status
		}
		return fmt.Errorf("server: %s", e.Error)
	}
	if target != nil {
		return json.NewDecoder(resp.Body).Decode(target)
	}
	return nil
}
func (c *Client) Submit(ctx context.Context, file string, jobs int) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/runs", map[string]any{"pipeline_file": file, "max_parallel": jobs}, &out)
	return out.ID, err
}
func (c *Client) Runs(ctx context.Context, limit int) ([]store.Run, error) {
	var out struct {
		Runs []store.Run `json:"runs"`
	}
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/v1/runs?limit=%d", limit), nil, &out)
	return out.Runs, err
}
func (c *Client) Runners(ctx context.Context) ([]store.Runner, error) {
	var out struct {
		Runners []store.Runner `json:"runners"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/runners", nil, &out)
	return out.Runners, err
}
func (c *Client) Inspect(ctx context.Context, id string) (*store.Run, error) {
	var out store.Run
	err := c.do(ctx, http.MethodGet, "/v1/runs/"+id, nil, &out)
	return &out, err
}
func (c *Client) Cancel(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/v1/runs/"+id+"/cancel", nil, nil)
}
func (c *Client) Artifacts(ctx context.Context, id string) ([]store.Artifact, error) {
	var out []store.Artifact
	err := c.do(ctx, http.MethodGet, "/v1/runs/"+id+"/artifacts", nil, &out)
	return out, err
}
func (c *Client) DownloadArtifact(ctx context.Context, id, job, name string, destination io.Writer) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/runs/"+id+"/artifacts/"+job+"/"+name, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("control-plane request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Error == "" {
			e.Error = resp.Status
		}
		return "", fmt.Errorf("server: %s", e.Error)
	}
	digest := resp.Header.Get("X-ForgeCI-Blob-SHA256")
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(destination, h), resp.Body)
	if err != nil {
		return "", err
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return "", fmt.Errorf("artifact blob digest mismatch")
	}
	return digest, nil
}
