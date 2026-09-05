package github

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const tokenSafetyMargin = time.Minute

type App struct {
	ID  int64
	Key *rsa.PrivateKey
	Now func() time.Time
}

func ParsePrivateKey(data []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("invalid GitHub private key")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid GitHub private key")
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("GitHub private key must be RSA")
	}
	return rsaKey, nil
}

func (a App) JWT() (string, error) {
	if a.ID < 1 || a.Key == nil {
		return "", errors.New("invalid GitHub App credentials")
	}
	now := time.Now()
	if a.Now != nil {
		now = a.Now()
	}
	now = now.UTC()
	head, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{"iss": strconv.FormatInt(a.ID, 10), "iat": now.Add(-30 * time.Second).Unix(), "exp": now.Add(9 * time.Minute).Unix()})
	input := base64.RawURLEncoding.EncodeToString(head) + "." + base64.RawURLEncoding.EncodeToString(claims)
	sum := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, a.Key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

type Token struct {
	Value     string
	ExpiresAt time.Time
}
type APIError struct {
	Status    int
	Transient bool
}

func (e *APIError) Error() string { return fmt.Sprintf("GitHub API request failed (%d)", e.Status) }

type Client struct {
	base    *url.URL
	app     App
	http    *http.Client
	mu      sync.Mutex
	cache   map[string]Token
	flights map[string]*tokenFlight
	now     func() time.Time
}
type tokenFlight struct {
	done  chan struct{}
	token Token
	err   error
}

func NewClient(baseURL string, app App, httpClient *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme != "https" && u.Scheme != "http" || u.Host == "" {
		return nil, errors.New("invalid GitHub API base URL")
	}
	if app.ID < 1 || app.Key == nil {
		return nil, errors.New("invalid GitHub App credentials")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{base: u, app: app, http: httpClient, cache: map[string]Token{}, flights: map[string]*tokenFlight{}, now: time.Now}, nil
}

func ValidateCloneBase(value string) error {
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid GitHub clone base URL")
	}
	return nil
}
func (c *Client) InstallationToken(ctx context.Context, id string) (Token, error) {
	if _, err := strconv.ParseInt(id, 10, 64); err != nil || id == "0" {
		return Token{}, errors.New("invalid GitHub installation ID")
	}
	now := c.now().UTC()
	c.mu.Lock()
	if t, ok := c.cache[id]; ok && t.ExpiresAt.After(now.Add(tokenSafetyMargin)) {
		c.mu.Unlock()
		return t, nil
	}
	if f := c.flights[id]; f != nil {
		c.mu.Unlock()
		select {
		case <-f.done:
			return f.token, f.err
		case <-ctx.Done():
			return Token{}, ctx.Err()
		}
	}
	f := &tokenFlight{done: make(chan struct{})}
	c.flights[id] = f
	c.mu.Unlock()
	f.token, f.err = c.issue(ctx, id)
	c.mu.Lock()
	if f.err == nil {
		c.cache[id] = f.token
	}
	delete(c.flights, id)
	close(f.done)
	c.mu.Unlock()
	return f.token, f.err
}
func (c *Client) issue(ctx context.Context, id string) (Token, error) {
	jwt, err := c.app.JWT()
	if err != nil {
		return Token{}, err
	}
	u := *c.base
	u.Path = strings.TrimSuffix(u.Path, "/") + "/app/installations/" + id + "/access_tokens"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return Token{}, err
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := c.http.Do(req)
	if err != nil {
		return Token{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return Token{}, &APIError{Status: resp.StatusCode, Transient: resp.StatusCode == 429 || resp.StatusCode >= 500}
	}
	var out struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	d := json.NewDecoder(io.LimitReader(resp.Body, 64<<10))
	d.DisallowUnknownFields()
	if err := d.Decode(&out); err != nil {
		return Token{}, errors.New("invalid GitHub token response")
	}
	if out.Token == "" || !out.ExpiresAt.After(c.now().UTC()) {
		return Token{}, errors.New("invalid GitHub token response")
	}
	return Token{out.Token, out.ExpiresAt.UTC()}, nil
}
