package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func testApp(t *testing.T) App {
	t.Helper()
	k, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		t.Fatal(e)
	}
	return App{ID: 42, Key: k, Now: func() time.Time { return time.Unix(1000, 0) }}
}
func TestAppJWT(t *testing.T) {
	a := testApp(t)
	v, e := a.JWT()
	if e != nil {
		t.Fatal(e)
	}
	p := string(v)
	parts := splitJWT(p)
	var c map[string]any
	if json.Unmarshal(parts[1], &c) != nil || c["iss"] != "42" || c["exp"].(float64)-c["iat"].(float64) > 600 {
		t.Fatal("invalid claims")
	}
	if _, e := ParsePrivateKey(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(a.Key)})); e != nil {
		t.Fatal(e)
	}
	if _, e := ParsePrivateKey([]byte("bad")); e == nil {
		t.Fatal("bad key accepted")
	}
}
func splitJWT(v string) [][]byte {
	var out [][]byte
	for _, p := range strings.Split(v, ".") {
		b, _ := base64.RawURLEncoding.DecodeString(p)
		out = append(out, b)
	}
	return out
}
func TestInstallationTokenCache(t *testing.T) {
	a := testApp(t)
	var mu sync.Mutex
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		if r.Method != "POST" || r.Header.Get("Authorization") == "" {
			t.Error("request")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"token":"test-token","expires_at":"2030-01-01T00:00:00Z"}`))
	}))
	defer s.Close()
	c, e := NewClient(s.URL, a, s.Client())
	if e != nil {
		t.Fatal(e)
	}
	c.now = func() time.Time { return time.Unix(1000, 0) }
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := c.InstallationToken(context.Background(), "7"); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
}
