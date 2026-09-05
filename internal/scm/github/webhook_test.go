package github

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func signature(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifySignature(t *testing.T) {
	secret, body := []byte("secret"), []byte(`{"value":1}`)
	if err := VerifySignature(secret, signature(secret, body), body); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "sha1=abcd", "sha256=abcd", "sha256=" + string(make([]byte, 64))} {
		if err := VerifySignature(secret, value, body); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
	if err := VerifySignature(secret, signature(secret, body), []byte(`{"value":2}`)); err == nil {
		t.Fatal("accepted changed body")
	}
}

func TestNormalize(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	push := []byte(`{"after":"0123456789abcdef0123456789abcdef01234567","ref":"refs/heads/main","repository":{"full_name":"Owner/Repo","clone_url":"http://169.254.169.254/"},"installation":{"id":7}}`)
	got, err := Normalize("push", "d1", push, now)
	if err != nil || got.Ignored || got.Event.RepositoryFullName != "owner/repo" || got.Event.CommitSHA != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	for _, body := range [][]byte{[]byte(`{"after":"0123456789abcdef0123456789abcdef01234567","ref":"refs/tags/v1","repository":{"full_name":"owner/repo"}}`), []byte(`{"after":"0000000000000000000000000000000000000000","ref":"refs/heads/main","repository":{"full_name":"owner/repo"}}`)} {
		got, err = Normalize("push", "d1", body, now)
		if err != nil || !got.Ignored {
			t.Fatalf("got=%+v err=%v", got, err)
		}
	}
	pr := []byte(`{"action":"ready_for_review","repository":{"full_name":"owner/repo"},"pull_request":{"number":4,"draft":true,"head":{"sha":"0123456789abcdef0123456789abcdef01234567","ref":"feature"},"base":{"ref":"main"}}}`)
	got, err = Normalize("pull_request", "d2", pr, now)
	if err != nil || got.Ignored || got.Event.PullRequestNumber == nil || *got.Event.PullRequestNumber != 4 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	draft := []byte(`{"action":"opened","repository":{"full_name":"owner/repo"},"pull_request":{"number":4,"draft":true,"head":{"sha":"0123456789abcdef0123456789abcdef01234567","ref":"feature"},"base":{"ref":"main"}}}`)
	got, err = Normalize("pull_request", "d3", draft, now)
	if err != nil || !got.Ignored {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	got, err = Normalize("issues", "d4", []byte(`{`), now)
	if err != nil || !got.Ignored {
		t.Fatalf("unsupported got=%+v err=%v", got, err)
	}
}
