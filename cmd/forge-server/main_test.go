package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateLoopback(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080"} {
		if err := validateLoopback(address); err != nil {
			t.Fatalf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"0.0.0.0:8080", "192.168.1.10:8080", ":8080", "bad"} {
		if err := validateLoopback(address); err == nil {
			t.Fatalf("accepted %s", address)
		}
	}
}

func TestReadSecretFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	if err := os.WriteFile(path, []byte(" value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readSecretFile(path)
	if err != nil || string(got) != "value" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	if _, err := readSecretFile(""); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecretFile(path); err == nil {
		t.Fatal("accepted public secret file")
	}
}

func TestValidateRunnerListener(t *testing.T) {
	for _, address := range []string{"127.0.0.1:9090", "[::1]:9090", "localhost:9090"} {
		if err := validateRunnerListener(address, "", ""); err != nil {
			t.Fatalf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"0.0.0.0:9090", "[::]:9090", "192.0.2.1:9090"} {
		if err := validateRunnerListener(address, "", ""); err == nil {
			t.Fatalf("accepted plaintext %s", address)
		}
	}
	if err := validateRunnerListener("127.0.0.1:9090", "cert.pem", ""); err == nil {
		t.Fatal("accepted certificate without key")
	}
	if err := validateRunnerListener("127.0.0.1:9090", "", "key.pem"); err == nil {
		t.Fatal("accepted key without certificate")
	}
	if err := validateRunnerListener("0.0.0.0:9090", "missing.pem", "missing-key.pem"); err == nil {
		t.Fatal("accepted missing TLS files")
	}
}
