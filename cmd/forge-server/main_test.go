package main

import "testing"

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
