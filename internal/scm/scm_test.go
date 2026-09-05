package scm

import "testing"

func TestNormalizeRepository(t *testing.T) {
	got, err := NormalizeRepository(GitHub, "Foo/Bar")
	if err != nil || got != "foo/bar" {
		t.Fatalf("NormalizeRepository = %q, %v", got, err)
	}
	for _, value := range []string{"", "foo", "/foo", "foo/", "foo//bar", "foo/../bar", "github.com/foo/bar", "https://github.com/foo/bar"} {
		if _, err := NormalizeRepository(GitHub, value); err == nil {
			t.Errorf("NormalizeRepository(%q) succeeded", value)
		}
	}
}

func TestValidatePipelinePath(t *testing.T) {
	for _, value := range []string{"forge.yaml", "ci/forge.yaml", ".build/forge.yaml"} {
		if got, err := ValidatePipelinePath(value); err != nil || got != value {
			t.Errorf("ValidatePipelinePath(%q) = %q, %v", value, got, err)
		}
	}
	for _, value := range []string{"", "/forge.yaml", "../forge.yaml", "foo/../../forge.yaml", "foo/../forge.yaml", ".", "..", "C:\\\\forge.yaml"} {
		if _, err := ValidatePipelinePath(value); err == nil {
			t.Errorf("ValidatePipelinePath(%q) succeeded", value)
		}
	}
}
