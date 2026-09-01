package config

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

func Load(path string) (*Pipeline, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open pipeline %q: %w", path, err)
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	decoder.KnownFields(true)
	var cfg Pipeline
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse pipeline %q: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("parse pipeline %q: multiple YAML documents are not supported", path)
		}
		return nil, fmt.Errorf("parse pipeline %q: %w", path, err)
	}
	if err := Validate(&cfg); err != nil {
		return nil, fmt.Errorf("validate pipeline %q: %w", path, err)
	}
	return &cfg, nil
}
