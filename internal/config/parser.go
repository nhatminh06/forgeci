package config

import (
	"bytes"
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

	return Parse(f, path)
}

func ParseBytes(data []byte, source string) (*Pipeline, error) {
	return Parse(bytes.NewReader(data), source)
}

func Parse(r io.Reader, source string) (*Pipeline, error) {
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	var cfg Pipeline
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse pipeline %q: %w", source, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("parse pipeline %q: multiple YAML documents are not supported", source)
		}
		return nil, fmt.Errorf("parse pipeline %q: %w", source, err)
	}
	if err := Validate(&cfg); err != nil {
		return nil, fmt.Errorf("validate pipeline %q: %w", source, err)
	}
	return &cfg, nil
}
