package config

type Pipeline struct {
	Version int            `yaml:"version"`
	Jobs    map[string]Job `yaml:"jobs"`
}

type Job struct {
	Needs []string `yaml:"needs,omitempty"`
	Steps []Step   `yaml:"steps"`
}

type Step struct {
	Run string `yaml:"run"`
}
