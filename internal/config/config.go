package config

type Pipeline struct {
	Version int            `yaml:"version"`
	Jobs    map[string]Job `yaml:"jobs"`
}

type Job struct {
	Needs     []string  `yaml:"needs,omitempty"`
	Image     *string   `yaml:"image,omitempty"`
	Steps     []Step    `yaml:"steps"`
	Artifacts Artifacts `yaml:"artifacts,omitempty"`
}

type Artifacts struct {
	Upload   []ArtifactUpload   `yaml:"upload,omitempty"`
	Download []ArtifactDownload `yaml:"download,omitempty"`
}

type ArtifactUpload struct {
	Name string `yaml:"name"`
	Path string `yaml:"path"`
}

type ArtifactDownload struct {
	From string `yaml:"from"`
	Name string `yaml:"name"`
	Into string `yaml:"into"`
}

type Step struct {
	Run string `yaml:"run"`
}
