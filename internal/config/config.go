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
	Cache     Cache     `yaml:"cache,omitempty"`
}

type Cache struct {
	Restore []CacheEntry `yaml:"restore,omitempty"`
	Save    []CacheEntry `yaml:"save,omitempty"`
}

type CacheEntry struct {
	Key  string `yaml:"key"`
	Path string `yaml:"path"`
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
