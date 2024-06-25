package config

type GeneralConfig struct {
	Enabled bool                `json:"enabled,omitempty" mapstructure:"enabled" yaml:"enabled,omitempty"`
	Source  GeneralConfigSource `json:"source,omitempty" mapstructure:"source" yaml:"source,omitempty"`
}

type GeneralConfigSource struct {
	Type  GeneralConfigSourceType   `json:"type,omitempty" mapstructure:"type" yaml:"type,omitempty"`
	Local *LocalGeneralConfigSource `json:"local,omitempty" mapstructure:"local" yaml:"local,omitempty"`
}

type GeneralConfigSourceType string

const (
	LocalGeneralConfigSourceType = "local"
)

type LocalGeneralConfigSource struct {
	Path string `json:"path,omitempty" mapstructure:"path" yaml:"path,omitempty"`
}
