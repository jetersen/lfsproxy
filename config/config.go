package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	AllowedOrgs              []string      `split_words:"true"`
	CacheEviction            time.Duration `split_words:"true" default:"23h"`
	S3PresignExpiration      time.Duration `split_words:"true" default:"24h"`
	UpstreamHost             string        `split_words:"true" required:"true"`
	UpstreamToken            string        `split_words:"true"`
	S3Bucket                 string        `split_words:"true" required:"true"`
	DebugMode                bool          `split_words:"true" default:"false"`
	S3UseAccelerate          bool          `split_words:"true" default:"false"`
	S3PresignEnabled         bool          `split_words:"true" default:"true"`
	EnablePrometheusExporter bool          `split_words:"true" default:"false"`
}

func GetConfig() (*Config, error) {
	var proxyConfiguration Config

	err := envconfig.Process("app", &proxyConfiguration)
	if err != nil {
		return nil, err
	}

	return &proxyConfiguration, nil
}
