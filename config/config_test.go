package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3PresignEnabled(t *testing.T) {
	for _, value := range []string{"default", "true", "false"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("APP_UPSTREAM_HOST", "https://github.com")
			t.Setenv("APP_S3_BUCKET", "test-bucket")
			t.Setenv("APP_S3_PRESIGN_ENABLED", value)
			if value == "default" {
				require.NoError(t, os.Unsetenv("APP_S3_PRESIGN_ENABLED"))
			}
			cfg, err := GetConfig()
			require.NoError(t, err)
			assert.Equal(t, value != "false", cfg.S3PresignEnabled)
		})
	}
}
