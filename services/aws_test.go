package services

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
)

type mockS3Client struct {
	bucket          string
	objectsInBucket []string
}

func (m mockS3Client) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if *input.Bucket == m.bucket {
		for _, object := range m.objectsInBucket {
			if object == *input.Key {
				return &s3.HeadObjectOutput{}, nil
			}
		}
	}
	return nil, &smithy.GenericAPIError{Code: "NotFound", Message: "Object not found"}
}

type mockManifestClient struct {
	objects []string
}

func (m mockManifestClient) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
}

func (m mockManifestClient) PutObject(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func (m mockManifestClient) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	contents := make([]s3types.Object, len(m.objects))
	for i, key := range m.objects {
		k := key
		contents[i] = s3types.Object{Key: &k}
	}
	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

func TestOIDExists(t *testing.T) {
	ctx := context.Background()

	t.Run("OIDExists return false because OID doesn't exist", func(t *testing.T) {
		svc := AWS{bucket: "test-bucket", client: mockS3Client{bucket: "test-bucket"}}
		exists, err := svc.OIDExists(ctx, "test-oid")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("OIDExists return true because OID exists", func(t *testing.T) {
		svc := AWS{bucket: "test-bucket", client: mockS3Client{bucket: "test-bucket", objectsInBucket: []string{"test-oid"}}}
		exists, err := svc.OIDExists(ctx, "test-oid")
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestGetDirectURL(t *testing.T) {
	t.Run("standard S3 URL", func(t *testing.T) {
		svc := AWS{bucket: "my-bucket", region: "eu-west-1"}
		assert.Equal(t, "https://my-bucket.s3.eu-west-1.amazonaws.com/abc123", svc.GetDirectURL("abc123"))
	})

	t.Run("accelerate S3 URL", func(t *testing.T) {
		svc := AWS{bucket: "my-bucket", useAccelerate: true}
		assert.Equal(t, "https://my-bucket.s3-accelerate.amazonaws.com/abc123", svc.GetDirectURL("abc123"))
	})
}

func TestBootstrapManifest(t *testing.T) {
	svc := AWS{
		bucket:         "test-bucket",
		manifestClient: mockManifestClient{objects: []string{"abc", "def", ".lfsproxy-manifest"}},
	}
	oids, err := svc.bootstrapManifest(context.Background())
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"abc", "def"}, oids)
}
