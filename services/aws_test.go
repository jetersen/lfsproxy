package services

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

type mockPresignClient struct {
	called *int
}

func (m mockPresignClient) PresignGetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	*m.called++
	return &v4.PresignedHTTPRequest{URL: "https://presigned.example.com/get/" + aws.ToString(input.Key)}, nil
}

func (m mockPresignClient) PresignHeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	*m.called++
	return &v4.PresignedHTTPRequest{URL: "https://presigned.example.com/head/" + aws.ToString(input.Key)}, nil
}

func TestOIDExists(t *testing.T) {
	ctx := context.Background()

	t.Run("OIDExists return false because OID doesn't exist", func(t *testing.T) {
		awsService := AWS{
			bucket: "test-bucket",
			client: mockS3Client{
				bucket:          "test-bucket",
				objectsInBucket: []string{},
			},
		}

		exists, err := awsService.OIDExists(ctx, "test-oid")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("OIDExists return true because OID exists", func(t *testing.T) {
		awsService := AWS{
			bucket: "test-bucket",
			client: mockS3Client{
				bucket:          "test-bucket",
				objectsInBucket: []string{"test-oid"},
			},
		}

		exists, err := awsService.OIDExists(ctx, "test-oid")
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestGetOIDPreSignedURL(t *testing.T) {
	ctx := context.Background()

	t.Run("Returns non-presign urls", func(t *testing.T) {
		awsService := AWS{
			bucket:         "test-bucket",
			presignEnabled: false,
			useAccelerate:  false,
			region:         "eu-west-1",
		}

		urlStr, headUrlStr, err := awsService.GetOIDPreSignedURL(ctx, "test-oid")
		assert.NoError(t, err)
		assert.Equal(t, "https://test-bucket.s3.eu-west-1.amazonaws.com/test-oid", urlStr)
		assert.Equal(t, "https://test-bucket.s3.eu-west-1.amazonaws.com/test-oid", headUrlStr)
	})

	t.Run("Returns non-presign s3 accelerate urls", func(t *testing.T) {
		awsService := AWS{
			bucket:         "test-bucket",
			presignEnabled: false,
			useAccelerate:  true,
			region:         "eu-west-1",
		}

		urlStr, headUrlStr, err := awsService.GetOIDPreSignedURL(ctx, "test-oid")
		assert.NoError(t, err)
		assert.Equal(t, "https://test-bucket.s3-accelerate.amazonaws.com/test-oid", urlStr)
		assert.Equal(t, "https://test-bucket.s3-accelerate.amazonaws.com/test-oid", headUrlStr)
	})

	t.Run("Returns presign s3 urls", func(t *testing.T) {
		counter := new(int)
		awsService := AWS{
			bucket:            "test-bucket",
			presignEnabled:    true,
			useAccelerate:     false,
			presignExpiration: 1 * time.Hour,
			region:            "eu-west-1",
			presigner:         mockPresignClient{called: counter},
		}

		getURL, headURL, err := awsService.GetOIDPreSignedURL(ctx, "test-oid")
		assert.NoError(t, err)
		assert.Equal(t, 2, *counter)
		assert.Equal(t, "https://presigned.example.com/get/test-oid", getURL)
		assert.Equal(t, "https://presigned.example.com/head/test-oid", headURL)
	})
}
