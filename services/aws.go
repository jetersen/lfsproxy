package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/jetersen/lfsproxy/manifest"
)

type s3API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type s3UploadAPI interface {
	UploadObject(ctx context.Context, input *transfermanager.UploadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
}

type AWSService interface {
	OIDExists(ctx context.Context, oid string) (bool, error)
	GetDirectURL(oid string) string
	PresignObject(ctx context.Context, oid string, ttl time.Duration) (url string, expiresAt time.Time, err error)
	UploadOID(ctx context.Context, oid string, body io.ReadCloser) error
	LoadManifest(ctx context.Context) (oids []string, bootstrapped bool, err error)
	SaveManifest(ctx context.Context, oids []string) error
}

type AWS struct {
	bucket         string
	useAccelerate  bool
	client         s3API
	uploader       s3UploadAPI
	presignClient  *s3.PresignClient
	credentials    aws.CredentialsProvider
	manifestClient manifest.S3Client
	region         string
}

func NewAWSService(ctx context.Context, bucket string, useAccelerate bool) (AWSService, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UseAccelerate = useAccelerate
	})

	return &AWS{
		bucket:         bucket,
		useAccelerate:  useAccelerate,
		client:         client,
		uploader:       transfermanager.New(client),
		presignClient:  s3.NewPresignClient(client),
		credentials:    cfg.Credentials,
		manifestClient: client,
		region:         cfg.Region,
	}, nil
}

func (a *AWS) OIDExists(ctx context.Context, oid string) (bool, error) {
	_, err := a.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(oid),
	})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (a *AWS) GetDirectURL(oid string) string {
	return a.bucket + "/" + oid
}

func (a *AWS) PresignObject(ctx context.Context, oid string, ttl time.Duration) (string, time.Time, error) {
	if a.credentials != nil {
		creds, err := a.credentials.Retrieve(ctx)
		if err != nil {
			return "", time.Time{}, err
		}
		ttl = capTTLToCredentials(time.Now(), ttl, creds)
		if ttl <= 0 {
			return "", time.Time{}, errors.New("presign: credentials expired")
		}
	}
	req, err := a.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(oid),
	}, func(o *s3.PresignOptions) {
		o.Expires = ttl
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return req.URL, time.Now().Add(ttl), nil
}

// capTTLToCredentials limits ttl to the credentials' lifetime: S3 rejects a presigned
// URL with ExpiredToken once the session credentials that signed it expire.
func capTTLToCredentials(now time.Time, ttl time.Duration, creds aws.Credentials) time.Duration {
	if creds.CanExpire {
		if remaining := creds.Expires.Sub(now); remaining < ttl {
			return remaining
		}
	}
	return ttl
}

func (a *AWS) UploadOID(ctx context.Context, oid string, body io.ReadCloser) error {
	defer func() { _ = body.Close() }()

	_, err := a.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(oid),
		Body:   body,
	})
	if err != nil {
		log.Printf("error uploading: %v\n", err.Error())
		return err
	}

	return nil
}

func (a *AWS) LoadManifest(ctx context.Context) ([]string, bool, error) {
	oids, err := manifest.Load(ctx, a.manifestClient, a.bucket)
	if errors.Is(err, manifest.ErrNotFound) {
		oids, err = a.bootstrapManifest(ctx)
		return oids, true, err
	}
	return oids, false, err
}

func (a *AWS) bootstrapManifest(ctx context.Context) ([]string, error) {
	var oids []string
	var continuationToken *string

	for {
		result, err := a.manifestClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(a.bucket),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("manifest: list objects: %w", err)
		}
		for _, obj := range result.Contents {
			if key := aws.ToString(obj.Key); key != manifest.Key {
				oids = append(oids, key)
			}
		}
		if !aws.ToBool(result.IsTruncated) {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return oids, nil
}

func (a *AWS) SaveManifest(ctx context.Context, oids []string) error {
	return manifest.Save(ctx, a.manifestClient, a.bucket, oids)
}
