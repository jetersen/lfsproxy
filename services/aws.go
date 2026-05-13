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
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/smithy-go"
)

type s3API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type s3PresignAPI interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
	PresignHeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

type s3UploadAPI interface {
	UploadObject(ctx context.Context, input *transfermanager.UploadObjectInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)
}

type AWSService interface {
	OIDExists(ctx context.Context, oid string) (bool, error)
	GetOIDPreSignedURL(ctx context.Context, oid string) (string, string, error)
	UploadOID(ctx context.Context, oid string, body io.ReadCloser) error
}

type AWS struct {
	bucket            string
	useAccelerate     bool
	presignEnabled    bool
	presignExpiration time.Duration
	client            s3API
	presigner         s3PresignAPI
	uploader          s3UploadAPI
	region            string
}

func NewAWSService(ctx context.Context, bucket string, useAccelerate bool, presignEnabled bool, presignExpiration time.Duration) (AWSService, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UseAccelerate = useAccelerate
	})

	return &AWS{
		bucket:            bucket,
		useAccelerate:     useAccelerate,
		presignEnabled:    presignEnabled,
		presignExpiration: presignExpiration,
		client:            client,
		presigner:         s3.NewPresignClient(client),
		uploader:          transfermanager.New(client),
		region:            cfg.Region,
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

func (a *AWS) GetOIDPreSignedURL(ctx context.Context, oid string) (string, string, error) {
	if a.presignEnabled {
		getResult, err := a.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(a.bucket),
			Key:    aws.String(oid),
		}, s3.WithPresignExpires(a.presignExpiration))
		if err != nil {
			return "", "", err
		}

		headResult, err := a.presigner.PresignHeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(a.bucket),
			Key:    aws.String(oid),
		}, s3.WithPresignExpires(a.presignExpiration))
		if err != nil {
			return "", "", err
		}

		return getResult.URL, headResult.URL, nil
	}

	if a.useAccelerate {
		url := fmt.Sprintf("https://%s.s3-accelerate.amazonaws.com/%s", a.bucket, oid)
		return url, url, nil
	}

	url := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", a.bucket, a.region, oid)
	return url, url, nil
}

func (a *AWS) UploadOID(ctx context.Context, oid string, body io.ReadCloser) error {
	defer body.Close()

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
