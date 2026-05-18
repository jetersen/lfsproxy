package manifest

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const Key = ".lfsproxy-manifest"

type S3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func Parse(r io.Reader) ([]string, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("manifest: decompress: %w", err)
	}
	defer gz.Close()

	var oids []string
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			oids = append(oids, line)
		}
	}
	return oids, scanner.Err()
}

func Write(w io.Writer, oids []string) error {
	gz := gzip.NewWriter(w)
	for _, oid := range oids {
		fmt.Fprintln(gz, oid) //nolint:errcheck
	}
	return gz.Close()
}

// ErrNotFound is returned by Load when the manifest object does not exist in S3.
var ErrNotFound = errors.New("manifest: not found")

func Load(ctx context.Context, client S3Client, bucket string) ([]string, error) {
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(Key),
	})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && (ae.ErrorCode() == "NoSuchKey" || ae.ErrorCode() == "NotFound") {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer result.Body.Close()
	return Parse(result.Body)
}

func Save(ctx context.Context, client S3Client, bucket string, oids []string) error {
	var buf bytes.Buffer
	if err := Write(&buf, oids); err != nil {
		return err
	}
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(Key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/gzip"),
	})
	return err
}

