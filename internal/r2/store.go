package r2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config contains Cloudflare R2 connection settings.
type Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
}

// Store writes objects to Cloudflare R2.
type Store struct {
	client *s3.Client
	bucket string
}

// ObjectInfo describes one bucket object.
type ObjectInfo struct {
	Key          string
	SizeBytes    int64
	ETag         string
	LastModified time.Time
}

// NewStore creates an R2-backed store.
func NewStore(ctx context.Context, cfg Config) (*Store, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	region := strings.TrimSpace(cfg.Region)
	bucket := strings.TrimSpace(cfg.Bucket)
	accessKeyID := strings.TrimSpace(cfg.AccessKeyID)
	secretAccessKey := strings.TrimSpace(cfg.SecretAccessKey)

	if endpoint == "" {
		return nil, fmt.Errorf("r2 endpoint is required")
	}
	if region == "" {
		region = "auto"
	}
	if bucket == "" {
		return nil, fmt.Errorf("r2 bucket is required")
	}
	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("r2 access key id and secret key are required")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = &endpoint
	})

	return &Store{
		client: client,
		bucket: bucket,
	}, nil
}

// Bucket returns the configured bucket name.
func (s *Store) Bucket() string {
	if s == nil {
		return ""
	}
	return s.bucket
}

// Put uploads one object with an explicit content type.
func (s *Store) Put(ctx context.Context, objectKey string, payload []byte, contentType string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}

	objectKey = normalizeObjectKey(objectKey)
	if objectKey == "" {
		return fmt.Errorf("object key is required")
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &objectKey,
		Body:          bytes.NewReader(payload),
		ContentLength: aws.Int64(int64(len(payload))),
		ContentType:   aws.String(contentType),
		CacheControl:  aws.String("private, max-age=0, no-store"),
	})
	if err != nil {
		return fmt.Errorf("put object %q: %w", objectKey, err)
	}
	return nil
}

// Get downloads one object payload and returns object metadata.
func (s *Store) Get(ctx context.Context, objectKey string) ([]byte, ObjectInfo, error) {
	if err := s.ensureReady(); err != nil {
		return nil, ObjectInfo{}, err
	}

	objectKey = normalizeObjectKey(objectKey)
	if objectKey == "" {
		return nil, ObjectInfo{}, fmt.Errorf("object key is required")
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &objectKey,
	})
	if err != nil {
		return nil, ObjectInfo{}, fmt.Errorf("get object %q: %w", objectKey, err)
	}
	defer out.Body.Close()

	payload, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, ObjectInfo{}, fmt.Errorf("read object %q: %w", objectKey, err)
	}

	lastModified := time.Time{}
	if out.LastModified != nil {
		lastModified = out.LastModified.UTC()
	}

	return payload, ObjectInfo{
		Key:          objectKey,
		SizeBytes:    aws.ToInt64(out.ContentLength),
		ETag:         strings.Trim(strings.TrimSpace(aws.ToString(out.ETag)), "\""),
		LastModified: lastModified,
	}, nil
}

// List returns one page of objects under the prefix, plus a continuation token.
func (s *Store) List(ctx context.Context, prefix, continuationToken string, maxKeys int32) ([]ObjectInfo, string, error) {
	if err := s.ensureReady(); err != nil {
		return nil, "", err
	}

	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	continuationToken = strings.TrimSpace(continuationToken)
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	in := &s3.ListObjectsV2Input{
		Bucket:  &s.bucket,
		MaxKeys: aws.Int32(maxKeys),
	}
	if prefix != "" {
		in.Prefix = aws.String(prefix + "/")
	}
	if continuationToken != "" {
		in.ContinuationToken = aws.String(continuationToken)
	}

	out, err := s.client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, "", fmt.Errorf("list objects in bucket %q: %w", s.bucket, err)
	}

	objects := make([]ObjectInfo, 0, len(out.Contents))
	for _, obj := range out.Contents {
		key := strings.TrimSpace(aws.ToString(obj.Key))
		if key == "" {
			continue
		}

		lastModified := time.Time{}
		if obj.LastModified != nil {
			lastModified = obj.LastModified.UTC()
		}

		objects = append(objects, ObjectInfo{
			Key:          key,
			SizeBytes:    aws.ToInt64(obj.Size),
			ETag:         strings.Trim(strings.TrimSpace(aws.ToString(obj.ETag)), "\""),
			LastModified: lastModified,
		})
	}

	next := strings.TrimSpace(aws.ToString(out.NextContinuationToken))
	return objects, next, nil
}

func normalizeObjectKey(objectKey string) string {
	return strings.Trim(strings.TrimSpace(objectKey), "/")
}

func (s *Store) ensureReady() error {
	if s == nil || s.client == nil {
		return fmt.Errorf("r2 store is not configured")
	}
	if strings.TrimSpace(s.bucket) == "" {
		return fmt.Errorf("r2 bucket is not configured")
	}
	return nil
}
