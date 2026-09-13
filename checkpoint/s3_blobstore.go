package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3BlobAPI is the subset of the AWS S3 client used by S3Blobstore.
type S3BlobAPI interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// S3Blobstore stores savepoint/checkpoint blobs in S3 or an S3-compatible
// object store. Objects receive a sha256 metadata value for integrity auditing.
type S3Blobstore struct {
	bucket      string
	prefix      string
	client      S3BlobAPI
	sse         types.ServerSideEncryption
	kmsKeyID    string
	tmpDir      string
	maxAttempts int
}

type S3BlobstoreOption func(*s3BlobstoreConfig)

type s3BlobstoreConfig struct {
	bucket       string
	prefix       string
	region       string
	endpoint     string
	pathStyle    bool
	accessKey    string
	secretKey    string
	sessionToken string
	client       S3BlobAPI
	sse          types.ServerSideEncryption
	kmsKeyID     string
	tmpDir       string
	maxAttempts  int
}

func S3BlobBucket(bucket string) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.bucket = bucket }
}

func S3BlobPrefix(prefix string) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.prefix = strings.Trim(prefix, "/") }
}

func S3BlobRegion(region string) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.region = region }
}

func S3BlobEndpoint(endpoint string) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.endpoint = endpoint }
}

func S3BlobPathStyle() S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.pathStyle = true }
}

func S3BlobStaticCredentials(accessKey, secretKey, sessionToken string) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) {
		c.accessKey, c.secretKey, c.sessionToken = accessKey, secretKey, sessionToken
	}
}

func S3BlobClient(client S3BlobAPI) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.client = client }
}

func S3BlobSSE(sse types.ServerSideEncryption, kmsKeyID string) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.sse, c.kmsKeyID = sse, kmsKeyID }
}

func S3BlobTempDir(dir string) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.tmpDir = dir }
}

func S3BlobMaxAttempts(n int) S3BlobstoreOption {
	return func(c *s3BlobstoreConfig) { c.maxAttempts = n }
}

func NewS3Blobstore(opts ...S3BlobstoreOption) (*S3Blobstore, error) {
	cfg := s3BlobstoreConfig{maxAttempts: 3}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.bucket == "" {
		return nil, fmt.Errorf("checkpoint: S3 blobstore requires bucket")
	}
	client := cfg.client
	if client == nil {
		awsCfg, err := loadS3BlobAWSConfig(cfg)
		if err != nil {
			return nil, err
		}
		client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if cfg.endpoint != "" {
				o.BaseEndpoint = aws.String(cfg.endpoint)
			}
			o.UsePathStyle = cfg.pathStyle
		})
	}
	return &S3Blobstore{
		bucket: cfg.bucket, prefix: cfg.prefix, client: client,
		sse: cfg.sse, kmsKeyID: cfg.kmsKeyID, tmpDir: cfg.tmpDir,
		maxAttempts: cfg.maxAttempts,
	}, nil
}

func loadS3BlobAWSConfig(cfg s3BlobstoreConfig) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{}
	if cfg.region != "" {
		opts = append(opts, config.WithRegion(cfg.region))
	}
	if cfg.maxAttempts > 0 {
		opts = append(opts, config.WithRetryMaxAttempts(cfg.maxAttempts))
	}
	if cfg.accessKey != "" || cfg.secretKey != "" || cfg.sessionToken != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.accessKey, cfg.secretKey, cfg.sessionToken)))
	}
	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("checkpoint: S3 blobstore load config: %w", err)
	}
	return awsCfg, nil
}

func (b *S3Blobstore) Put(key string, r io.Reader) error {
	objectKey, err := b.objectKey(key)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(b.tmpDir, "weibo-s3-blob-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	sum := sha256.New()
	if _, err := io.Copy(tmp, io.TeeReader(r, sum)); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		return err
	}
	in := &s3.PutObjectInput{
		Bucket:   aws.String(b.bucket),
		Key:      aws.String(objectKey),
		Body:     tmp,
		Metadata: map[string]string{"sha256": hex.EncodeToString(sum.Sum(nil))},
	}
	if b.sse != "" {
		in.ServerSideEncryption = b.sse
	}
	if b.kmsKeyID != "" {
		in.SSEKMSKeyId = aws.String(b.kmsKeyID)
	}
	_, err = b.client.PutObject(context.Background(), in)
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (b *S3Blobstore) Get(key string) (io.ReadCloser, error) {
	objectKey, err := b.objectKey(key)
	if err != nil {
		return nil, err
	}
	out, err := b.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(objectKey)})
	if isS3NotFound(err) {
		return nil, ErrBlobNotFound
	}
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (b *S3Blobstore) Exists(key string) (bool, error) {
	objectKey, err := b.objectKey(key)
	if err != nil {
		return false, err
	}
	_, err = b.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(objectKey)})
	if isS3NotFound(err) {
		return false, nil
	}
	return err == nil, err
}

func (b *S3Blobstore) List(prefix string) ([]string, error) {
	objectPrefix, err := b.objectKey(prefix)
	if err != nil {
		return nil, err
	}
	var keys []string
	var token *string
	for {
		out, err := b.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket: aws.String(b.bucket), Prefix: aws.String(objectPrefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			keys = append(keys, b.externalKey(aws.ToString(obj.Key)))
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

func (b *S3Blobstore) Delete(key string) error {
	objectKey, err := b.objectKey(key)
	if err != nil {
		return err
	}
	_, err = b.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(objectKey)})
	if isS3NotFound(err) {
		return nil
	}
	return err
}

func (b *S3Blobstore) objectKey(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("checkpoint: empty blob key")
	}
	if slices.Contains(strings.Split(key, "/"), "..") {
		return "", fmt.Errorf("checkpoint: invalid blob key %q", key)
	}
	clean := path.Clean("/" + key)
	clean = strings.TrimPrefix(clean, "/")
	if b.prefix == "" {
		return clean, nil
	}
	return b.prefix + "/" + clean, nil
}

func (b *S3Blobstore) externalKey(objectKey string) string {
	if b.prefix == "" {
		return objectKey
	}
	return strings.TrimPrefix(strings.TrimPrefix(objectKey, b.prefix), "/")
}

func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey")
}
