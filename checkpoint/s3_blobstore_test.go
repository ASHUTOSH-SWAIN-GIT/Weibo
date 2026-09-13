package checkpoint_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeBlobS3 struct {
	objects map[string][]byte
	meta    map[string]map[string]string
	sse     map[string]types.ServerSideEncryption
	kms     map[string]string
}

func newFakeBlobS3() *fakeBlobS3 {
	return &fakeBlobS3{objects: map[string][]byte{}, meta: map[string]map[string]string{}, sse: map[string]types.ServerSideEncryption{}, kms: map[string]string{}}
}

func (f *fakeBlobS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	key := aws.ToString(in.Key)
	f.objects[key] = body
	f.meta[key] = in.Metadata
	f.sse[key] = in.ServerSideEncryption
	f.kms[key] = aws.ToString(in.SSEKMSKeyId)
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeBlobS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	body, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func (f *fakeBlobS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if _, ok := f.objects[aws.ToString(in.Key)]; !ok {
		return nil, &types.NotFound{}
	}
	return &s3.HeadObjectOutput{}, nil
}

func (f *fakeBlobS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(in.Prefix)
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			out.Contents = append(out.Contents, types.Object{Key: aws.String(key)})
		}
	}
	sort.Slice(out.Contents, func(i, j int) bool { return aws.ToString(out.Contents[i].Key) < aws.ToString(out.Contents[j].Key) })
	return out, nil
}

func (f *fakeBlobS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	delete(f.objects, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func TestS3BlobstoreRoundTripMetadataAndList(t *testing.T) {
	fake := newFakeBlobS3()
	bs, err := checkpoint.NewS3Blobstore(
		checkpoint.S3BlobBucket("weibo"),
		checkpoint.S3BlobPrefix("prod"),
		checkpoint.S3BlobClient(fake),
		checkpoint.S3BlobSSE(types.ServerSideEncryptionAwsKms, "key-1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Put("savepoints/before", strings.NewReader("checkpoint bytes")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := bs.Get("savepoints/before")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "checkpoint bytes" {
		t.Fatalf("body=%q", got)
	}
	key := "prod/savepoints/before"
	sum := sha256.Sum256([]byte("checkpoint bytes"))
	if fake.meta[key]["sha256"] != hex.EncodeToString(sum[:]) {
		t.Fatalf("sha metadata=%v", fake.meta[key])
	}
	if fake.sse[key] != types.ServerSideEncryptionAwsKms || fake.kms[key] != "key-1" {
		t.Fatalf("sse/kms = %q/%q", fake.sse[key], fake.kms[key])
	}
	exists, err := bs.Exists("savepoints/before")
	if err != nil || !exists {
		t.Fatalf("Exists=%v err=%v", exists, err)
	}
	keys, err := bs.List("savepoints/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "savepoints/before" {
		t.Fatalf("List=%v", keys)
	}
	if err := bs.Delete("savepoints/before"); err != nil {
		t.Fatal(err)
	}
	exists, err = bs.Exists("savepoints/before")
	if err != nil || exists {
		t.Fatalf("Exists after delete=%v err=%v", exists, err)
	}
}

func TestS3BlobstoreGuardsAndMissing(t *testing.T) {
	bs, err := checkpoint.NewS3Blobstore(checkpoint.S3BlobBucket("weibo"), checkpoint.S3BlobClient(newFakeBlobS3()))
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Put("../escape", strings.NewReader("x")); err == nil {
		t.Fatal("expected traversal guard")
	}
	if _, err := bs.Get("missing"); !errors.Is(err, checkpoint.ErrBlobNotFound) {
		t.Fatalf("missing=%v, want ErrBlobNotFound", err)
	}
}
