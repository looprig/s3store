//go:build integration && minio

// This file runs only against a disposable local MinIO that
// scripts/minio-test.sh starts (pinned by digest) and removes. It exists for
// D1: MinIO refuses an object-key path segment above 255 bytes, a limit the
// in-process fixture and AWS S3 do not have, so only a real MinIO proves the
// segmented manifest encoding.

package s3store

import (
	"bytes"
	"context"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type minioCredentials struct{ access, secret string }

func (c minioCredentials) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: c.access, SecretAccessKey: c.secret, Source: "s3store minio test"}, nil
}

func openMinio(t *testing.T) *Store {
	t.Helper()
	endpoint := os.Getenv("S3STORE_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3STORE_MINIO_ENDPOINT not set; run scripts/minio-test.sh")
	}
	options := Options{
		Endpoint: endpoint, Region: "us-east-1", Bucket: os.Getenv("S3STORE_MINIO_BUCKET"),
		DeploymentPrefix: "minio-d1", AddressingStyle: AddressingPath, Encryption: EncryptionBucketDefault,
		AllowInsecureLocalhostOnly: true,
		Credentials:                minioCredentials{os.Getenv("S3STORE_MINIO_ACCESS_KEY"), os.Getenv("S3STORE_MINIO_SECRET_KEY")},
	}
	// No caller deadline anywhere in this file: D2's default bound is what
	// keeps each call finite.
	store, err := Open(context.Background(), options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return store
}

// TestMinIORefusesAnOverlongSegment keeps the next test from being vacuous:
// the service under test really does enforce the 255-byte segment limit.
func TestMinIORefusesAnOverlongSegment(t *testing.T) {
	store := openMinio(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for length, wantOK := range map[int]bool{255: true, 256: false} {
		key := "minio-d1-witness/" + strings.Repeat("w", length)
		_, err := store.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(store.options.bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("w")),
		})
		if (err == nil) != wantOK {
			t.Fatalf("raw PutObject with a %d-byte segment: err=%v, want success=%v", length, err, wantOK)
		}
	}
}

func TestMinIOStoresLongLogicalKeys(t *testing.T) {
	store := openMinio(t)
	ctx := context.Background()
	lengths := []int{191, 192, 235, 300, 512}
	var keys []string
	for _, length := range lengths {
		head := "tenants/t/sessions/s/"
		key := head + strings.Repeat("k", length-len(head))
		if len(key) != length {
			t.Fatalf("fixture key is %d bytes, want %d", len(key), length)
		}
		body := []byte("value-" + key[len(key)-8:] + string(rune('a'+len(keys))))
		if err := store.Put(ctx, key, bytes.NewReader(body)); err != nil {
			t.Fatalf("Put %d-byte key: %v", length, err)
		}
		reader, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get %d-byte key: %v", length, err)
		}
		got, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("Get %d-byte key = (%q, %v), want %q", length, got, err, body)
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	listed, err := store.List(ctx, "tenants/t/")
	if err != nil || !slices.Equal(listed, keys) {
		t.Fatalf("List = (%d keys, %v), want %d keys in order", len(listed), err, len(keys))
	}
	for _, key := range keys {
		if err := store.Delete(ctx, key); err != nil {
			t.Fatalf("Delete %d-byte key: %v", len(key), err)
		}
	}
	if listed, err := store.List(ctx, "tenants/t/"); err != nil || len(listed) != 0 {
		t.Fatalf("List after Delete = (%v, %v), want empty", listed, err)
	}
}
