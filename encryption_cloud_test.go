//go:build cloud

// This file is behind the `cloud` build tag and is compiled by neither
// `make check` nor `go test ./...` nor `go test -tags integration ./...`. It is
// the only test in this module that would contact a service it did not start,
// so it is opt-in twice: the tag selects it, and it still skips unless a
// deployment names an endpoint and bucket in the environment. It reads no
// credential from the environment itself — the AWS SDK's standard chain does
// that — and asserts nothing that would put a bucket name or key identifier
// into a failure message.
//
// It has not been executed. Running it requires an S3-compatible endpoint that
// this repository is not permitted to create.

package s3store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type cloudTarget struct {
	endpoint string
	region   string
	bucket   string
	prefix   string
	kmsKeyID string
}

func cloudTargetFromEnvironment(t *testing.T) cloudTarget {
	t.Helper()
	target := cloudTarget{
		endpoint: os.Getenv("S3STORE_CLOUD_ENDPOINT"),
		region:   os.Getenv("S3STORE_CLOUD_REGION"),
		bucket:   os.Getenv("S3STORE_CLOUD_BUCKET"),
		prefix:   os.Getenv("S3STORE_CLOUD_PREFIX"),
		kmsKeyID: os.Getenv("S3STORE_CLOUD_KMS_KEY_ID"),
	}
	if target.endpoint == "" || target.bucket == "" {
		t.Skip("cloud target not configured; set S3STORE_CLOUD_ENDPOINT and S3STORE_CLOUD_BUCKET")
	}
	if target.region == "" {
		target.region = "us-east-1"
	}
	if target.prefix == "" {
		target.prefix = "s3store-cloud-check"
	}
	return target
}

// TestCloudServiceReportsTheRequestedServerSideEncryption verifies against a
// live service the half of the requirement the in-process fixture cannot: that
// the service actually applied encryption, rather than merely accepting the
// header. It covers the requested postures and, separately, the bucket-default
// posture whose enforcement is a bucket policy.
func TestCloudServiceReportsTheRequestedServerSideEncryption(t *testing.T) {
	target := cloudTargetFromEnvironment(t)
	cases := []struct {
		name string
		mode EncryptionMode
		want types.ServerSideEncryption
	}{
		{name: "aes256", mode: EncryptionAES256, want: types.ServerSideEncryptionAes256},
		{name: "kms", mode: EncryptionKMS, want: types.ServerSideEncryptionAwsKms},
		// EncryptionBucketDefault sends no header. Whatever the service reports
		// here IS the bucket policy, which is the thing Open cannot confirm.
		{name: "bucket default", mode: EncryptionBucketDefault},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.mode == EncryptionKMS && target.kmsKeyID == "" {
				t.Skip("S3STORE_CLOUD_KMS_KEY_ID not configured")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			options := Options{
				Endpoint: target.endpoint, Region: target.region, Bucket: target.bucket,
				DeploymentPrefix: target.prefix, AddressingStyle: AddressingPath,
				Encryption: testCase.mode,
			}
			if testCase.mode == EncryptionKMS {
				options.KMSKeyID = target.kmsKeyID
			}
			store, err := Open(ctx, options)
			if err != nil {
				t.Fatalf("Open: %v", RedactedErrorText(err))
			}
			key := "cloudcheck/" + randomCloudSuffix(t)
			if err := store.Put(ctx, key, strings.NewReader("cloud-encryption-probe")); err != nil {
				t.Fatalf("Put: %v", RedactedErrorText(err))
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				_ = store.Delete(cleanupCtx, key)
			})
			manifestKey, err := manifestObjectKey(target.prefix, key)
			if err != nil {
				t.Fatalf("manifestObjectKey: %v", RedactedErrorText(err))
			}
			head, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(target.bucket), Key: aws.String(manifestKey),
			})
			if err != nil {
				t.Fatal("HeadObject on the committed manifest failed")
			}
			if testCase.want != "" {
				if head.ServerSideEncryption != testCase.want {
					t.Fatalf("service reported server-side encryption %q, want %q", head.ServerSideEncryption, testCase.want)
				}
				return
			}
			// The bucket-default row is a report, not a pass/fail on the
			// service: a bucket that enforces nothing is a real deployment
			// state, and it is exactly the state RequireConfirmedEncryption
			// refuses to start against.
			if head.ServerSideEncryption == "" {
				t.Fatal("the bucket applied no default server-side encryption; a deployment requiring encryption must not use EncryptionBucketDefault against this bucket")
			}
		})
	}
}

// TestCloudDeploymentPolicyRefusesTheUnconfirmablePosture is the live-service
// statement of the startup rule: even against a real endpoint, Open refuses the
// bucket-default posture under a confirmation requirement, because Open issues
// no request and so confirms nothing. It contacts no service and is here rather
// than in the default suite only so a cloud run reports it alongside the rows
// above.
func TestCloudDeploymentPolicyRefusesTheUnconfirmablePosture(t *testing.T) {
	target := cloudTargetFromEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := Open(ctx, Options{
		Endpoint: target.endpoint, Region: target.region, Bucket: target.bucket,
		DeploymentPrefix: target.prefix, AddressingStyle: AddressingPath,
		Encryption: EncryptionBucketDefault, RequireConfirmedEncryption: true,
	})
	var policy *EncryptionPolicyError
	if !errors.As(err, &policy) {
		t.Fatalf("Open = %v, want *EncryptionPolicyError", RedactedErrorText(err))
	}
}

func randomCloudSuffix(t *testing.T) string {
	t.Helper()
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatal("random suffix generation failed")
	}
	return hex.EncodeToString(raw[:])
}
