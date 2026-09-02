//go:build integration

package s3store

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/looprig/s3store/internal/testserver"
)

const (
	sseHeader       = "X-Amz-Server-Side-Encryption"
	sseKMSKeyHeader = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
	testKMSKeyID    = "alias/looprig-test-key"
)

// TestEveryObjectCreatingRequestCarriesTheConfiguredEncryptionHeader is the
// evidence behind EncryptionMode.confirmedByThisModule. It checks every request
// that commits object bytes, on both the single-part and the multipart write
// path, for both the payload and the manifest — not one representative request.
//
// It does not, and cannot, verify a bucket policy: the fixture accepts whatever
// it is sent. That half of the requirement is the cloud-tagged test, and the
// deployment-policy refusal in Open is what stands in for it until then.
func TestEveryObjectCreatingRequestCarriesTheConfiguredEncryptionHeader(t *testing.T) {
	postures := []struct {
		name       string
		mode       EncryptionMode
		kmsKeyID   string
		wantSSE    string
		wantKMSKey string
	}{
		{name: "aes256", mode: EncryptionAES256, wantSSE: "AES256"},
		{name: "kms", mode: EncryptionKMS, kmsKeyID: testKMSKeyID, wantSSE: "aws:kms", wantKMSKey: testKMSKeyID},
		// The other direction: a posture s3store does not configure must send
		// no header at all, so a bucket policy is not silently overridden.
		{name: "bucket default", mode: EncryptionBucketDefault, wantSSE: ""},
	}
	writes := []struct {
		name      string
		body      []byte
		multipart bool
	}{
		{name: "single part", body: []byte("small-object")},
		{name: "multipart", body: bytes.Repeat([]byte("m"), int(minMultipartBytes+1)), multipart: true},
	}
	for _, posture := range postures {
		for _, write := range writes {
			t.Run(posture.name+"/"+write.name, func(t *testing.T) {
				server := testserver.New()
				t.Cleanup(server.Close)
				store := newIntegrationStore(t, server, func(options *Options) {
					options.Encryption = posture.mode
					options.KMSKeyID = posture.kmsKeyID
					if write.multipart {
						options.MultipartThreshold = minMultipartBytes
						options.MultipartPartSize = minMultipartBytes
						options.Concurrency = 1
						options.MaxConcurrentTransfers = 1
					}
				})
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := store.Put(ctx, "blobs/encrypted", bytes.NewReader(write.body)); err != nil {
					t.Fatalf("Put: %v", err)
				}
				payloadOperation := "PutPayload"
				if write.multipart {
					payloadOperation = "CreateMultipartUpload"
				}
				for _, operation := range []string{payloadOperation, "PutManifest"} {
					headers := server.ObjectHeaders(operation)
					if len(headers) == 0 {
						t.Fatalf("no %s request reached the fixture; the header assertion would be vacuous", operation)
					}
					for index, header := range headers {
						if got := header.Get(sseHeader); got != posture.wantSSE {
							t.Errorf("%s[%d] %s = %q, want %q", operation, index, sseHeader, got, posture.wantSSE)
						}
						if got := header.Get(sseKMSKeyHeader); got != posture.wantKMSKey {
							t.Errorf("%s[%d] %s = %q, want %q", operation, index, sseKMSKeyHeader, got, posture.wantKMSKey)
						}
					}
				}
			})
		}
	}
}

// TestConfirmedEncryptionRequirementDoesNotChangeTheBytesOnTheWire pins that
// RequireConfirmedEncryption is a startup gate and nothing else. Without this
// row the option could be implemented as a second, divergent source of the
// header and the gate test above would still pass.
func TestConfirmedEncryptionRequirementDoesNotChangeTheBytesOnTheWire(t *testing.T) {
	headersFor := func(required bool) []http.Header {
		server := testserver.New()
		t.Cleanup(server.Close)
		store := newIntegrationStore(t, server, func(options *Options) {
			options.Encryption = EncryptionAES256
			options.RequireConfirmedEncryption = required
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.Put(ctx, "blobs/gated", strings.NewReader("bytes")); err != nil {
			t.Fatalf("Put(required=%v): %v", required, err)
		}
		return append(server.ObjectHeaders("PutPayload"), server.ObjectHeaders("PutManifest")...)
	}
	ungated, gated := headersFor(false), headersFor(true)
	if len(ungated) != len(gated) || len(gated) == 0 {
		t.Fatalf("object-creating requests = %d ungated, %d gated; want an equal positive count", len(ungated), len(gated))
	}
	for index := range gated {
		if got, want := gated[index].Get(sseHeader), ungated[index].Get(sseHeader); got != want {
			t.Errorf("request %d %s = %q gated, %q ungated; the requirement must not alter the wire", index, sseHeader, got, want)
		}
	}
}
