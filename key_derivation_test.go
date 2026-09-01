package s3store

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/looprig/storage"
)

func TestBackendManifestKeyIsCanonicalAndReversible(t *testing.T) {
	t.Parallel()
	key := strings.Repeat("a", 512)
	prefix := strings.Repeat("p", 256)

	backend, err := manifestObjectKey(prefix, key)
	if err != nil {
		t.Fatalf("manifestObjectKey: %v", err)
	}
	if len(backend) > maxS3ObjectKeyBytes {
		t.Fatalf("backend key length = %d, want at most %d", len(backend), maxS3ObjectKeyBytes)
	}
	wantDigest := sha256.Sum256([]byte(key))
	wantSuffix := hex.EncodeToString(wantDigest[:]) + "/" + base64.RawURLEncoding.EncodeToString([]byte(key))
	if !strings.HasPrefix(backend, prefix+"/blobs/v1/") || !strings.HasSuffix(backend, wantSuffix) {
		t.Fatalf("backend key = %q, want versioned prefix and hash/encoding suffix %q", backend, wantSuffix)
	}

	decoded, ok := logicalKeyFromManifestObject(prefix, backend)
	if !ok || decoded != key {
		t.Fatalf("logicalKeyFromManifestObject = (%q, %v), want (%q, true)", decoded, ok, key)
	}
}

func TestLogicalObjectKeyMaximumOnBothSides(t *testing.T) {
	t.Parallel()
	for _, length := range []int{511, 512} {
		key := strings.Repeat("a", length)
		if _, err := manifestObjectKey("deployments/test", key); err != nil {
			t.Errorf("manifestObjectKey length %d: %v", length, err)
		}
	}
	_, err := manifestObjectKey("deployments/test", strings.Repeat("a", 513))
	var invalid *storage.InvalidNameError
	if !errors.As(err, &invalid) {
		t.Fatalf("513-byte logical key error = %T %v, want *storage.InvalidNameError", err, err)
	}
	// Multi-byte input is rejected by the canonical ASCII grammar itself, so it
	// cannot smuggle a character count past the byte limit.
	_, err = manifestObjectKey("deployments/test", strings.Repeat("é", 256))
	if !errors.As(err, &invalid) {
		t.Fatalf("512-byte Unicode logical key error = %T %v, want *storage.InvalidNameError", err, err)
	}
	_, err = manifestObjectKey(strings.Repeat("p", 512), strings.Repeat("a", 512))
	var optionsErr *OptionsError
	if !errors.As(err, &optionsErr) {
		t.Fatalf("combined backend key above 1024 bytes error = %T %v, want *OptionsError", err, err)
	}
}

func TestBackendManifestKeyRejectsPrefixAndObjectInjection(t *testing.T) {
	t.Parallel()
	bad := []string{"/", ":", "UPPER", "é", "..", "%2f", "a//b", "a/../b"}
	for _, candidate := range bad {
		candidate := candidate
		t.Run("object/"+base64.RawURLEncoding.EncodeToString([]byte(candidate)), func(t *testing.T) {
			_, err := manifestObjectKey("deployments/test", candidate)
			var invalid *storage.InvalidNameError
			if !errors.As(err, &invalid) {
				t.Fatalf("manifestObjectKey object %q error = %T %v, want *storage.InvalidNameError", candidate, err, err)
			}
		})
		t.Run("prefix/"+base64.RawURLEncoding.EncodeToString([]byte(candidate)), func(t *testing.T) {
			_, err := manifestObjectKey(candidate, "blobs/key")
			var invalid *storage.InvalidNameError
			if !errors.As(err, &invalid) {
				t.Fatalf("manifestObjectKey prefix %q error = %T %v, want *storage.InvalidNameError", candidate, err, err)
			}
		})
	}
}

func TestLogicalKeyFromManifestObjectFailsClosedPerRow(t *testing.T) {
	t.Parallel()
	prefix := "deployments/test"
	valid, err := manifestObjectKey(prefix, "blobs/good")
	if err != nil {
		t.Fatal(err)
	}
	rows := []string{
		valid,
		prefix + "/blobs/v1/not-a-digest/" + base64.RawURLEncoding.EncodeToString([]byte("blobs/good")),
		prefix + "/blobs/v1/" + strings.Repeat("0", 64) + "/" + base64.RawURLEncoding.EncodeToString([]byte("blobs/good")),
		prefix + "/blobs/v1/" + strings.Repeat("0", 64) + "/%%%",
		prefix + "/blobs/v1/" + strings.Repeat("0", 64) + "/" + base64.RawURLEncoding.EncodeToString([]byte("../escape")),
		"other/blobs/v1/" + strings.Repeat("0", 64) + "/ignored",
	}
	var got []string
	for _, row := range rows {
		if logical, ok := logicalKeyFromManifestObject(prefix, row); ok {
			got = append(got, logical)
		}
	}
	if len(got) != 1 || got[0] != "blobs/good" {
		t.Fatalf("decoded rows = %v, want only blobs/good", got)
	}
}
