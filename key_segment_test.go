package s3store

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
)

// TestManifestKeyShortFormIsTheV011Encoding pins the compatibility promise:
// a logical key whose encoding fits one MinIO path segment (at most 191 bytes)
// maps to exactly the backend key v0.1.1 wrote, so every object those
// releases stored stays addressable without migration.
func TestManifestKeyShortFormIsTheV011Encoding(t *testing.T) {
	t.Parallel()
	prefix := "deployments/test"
	for _, length := range []int{1, 64, 190, 191} {
		key := strings.Repeat("k", length)
		got, err := manifestObjectKey(prefix, key)
		if err != nil {
			t.Fatalf("manifestObjectKey(%d bytes): %v", length, err)
		}
		digest := sha256.Sum256([]byte(key))
		want := prefix + "/blobs/v1/" + hex.EncodeToString(digest[:]) + "/" + base64.RawURLEncoding.EncodeToString([]byte(key))
		if got != want {
			t.Fatalf("%d-byte key: backend key = %q, want v0.1.1 encoding %q", length, got, want)
		}
		if _, differs := legacyManifestObjectKey(prefix, key); differs {
			t.Fatalf("%d-byte key reports a distinct legacy encoding; short keys have exactly one", length)
		}
	}
}

// TestManifestKeySegmentsFitMinIO holds D1: MinIO refuses an object-key path
// segment above 255 bytes, which v0.1.1 reached at a 192-byte logical key.
// Every storage-legal key length must stay under the bound and round-trip.
func TestManifestKeySegmentsFitMinIO(t *testing.T) {
	t.Parallel()
	// MinIO's limit, stated independently of the production constant.
	const minioSegmentLimit = 255
	prefix := "deployments/test"
	for length := 1; length <= 512; length++ {
		key := strings.Repeat("s", length)
		backend, err := manifestObjectKey(prefix, key)
		if err != nil {
			t.Fatalf("manifestObjectKey(%d bytes): %v", length, err)
		}
		for _, segment := range strings.Split(backend, "/") {
			if len(segment) > minioSegmentLimit {
				t.Fatalf("%d-byte key: segment of %d bytes exceeds %d", length, len(segment), minioSegmentLimit)
			}
		}
		decoded, ok := logicalKeyFromManifestObject(prefix, backend)
		if !ok || decoded != key {
			t.Fatalf("%d-byte key: round trip = (%q, %v)", length, decoded, ok)
		}
	}
}

// TestManifestKeyDecodingIsCanonical proves injectivity from the other side:
// only the one canonical split of an encoding decodes. A row that splits the
// same characters elsewhere, or adds an empty segment, is foreign data.
func TestManifestKeyDecodingIsCanonical(t *testing.T) {
	t.Parallel()
	prefix := "deployments/test"
	key := strings.Repeat("c", 300)
	digest := sha256.Sum256([]byte(key))
	encoded := base64.RawURLEncoding.EncodeToString([]byte(key))
	root := prefix + "/blobs/v1/" + hex.EncodeToString(digest[:]) + "/"
	for name, row := range map[string]string{
		"split early":       root + encoded[:100] + "/" + encoded[100:],
		"split late":        root + encoded[:255] + "/" + encoded[255:300] + "/" + encoded[300:],
		"trailing empty":    root + encoded[:255] + "/" + encoded[255:] + "/",
		"short key split":   root + "YQ/YQ",
		"empty middle":      root + encoded[:255] + "//" + encoded[255:],
		"short with legacy": root + encoded[:10],
	} {
		if logical, ok := logicalKeyFromManifestObject(prefix, row); ok {
			t.Errorf("%s: row decoded to %q, want refusal", name, logical)
		}
	}
}

// TestLegacyLongManifestKeyStaysReadable covers the one population v0.1.1
// wrote that the new encoding does not reproduce: a key above 191 bytes on a
// service with no segment limit (AWS S3). Its single-segment row must still
// list, and legacyManifestObjectKey must name it for Get/Put/Delete.
func TestLegacyLongManifestKeyStaysReadable(t *testing.T) {
	t.Parallel()
	prefix := "deployments/test"
	key := strings.Repeat("l", 235)
	digest := sha256.Sum256([]byte(key))
	legacy := prefix + "/blobs/v1/" + hex.EncodeToString(digest[:]) + "/" + base64.RawURLEncoding.EncodeToString([]byte(key))
	got, differs := legacyManifestObjectKey(prefix, key)
	if !differs || got != legacy {
		t.Fatalf("legacyManifestObjectKey = (%q, %v), want (%q, true)", got, differs, legacy)
	}
	current, err := manifestObjectKey(prefix, key)
	if err != nil {
		t.Fatal(err)
	}
	if current == legacy {
		t.Fatal("a 235-byte key still encodes into one over-long segment")
	}
	decoded, ok := logicalKeyFromManifestObject(prefix, legacy)
	if !ok || decoded != key {
		t.Fatalf("legacy row decodes to (%q, %v), want (%q, true)", decoded, ok, key)
	}
}
