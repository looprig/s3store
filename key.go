package s3store

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"

	"github.com/looprig/storage"
)

const (
	maxS3ObjectKeyBytes = 1024
	manifestNamespace   = "blobs/v1/"
	payloadNamespace    = "payloads/v1/"
)

// namespaceRoot is the single derivation of a deployment's backend root. Every
// key this module writes, reverses, or lists is built from it, so the
// deployment prefix cannot be dropped from one path while surviving in
// another. Two deployments sharing one bucket are isolated by exactly this
// component; TestTenantDeploymentsShareOneBucketWithoutCrossing is what proves
// dropping it is observable.
func namespaceRoot(deploymentPrefix, namespace string) string {
	return deploymentPrefix + "/" + namespace
}

func validateBlobKey(key string) error {
	return storage.ValidateName(key)
}

func validateListPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	return storage.ValidateName(strings.TrimSuffix(prefix, "/"))
}

// maxObjectKeySegmentBytes is the longest '/'-delimited component this module
// writes into a backend object key. AWS S3 has no per-segment limit, but MinIO
// refuses any segment above 255 bytes (XMinioInvalidObjectName), and v0.1.1
// reached that at a 192-byte logical key because it wrote the whole encoding
// as one segment.
const maxObjectKeySegmentBytes = 255

// manifestObjectKey maps a canonical logical key into one versioned S3
// namespace. The digest prevents an encoded key from being substituted during
// listing, while RawURLEncoding makes the logical key reversible without
// introducing '/', percent escapes, or path-cleaning aliases.
//
// The encoding is split into segments of exactly maxObjectKeySegmentBytes,
// the last one shorter or equal. The split is a pure function of the encoded
// length, so it stays injective and the decoder accepts only this one split.
// An encoding that already fits one segment (a logical key of at most 191
// bytes) is not split at all, which is byte-identical to v0.1.1.
func manifestObjectKey(deploymentPrefix, logicalKey string) (string, error) {
	if err := storage.ValidateName(deploymentPrefix); err != nil {
		return "", err
	}
	if err := storage.ValidateName(logicalKey); err != nil {
		return "", err
	}
	objectKey := manifestDigestRoot(deploymentPrefix, logicalKey) +
		segmentEncoding(base64.RawURLEncoding.EncodeToString([]byte(logicalKey)))
	if len(objectKey) > maxS3ObjectKeyBytes {
		return "", invalidOption("DeploymentPrefix", "and logical key exceed the S3 object-key limit")
	}
	return objectKey, nil
}

// legacyManifestObjectKey returns the single-segment key v0.1.1 wrote for a
// logical key, and whether it differs from the current encoding. It differs
// only above 191 bytes, where v0.1.1 could have stored an object on a service
// without a segment limit (AWS S3) but never on MinIO. Callers use it to keep
// those objects readable, deletable, and immutable; nothing writes it.
func legacyManifestObjectKey(deploymentPrefix, logicalKey string) (string, bool) {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(logicalKey))
	if len(encoded) <= maxObjectKeySegmentBytes {
		return "", false
	}
	return manifestDigestRoot(deploymentPrefix, logicalKey) + encoded, true
}

func manifestDigestRoot(deploymentPrefix, logicalKey string) string {
	digest := sha256.Sum256([]byte(logicalKey))
	return namespaceRoot(deploymentPrefix, manifestNamespace) + hex.EncodeToString(digest[:]) + "/"
}

func segmentEncoding(encoded string) string {
	var builder strings.Builder
	for len(encoded) > maxObjectKeySegmentBytes {
		builder.WriteString(encoded[:maxObjectKeySegmentBytes])
		builder.WriteByte('/')
		encoded = encoded[maxObjectKeySegmentBytes:]
	}
	builder.WriteString(encoded)
	return builder.String()
}

// logicalKeyFromManifestObject validates every component rather than merely
// stripping a prefix. A malformed or injected row is foreign data and is
// ignored independently so it cannot disable the rest of a listing page.
//
// Two row shapes decode: the current canonical segmentation, and v0.1.1's
// single segment of any length (legacy rows above 191 bytes). Both name the
// same logical key, and List deduplicates them.
func logicalKeyFromManifestObject(deploymentPrefix, objectKey string) (string, bool) {
	root := namespaceRoot(deploymentPrefix, manifestNamespace)
	if !strings.HasPrefix(objectKey, root) {
		return "", false
	}
	remainder := strings.TrimPrefix(objectKey, root)
	parts := strings.Split(remainder, "/")
	if len(parts) < 2 || len(parts[0]) != sha256.Size*2 {
		return "", false
	}
	// A split other than the canonical one, including any empty segment,
	// fails the re-segmentation comparison; a lone empty segment decodes to
	// the empty name, which ValidateName refuses.
	encodedParts := parts[1:]
	encoded := strings.Join(encodedParts, "")
	if len(encodedParts) > 1 && segmentEncoding(encoded) != strings.Join(encodedParts, "/") {
		return "", false
	}
	digest, err := hex.DecodeString(parts[0])
	if err != nil || len(digest) != sha256.Size {
		return "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return "", false
	}
	logicalKey := string(decoded)
	if storage.ValidateName(logicalKey) != nil {
		return "", false
	}
	want := sha256.Sum256(decoded)
	if !equalDigest(digest, want[:]) {
		return "", false
	}
	return logicalKey, true
}

func equalDigest(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for i := range left {
		difference |= left[i] ^ right[i]
	}
	return difference == 0
}
