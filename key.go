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

func validateBlobKey(key string) error {
	return storage.ValidateName(key)
}

func validateListPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	return storage.ValidateName(strings.TrimSuffix(prefix, "/"))
}

// manifestObjectKey maps a canonical logical key into one versioned S3
// namespace. The digest prevents an encoded key from being substituted during
// listing, while RawURLEncoding makes the logical key reversible without
// introducing '/', percent escapes, or path-cleaning aliases.
func manifestObjectKey(deploymentPrefix, logicalKey string) (string, error) {
	if err := storage.ValidateName(deploymentPrefix); err != nil {
		return "", err
	}
	if err := storage.ValidateName(logicalKey); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(logicalKey))
	objectKey := deploymentPrefix + "/" + manifestNamespace +
		hex.EncodeToString(digest[:]) + "/" +
		base64.RawURLEncoding.EncodeToString([]byte(logicalKey))
	if len(objectKey) > maxS3ObjectKeyBytes {
		return "", invalidOption("DeploymentPrefix", "and logical key exceed the S3 object-key limit")
	}
	return objectKey, nil
}

// logicalKeyFromManifestObject validates every component rather than merely
// stripping a prefix. A malformed or injected row is foreign data and is
// ignored independently so it cannot disable the rest of a listing page.
func logicalKeyFromManifestObject(deploymentPrefix, objectKey string) (string, bool) {
	root := deploymentPrefix + "/" + manifestNamespace
	if !strings.HasPrefix(objectKey, root) {
		return "", false
	}
	remainder := strings.TrimPrefix(objectKey, root)
	parts := strings.Split(remainder, "/")
	if len(parts) != 2 || len(parts[0]) != sha256.Size*2 || parts[1] == "" {
		return "", false
	}
	digest, err := hex.DecodeString(parts[0])
	if err != nil || len(digest) != sha256.Size {
		return "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != parts[1] {
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
