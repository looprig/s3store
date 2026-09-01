package s3store

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"unicode/utf8"

	"github.com/looprig/storage"
)

const (
	manifestMagic       = "LRBLOB01"
	manifestHeaderBytes = len(manifestMagic) + 2 + 2 + 8 + 32
	maxManifestBytes    = 1024
)

// BlobIntegrityError means an object exists but cannot be trusted as the blob
// named by its manifest. It is deliberately distinct from BlobNotFoundError:
// corrupt or truncated data must never license callers to recreate an absent
// logical key.
type BlobIntegrityError struct {
	Operation string
}

func (e *BlobIntegrityError) Error() string {
	return "s3store: blob integrity verification failed during " + e.Operation
}

type blobManifest struct {
	LogicalKey string
	PayloadKey string
	Size       int64
	Digest     [32]byte
}

func encodeManifest(manifest blobManifest) ([]byte, error) {
	if err := storage.ValidateName(manifest.LogicalKey); err != nil {
		return nil, err
	}
	if !validInternalObjectKey(manifest.PayloadKey) {
		return nil, errors.New("s3store: invalid internal payload key")
	}
	if manifest.Size < 0 {
		return nil, errors.New("s3store: invalid negative blob size")
	}
	logicalLength, ok := checkedUint16Length(len(manifest.LogicalKey))
	if !ok {
		return nil, errors.New("s3store: manifest logical key length overflows its field")
	}
	payloadLength, ok := checkedUint16Length(len(manifest.PayloadKey))
	if !ok {
		return nil, errors.New("s3store: manifest payload key length overflows its field")
	}
	encodedLength := manifestHeaderBytes + len(manifest.LogicalKey) + len(manifest.PayloadKey)
	if encodedLength > maxManifestBytes {
		return nil, errors.New("s3store: manifest exceeds its size bound")
	}
	encoded := make([]byte, encodedLength)
	copy(encoded, manifestMagic)
	binary.BigEndian.PutUint16(encoded[8:10], logicalLength)
	binary.BigEndian.PutUint16(encoded[10:12], payloadLength)
	binary.BigEndian.PutUint64(encoded[12:20], uint64(manifest.Size))
	copy(encoded[20:52], manifest.Digest[:])
	offset := manifestHeaderBytes
	copy(encoded[offset:], manifest.LogicalKey)
	offset += len(manifest.LogicalKey)
	copy(encoded[offset:], manifest.PayloadKey)
	return encoded, nil
}

func decodeManifest(reader io.Reader, contentLength int64) (blobManifest, error) {
	if contentLength < int64(manifestHeaderBytes) || contentLength > int64(maxManifestBytes) {
		return blobManifest{}, integrityError("manifest")
	}
	encoded := make([]byte, contentLength)
	if _, err := io.ReadFull(reader, encoded); err != nil {
		return blobManifest{}, integrityError("manifest")
	}
	var trailing [1]byte
	if n, _ := reader.Read(trailing[:]); n != 0 {
		return blobManifest{}, integrityError("manifest")
	}
	if string(encoded[:8]) != manifestMagic {
		return blobManifest{}, integrityError("manifest")
	}
	logicalLength := int(binary.BigEndian.Uint16(encoded[8:10]))
	payloadLength := int(binary.BigEndian.Uint16(encoded[10:12]))
	if manifestHeaderBytes+logicalLength+payloadLength != len(encoded) {
		return blobManifest{}, integrityError("manifest")
	}
	rawSize := binary.BigEndian.Uint64(encoded[12:20])
	if rawSize > math.MaxInt64 {
		return blobManifest{}, integrityError("manifest")
	}
	manifest := blobManifest{Size: int64(rawSize)}
	copy(manifest.Digest[:], encoded[20:52])
	offset := manifestHeaderBytes
	manifest.LogicalKey = string(encoded[offset : offset+logicalLength])
	offset += logicalLength
	manifest.PayloadKey = string(encoded[offset : offset+payloadLength])
	if storage.ValidateName(manifest.LogicalKey) != nil || !validInternalObjectKey(manifest.PayloadKey) {
		return blobManifest{}, integrityError("manifest")
	}
	return manifest, nil
}

func validInternalObjectKey(key string) bool {
	if key == "" || len(key) > maxS3ObjectKeyBytes || !utf8.ValidString(key) {
		return false
	}
	start := 0
	for index := 0; index <= len(key); index++ {
		if index < len(key) && key[index] != '/' {
			char := key[index]
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' && char != '.' && char != '-' {
				return false
			}
			continue
		}
		segment := key[start:index]
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
		start = index + 1
	}
	return true
}

func checkedUint16Length(length int) (uint16, bool) {
	if length < 0 || length > math.MaxUint16 {
		return 0, false
	}
	return uint16(length), true
}

func integrityError(operation string) *BlobIntegrityError {
	return &BlobIntegrityError{Operation: operation}
}
