package s3store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestManifestRoundTripIsExactAndBounded(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("payload"))
	want := blobManifest{
		LogicalKey: "blobs/key",
		PayloadKey: "deployments/test/payloads/v1/" + strings.Repeat("a", 64) + "/" + strings.Repeat("b", 32),
		Size:       7,
		Digest:     digest,
	}
	encoded, err := encodeManifest(want)
	if err != nil {
		t.Fatalf("encodeManifest: %v", err)
	}
	if len(encoded) > maxManifestBytes {
		t.Fatalf("encoded manifest length = %d, want at most %d", len(encoded), maxManifestBytes)
	}
	got, err := decodeManifest(bytes.NewReader(encoded), int64(len(encoded)))
	if err != nil {
		t.Fatalf("decodeManifest: %v", err)
	}
	if got != want {
		t.Fatalf("decoded manifest = %#v, want %#v", got, want)
	}
}

func TestManifestDecodeRejectsAmbiguousOrCorruptObjects(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("payload"))
	valid, err := encodeManifest(blobManifest{
		LogicalKey: "blobs/key",
		PayloadKey: "deployments/test/payloads/v1/" + strings.Repeat("a", 64) + "/" + strings.Repeat("b", 32),
		Size:       7,
		Digest:     digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	negativeSize := append([]byte(nil), valid...)
	negativeSize[12] = 0x80
	oversized := make([]byte, maxManifestBytes+1)
	copy(oversized, valid[:manifestHeaderBytes])
	logicalLength := int(binary.BigEndian.Uint16(valid[8:10]))
	binary.BigEndian.PutUint16(oversized[10:12], uint16(len(oversized)-manifestHeaderBytes-logicalLength))
	copy(oversized[manifestHeaderBytes:], valid[manifestHeaderBytes:manifestHeaderBytes+logicalLength])
	for index := manifestHeaderBytes + logicalLength; index < len(oversized); index++ {
		oversized[index] = 'p'
	}
	tests := []struct {
		name   string
		body   []byte
		length int64
	}{
		{name: "empty", body: nil, length: 0},
		{name: "truncated", body: valid[:len(valid)-1], length: int64(len(valid) - 1)},
		{name: "body beyond declared length", body: append(append([]byte(nil), valid...), 0), length: int64(len(valid))},
		{name: "declared short", body: valid, length: int64(len(valid) - 1)},
		{name: "declared long", body: valid, length: int64(len(valid) + 1)},
		{name: "above maximum", body: oversized, length: int64(len(oversized))},
		{name: "wrong magic", body: append([]byte("WRONG!!!"), valid[8:]...), length: int64(len(valid))},
		{name: "negative decoded size", body: negativeSize, length: int64(len(negativeSize))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeManifest(bytes.NewReader(tt.body), tt.length)
			var integrity *BlobIntegrityError
			if !errors.As(err, &integrity) {
				t.Fatalf("decodeManifest error = %T %v, want *BlobIntegrityError", err, err)
			}
		})
	}
}

func TestManifestEncodeRejectsInvalidFields(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256(nil)
	base := blobManifest{
		LogicalKey: "blobs/key",
		PayloadKey: "deployments/test/payloads/v1/" + strings.Repeat("a", 64) + "/" + strings.Repeat("b", 32),
		Digest:     digest,
	}
	tests := []struct {
		name   string
		mutate func(*blobManifest)
	}{
		{name: "invalid logical key", mutate: func(m *blobManifest) { m.LogicalKey = "../escape" }},
		{name: "empty payload key", mutate: func(m *blobManifest) { m.PayloadKey = "" }},
		{name: "oversized payload key", mutate: func(m *blobManifest) { m.PayloadKey = strings.Repeat("p", maxS3ObjectKeyBytes+1) }},
		{name: "negative size", mutate: func(m *blobManifest) { m.Size = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := base
			tt.mutate(&candidate)
			if _, err := encodeManifest(candidate); err == nil {
				t.Fatal("encodeManifest returned nil error")
			}
		})
	}
}
