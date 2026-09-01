package s3store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestAccountedHashReaderDrivesMaxObjectSizeOnBothSides(t *testing.T) {
	t.Parallel()
	const maximum = int64(8)
	tests := []struct {
		name    string
		body    string
		want    string
		wantErr bool
	}{
		{name: "one below", body: "1234567", want: "1234567"},
		{name: "at threshold", body: "12345678", want: "12345678"},
		{name: "one above", body: "123456789", want: "12345678", wantErr: true},
		{name: "multibyte at threshold", body: "éééé", want: "éééé"},
		{name: "multibyte one above", body: "ééééx", want: "éééé", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newAccountedHashReader(strings.NewReader(tt.body), maximum)
			got, err := io.ReadAll(reader)
			if string(got) != tt.want {
				t.Fatalf("read bytes = %q, want %q", got, tt.want)
			}
			var tooLarge *ObjectTooLargeError
			if errors.As(err, &tooLarge) != tt.wantErr {
				t.Fatalf("read error = %T %v, ObjectTooLarge=%v want %v", err, err, errors.As(err, &tooLarge), tt.wantErr)
			}
			if reader.Size() != int64(len(tt.want)) {
				t.Errorf("accounted size = %d, want %d", reader.Size(), len(tt.want))
			}
			wantDigest := sha256.Sum256([]byte(tt.want))
			if reader.Digest() != wantDigest {
				t.Errorf("digest = %x, want %x", reader.Digest(), wantDigest)
			}
		})
	}
}

func TestVerifyingBlobReaderRequiresExactLengthAndDigest(t *testing.T) {
	t.Parallel()
	want := []byte("committed bytes")
	digest := sha256.Sum256(want)
	tests := []struct {
		name    string
		body    []byte
		size    int64
		digest  [sha256.Size]byte
		want    []byte
		wantErr bool
	}{
		{name: "exact", body: want, size: int64(len(want)), digest: digest, want: want},
		{name: "truncated", body: want[:len(want)-1], size: int64(len(want)), digest: digest, want: want[:len(want)-1], wantErr: true},
		{name: "truncated despite matching prefix digest", body: want[:len(want)-1], size: int64(len(want)), digest: sha256.Sum256(want[:len(want)-1]), want: want[:len(want)-1], wantErr: true},
		{name: "extra byte is bounded", body: append(append([]byte(nil), want...), '!'), size: int64(len(want)), digest: digest, want: want, wantErr: true},
		{name: "wrong digest", body: want, size: int64(len(want)), digest: sha256.Sum256([]byte("different")), want: want, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newVerifyingBlobReader(io.NopCloser(bytes.NewReader(tt.body)), tt.size, tt.digest)
			got, err := io.ReadAll(reader)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("ReadAll bytes = %q, want %q", got, tt.want)
			}
			var integrity *BlobIntegrityError
			if errors.As(err, &integrity) != tt.wantErr {
				t.Fatalf("ReadAll error = %T %v, integrity=%v want %v", err, err, errors.As(err, &integrity), tt.wantErr)
			}
			if err := reader.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func TestVerifyingBlobReaderCapturesImmutableMetadata(t *testing.T) {
	t.Parallel()
	body := []byte("immutable")
	digest := sha256.Sum256(body)
	reader := newVerifyingBlobReader(io.NopCloser(bytes.NewReader(body)), int64(len(body)), digest)
	digest[0] ^= 0xff
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll after caller digest mutation: %v", err)
	}
}
