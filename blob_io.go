package s3store

import (
	"crypto/sha256"
	"hash"
	"io"
)

// ObjectTooLargeError reports the configured byte ceiling without retaining a
// logical key or any source data.
type ObjectTooLargeError struct {
	Maximum int64
}

func (e *ObjectTooLargeError) Error() string {
	return "s3store: blob exceeds the configured accounted object-size maximum"
}

type accountedHashReader struct {
	source  io.Reader
	maximum int64
	size    int64
	hash    hash.Hash
	digest  [sha256.Size]byte
	final   bool
}

func newAccountedHashReader(source io.Reader, maximum int64) *accountedHashReader {
	return &accountedHashReader{source: source, maximum: maximum, hash: sha256.New()}
}

func (r *accountedHashReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if r.size == r.maximum {
		var probe [1]byte
		n, err := r.source.Read(probe[:])
		if n > 0 {
			return 0, &ObjectTooLargeError{Maximum: r.maximum}
		}
		return 0, err
	}
	remaining := r.maximum - r.size
	if int64(len(buffer)) > remaining {
		buffer = buffer[:remaining]
	}
	n, err := r.source.Read(buffer)
	if n > 0 {
		_, _ = r.hash.Write(buffer[:n])
		r.size += int64(n)
		r.final = false
	}
	return n, err
}

func (r *accountedHashReader) Size() int64 {
	return r.size
}

func (r *accountedHashReader) Digest() [sha256.Size]byte {
	if !r.final {
		sum := r.hash.Sum(nil)
		copy(r.digest[:], sum)
		r.final = true
	}
	return r.digest
}

type verifyingBlobReader struct {
	body           io.ReadCloser
	expectedSize   int64
	expectedDigest [sha256.Size]byte
	read           int64
	hash           hash.Hash
	terminal       error
}

func newVerifyingBlobReader(body io.ReadCloser, size int64, digest [sha256.Size]byte) *verifyingBlobReader {
	return &verifyingBlobReader{
		body: body, expectedSize: size, expectedDigest: digest, hash: sha256.New(),
	}
}

func (r *verifyingBlobReader) Read(buffer []byte) (int, error) {
	if r.terminal != nil {
		return 0, r.terminal
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	if r.read == r.expectedSize {
		var probe [1]byte
		n, err := r.body.Read(probe[:])
		if n > 0 {
			r.terminal = integrityError("payload read")
			return 0, r.terminal
		}
		if err != nil && err != io.EOF {
			return 0, err
		}
		return 0, r.finish()
	}
	remaining := r.expectedSize - r.read
	if int64(len(buffer)) > remaining {
		buffer = buffer[:remaining]
	}
	n, err := r.body.Read(buffer)
	if n > 0 {
		_, _ = r.hash.Write(buffer[:n])
		r.read += int64(n)
	}
	if err == io.EOF {
		if r.read != r.expectedSize {
			r.terminal = integrityError("payload read")
			return n, r.terminal
		}
		return n, r.finish()
	}
	return n, err
}

func (r *verifyingBlobReader) finish() error {
	var actual [sha256.Size]byte
	copy(actual[:], r.hash.Sum(nil))
	if !equalDigest(actual[:], r.expectedDigest[:]) {
		r.terminal = integrityError("payload digest")
		return r.terminal
	}
	r.terminal = io.EOF
	return io.EOF
}

func (r *verifyingBlobReader) Close() error {
	return r.body.Close()
}
