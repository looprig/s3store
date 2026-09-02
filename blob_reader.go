package s3store

import (
	"context"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"time"
)

// blobReaderCloseBound is the advertised upper bound for Close itself and for
// any provider-controlled Read to return after Close begins.
//
// The mechanism is not a timer, and nothing here sleeps: Close cancels the
// context the payload GetObject was issued with and then closes the response
// body. Cancelling the request context makes the HTTP transport tear the
// connection down, which is what forces a Read blocked on a socket to return.
// That is the capability fsstore cannot offer -- a blocked filesystem read has
// no equivalent interruption -- and it is the reason this module may claim the
// capability while fsstore may not.
//
// The number is a conservative ceiling on that teardown, not a measurement of
// it. It is generous on purpose: sessionstore consults it once, at Open, for
// positivity, and derives no timer from it, so an over-estimate costs nothing
// while an under-estimate would make a truthful provider look unconforming.
// TestCloseUnblocksAStalledPayloadRead is what actually measures the mechanism,
// against a fixture that stops writing mid-body.
const blobReaderCloseBound = 5 * time.Second

// BlobReaderCloseBound implements storage.BlobReaderLifecycle.
func (s *Store) BlobReaderCloseBound() time.Duration {
	return blobReaderCloseBound
}

// BlobReaderClosedError is the terminal error every Read returns once Close has
// begun. It is deliberately not io.EOF: a consumer that treats EOF as "the
// stream was verified through its end" would otherwise record a torn-down
// stream as verified content. It matches fs.ErrClosed under errors.Is so the
// classification is the same one memstore uses.
type BlobReaderClosedError struct{}

func (e *BlobReaderClosedError) Error() string {
	return "s3store: blob reader is closed"
}

func (e *BlobReaderClosedError) Is(target error) bool {
	return target == fs.ErrClosed
}

var errBlobReaderClosed error = &BlobReaderClosedError{}

// blobReader is the reader Get returns. It owns the bounded shutdown contract:
// Close is latched and classification-stable, Close does not wait for a Read,
// and no Read after Close returns bytes or io.EOF.
//
// Read and Close share no lock. Serializing them on one mutex would make Close
// wait for a blocked network Read, which is the exact outcome the capability
// exists to forbid; the only shared state is an atomic flag and two sync.Onces.
type blobReader struct {
	verifier *verifyingBlobReader
	// abort cancels the context the payload request was issued with. It is the
	// unblocking mechanism; closing the body alone is not relied on.
	abort   context.CancelFunc
	release func()

	// closed is what lets a Read tell a teardown from a genuine end of stream.
	// It is stored before the teardown below rather than after, so that an
	// in-flight Read released BY the teardown is more likely to classify as
	// closed than as a raw transport error. That preference is defensive only:
	// the contract constrains Reads after Close RETURNS, the flag is set before
	// Close returns either way, and the ordering was probed and is not
	// separately observable without racing the teardown against the Read.
	closed      atomic.Bool
	closeOnce   sync.Once
	closeErr    error
	releaseOnce sync.Once
}

func newBlobReader(verifier *verifyingBlobReader, abort context.CancelFunc, release func()) *blobReader {
	return &blobReader{verifier: verifier, abort: abort, release: release}
}

// Read checks the closed latch BEFORE consulting the verifier, so a reader
// whose verifier has already latched io.EOF still reports closure rather than a
// clean end of stream, and again after, so a Read that was in flight when Close
// began cannot report either bytes or EOF.
func (r *blobReader) Read(buffer []byte) (int, error) {
	if r.closed.Load() {
		return 0, errBlobReaderClosed
	}
	n, err := r.verifier.Read(buffer)
	if r.closed.Load() {
		// Close began while this Read was in flight. Whatever the aborted body
		// produced -- bytes, nil, io.EOF, or a transport error -- is not a
		// terminal result for this stream, and the bytes are discarded.
		return 0, errBlobReaderClosed
	}
	if err != nil {
		r.releaseOnce.Do(r.release)
	}
	return n, err
}

// Close is idempotent with a stable classification: the first call computes the
// result under a sync.Once and every later call returns that same value rather
// than closing the body a second time.
func (r *blobReader) Close() error {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.abort()
		r.closeErr = r.verifier.Close()
		r.releaseOnce.Do(r.release)
	})
	return r.closeErr
}

// blobReader is the only reader Get returns.
var _ io.ReadCloser = (*blobReader)(nil)
