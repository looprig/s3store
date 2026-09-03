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
// context the payload GetObject was issued with AND closes the response body.
// Each of those was measured to release a Read blocked on a socket on its own,
// so neither is load-bearing against the other; both are done because the
// cancellation reaches the transport through any SDK body wrapper while the
// body Close is the documented way to release a response. Either way a blocked
// socket read is interrupted, which is the capability fsstore cannot offer --
// a blocked filesystem read has no equivalent interruption -- and it is the
// reason this module may claim the capability while fsstore may not.
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
// begun. It is deliberately not io.EOF: sessionstore compares bare io.EOF by
// identity to mean "verified through terminal EOF", so a closed reader that
// still answered io.EOF could be taken for a stream that ended cleanly.
//
// Scope of that claim: sessionstore only reaches the identity comparison for a
// caller DRAINING the stream with no termination already in flight -- a Read
// racing a Close has its error joined rather than identity-compared, and
// exactVerifier is a second guard behind it. So this is defence in depth on a
// reachable path, not the sole thing standing between a torn-down stream and a
// verified record. It matches fs.ErrClosed under errors.Is, the classification
// memstore uses.
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
//
// The consequence, which the contract permits and this type does not hide:
// Read is NOT safe against another Read. storage.BlobReaderLifecycle requires
// only that Read and Close be safe together, and the verifier behind this type
// keeps its offset, hash, and terminal error unsynchronized. memstore, whose
// single mutex serializes everything, IS safe against concurrent Reads, so a
// consumer that reads one blob from several goroutines will work there and
// race here. One goroutine per reader.
//
// That warning is repeated on Store.Get and in the README, because this type is
// unexported: the reader who needs it is an integrator swapping a Blobs backend
// under sessionstore, and none of them can see a comment here.
type blobReader struct {
	verifier *verifyingBlobReader
	// abort cancels the context the payload request was issued with. It is one
	// of the two unblocking mechanisms, not the only one: closing the body
	// releases a stalled Read too, measured in both directions. Both are used.
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
