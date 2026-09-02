package s3store

import (
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"sync/atomic"
	"testing"
	"time"
)

// The integration probe measures that a stalled network Read is released, but
// it cannot separate the parts of blobReader that produce that result: over a
// real HTTP body, aborting the request and closing the body each unblock a
// stalled Read on their own, net/http's Close is already idempotent, and the
// two closed checks are interchangeable for any Read that starts after Close
// returns. Every one of those mutations was measured and SURVIVED the whole
// integration suite.
//
// These tests supply the fixtures that violate the properties, which is the
// only way those detectors exist at all: a body whose Close is not idempotent,
// a body that yields bytes after Close, and a body whose Read never returns.

type fakeBody struct {
	readFn  func(buffer []byte) (int, error)
	closeFn func(call int) error
	closes  atomic.Int32
}

func (b *fakeBody) Read(buffer []byte) (int, error) {
	return b.readFn(buffer)
}

func (b *fakeBody) Close() error {
	call := int(b.closes.Add(1))
	if b.closeFn == nil {
		return nil
	}
	return b.closeFn(call)
}

func newTestBlobReader(body io.ReadCloser, size int64, digest [sha256.Size]byte) (*blobReader, *atomic.Int32, *atomic.Int32) {
	var aborts, releases atomic.Int32
	reader := newBlobReader(
		newVerifyingBlobReader(body, size, digest),
		func() { aborts.Add(1) },
		func() { releases.Add(1) },
	)
	return reader, &aborts, &releases
}

func neverEndingBody() *fakeBody {
	return &fakeBody{readFn: func(buffer []byte) (int, error) {
		select {}
	}}
}

// TestCloseInvokesBothReleaseMechanismsExactlyOnce is the detector the
// integration probe cannot be: over a real body the abort and the body Close
// each mask the other's removal, so the only way to hold both is to observe
// them directly.
func TestCloseInvokesBothReleaseMechanismsExactlyOnce(t *testing.T) {
	t.Parallel()
	body := neverEndingBody()
	reader, aborts, releases := newTestBlobReader(body, 16, sha256.Sum256(nil))
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := aborts.Load(); got != 1 {
		t.Errorf("request aborts = %d, want exactly 1; without it a Read blocked in a wrapped body has no interruption", got)
	}
	if got := body.closes.Load(); got != 1 {
		t.Errorf("body closes = %d, want exactly 1; cancelling the request alone leaks the response body", got)
	}
	if got := releases.Load(); got != 1 {
		t.Errorf("transfer-slot releases = %d, want exactly 1", got)
	}
	for range 3 {
		_ = reader.Close()
	}
	if got := body.closes.Load(); got != 1 {
		t.Errorf("body closes after repeated Close = %d, want 1; Close must not close the body again", got)
	}
	if got := releases.Load(); got != 1 {
		t.Errorf("transfer-slot releases after repeated Close = %d, want 1", got)
	}
}

// TestCloseClassificationIsLatchedAcrossAnUnstableBody uses a body whose Close
// fails once and then succeeds -- exactly what net/http does NOT do, which is
// why a real body cannot detect an unlatched Close.
func TestCloseClassificationIsLatchedAcrossAnUnstableBody(t *testing.T) {
	t.Parallel()
	failure := errors.New("body close failed")
	body := neverEndingBody()
	body.closeFn = func(call int) error {
		if call == 1 {
			return failure
		}
		return nil
	}
	reader, _, _ := newTestBlobReader(body, 16, sha256.Sum256(nil))
	first := reader.Close()
	if !errors.Is(first, failure) {
		t.Fatalf("first Close = %v, want the body's failure", first)
	}
	for attempt := range 3 {
		if repeat := reader.Close(); !errors.Is(repeat, failure) {
			t.Errorf("Close call %d = %v, want the latched %v; repeated Close must not change classification", attempt+2, repeat, failure)
		}
	}
}

// TestReadInFlightWhenCloseBeginsIsTerminal supplies the body the conformance
// suite cannot: one that returns real bytes AFTER Close has returned. Without
// the closed check that follows the verifier, those bytes reach the caller.
func TestReadInFlightWhenCloseBeginsIsTerminal(t *testing.T) {
	t.Parallel()
	released := make(chan struct{})
	entered := make(chan struct{})
	var once atomic.Bool
	body := &fakeBody{readFn: func(buffer []byte) (int, error) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-released
		}
		copy(buffer, "payload")
		return len("payload"), nil
	}}
	reader, _, _ := newTestBlobReader(body, int64(len("payload")), sha256.Sum256([]byte("payload")))

	type outcome struct {
		n   int
		err error
	}
	result := make(chan outcome, 1)
	go func() {
		var buffer [16]byte
		n, err := reader.Read(buffer[:])
		result <- outcome{n: n, err: err}
	}()
	<-entered
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(released)
	select {
	case got := <-result:
		if got.n != 0 {
			t.Errorf("in-flight Read returned %d bytes after Close, want 0", got.n)
		}
		if !errors.Is(got.err, fs.ErrClosed) || errors.Is(got.err, io.EOF) {
			t.Errorf("in-flight Read error = %v, want a non-EOF fs.ErrClosed error", got.err)
		}
	case <-time.After(blobReaderCloseBound):
		t.Fatalf("in-flight Read did not return within %v of Close", blobReaderCloseBound)
	}
}

// TestReadAfterCloseDoesNotConsultAnUnresponsiveBody supplies the body that
// separates the closed check placed BEFORE the verifier from the one after it.
// Over a real body the two are interchangeable, because both see the same flag
// once Close has returned. They stop being interchangeable when the body's Read
// does not return: only the earlier check keeps the post-Close Read inside the
// advertised bound.
func TestReadAfterCloseDoesNotConsultAnUnresponsiveBody(t *testing.T) {
	t.Parallel()
	reader, _, _ := newTestBlobReader(neverEndingBody(), 16, sha256.Sum256(nil))
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	type outcome struct {
		n   int
		err error
	}
	result := make(chan outcome, 1)
	go func() {
		var buffer [8]byte
		n, err := reader.Read(buffer[:])
		result <- outcome{n: n, err: err}
	}()
	select {
	case got := <-result:
		if got.n != 0 || !errors.Is(got.err, fs.ErrClosed) || errors.Is(got.err, io.EOF) {
			t.Errorf("post-Close Read = %d, %v; want 0 and a non-EOF fs.ErrClosed error", got.n, got.err)
		}
	case <-time.After(blobReaderCloseBound):
		t.Fatalf("post-Close Read did not return within the advertised %v; it reached a body that never answers", blobReaderCloseBound)
	}
}

// TestReadAfterCloseNeverReportsEOFOnADrainedStream is the row for a stream
// that DID reach a clean, verified end. sessionstore compares bare io.EOF by
// identity to mean "verified through terminal EOF", so a closed reader that
// still answers io.EOF would have a torn-down stream recorded as verified.
func TestReadAfterCloseNeverReportsEOFOnADrainedStream(t *testing.T) {
	t.Parallel()
	const content = "drained"
	var offset int
	body := &fakeBody{readFn: func(buffer []byte) (int, error) {
		if offset >= len(content) {
			return 0, io.EOF
		}
		n := copy(buffer, content[offset:])
		offset += n
		return n, nil
	}}
	reader, _, releases := newTestBlobReader(body, int64(len(content)), sha256.Sum256([]byte(content)))
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != content {
		t.Fatalf("ReadAll = %q, %v; want %q and a clean EOF", got, err, content)
	}
	if released := releases.Load(); released != 1 {
		t.Fatalf("transfer-slot releases after terminal read = %d, want 1", released)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var probe [4]byte
	n, readErr := reader.Read(probe[:])
	if n != 0 || !errors.Is(readErr, fs.ErrClosed) || errors.Is(readErr, io.EOF) {
		t.Errorf("Read after Close on a verified stream = %d, %v; want 0 and a non-EOF fs.ErrClosed error", n, readErr)
	}
	if released := releases.Load(); released != 1 {
		t.Errorf("transfer-slot releases = %d, want exactly 1 across terminal read and Close", released)
	}
}

// TestBlobReaderClosedErrorIsRecordable keeps the new exported error inside the
// module's recording rule.
func TestBlobReaderClosedErrorIsRecordable(t *testing.T) {
	t.Parallel()
	var closed *BlobReaderClosedError
	if !errors.As(errBlobReaderClosed, &closed) {
		t.Fatalf("errBlobReaderClosed is %T, want *BlobReaderClosedError", errBlobReaderClosed)
	}
	if !errors.Is(errBlobReaderClosed, fs.ErrClosed) {
		t.Error("BlobReaderClosedError does not match fs.ErrClosed")
	}
	if errors.Is(errBlobReaderClosed, io.EOF) {
		t.Error("BlobReaderClosedError matches io.EOF; a closed reader must never read as a verified end of stream")
	}
	if got := RedactedErrorText(errBlobReaderClosed); got != errBlobReaderClosed.Error() {
		t.Errorf("RedactedErrorText(*BlobReaderClosedError) = %q, want the typed classification %q", got, errBlobReaderClosed.Error())
	}
}
