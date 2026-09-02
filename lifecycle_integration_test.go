//go:build integration

package s3store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/looprig/s3store/internal/testserver"
	"github.com/looprig/storage"
	"github.com/looprig/storage/storetest"
)

// TestBlobReaderLifecycleConformance runs Storage's shared optional-capability
// suite. That suite says outright that it "cannot create such a wait", so it
// exercises the ordering and idempotence rules but never a genuinely blocked
// provider Read. TestCloseUnblocksAStalledPayloadRead below is the
// backend-specific probe it requires alongside it.
func TestBlobReaderLifecycleConformance(t *testing.T) {
	storetest.TestBlobReaderLifecycle(t, func(t *testing.T) storage.BlobReaderLifecycle {
		server := testserver.New()
		t.Cleanup(server.Close)
		return newIntegrationStore(t, server, nil)
	})
}

// TestCloseUnblocksAStalledPayloadRead is the measurement behind
// BlobReaderCloseBound, and the one assertion in this module that a constant
// cannot satisfy. The fixture writes a short prefix of the payload and then
// stops writing without closing the connection, so the second Read is blocked
// on a socket with no bytes on it. Close must still return within the bound,
// and the blocked Read must land within the bound with a non-EOF error.
//
// What it does not cover: it measures this fixture over a loopback connection,
// not a remote endpoint or a wedged middlebox. It is evidence that Close does
// not WAIT for a stalled Read, not a latency guarantee for the constant.
func TestCloseUnblocksAStalledPayloadRead(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const key = "blobs/stalled"
	const prefix = 8
	body := bytes.Repeat([]byte("s"), 1<<16)
	if err := store.Put(ctx, key, bytes.NewReader(body)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	stalled := server.StallNextPayloadRead(prefix)
	reader, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	select {
	case <-stalled:
	case <-ctx.Done():
		t.Fatal("the fixture never reached its stall")
	}
	head := make([]byte, prefix)
	if _, err := io.ReadFull(reader, head); err != nil {
		t.Fatalf("reading the delivered prefix: %v", err)
	}

	type readOutcome struct {
		n   int
		err error
	}
	blocked := make(chan readOutcome, 1)
	go func() {
		var buffer [4096]byte
		n, readErr := reader.Read(buffer[:])
		blocked <- readOutcome{n: n, err: readErr}
	}()
	// Establish that the Read really is blocked. Without this the test would
	// pass against a fixture that simply ended the body, which is not the
	// condition being measured.
	select {
	case outcome := <-blocked:
		t.Fatalf("Read returned %d, %v before Close; the fixture did not stall", outcome.n, outcome.err)
	case <-time.After(250 * time.Millisecond):
	}

	// Close is received under a bound rather than called inline, so a Close that
	// waits for the stalled Read fails here as an assertion instead of hanging
	// until the go test timeout.
	closeStarted := time.Now()
	closeResult := make(chan error, 1)
	go func() { closeResult <- reader.Close() }()
	var closeErr error
	select {
	case closeErr = <-closeResult:
		if elapsed := time.Since(closeStarted); elapsed > blobReaderCloseBound {
			t.Errorf("Close took %v with a Read blocked, want at most the advertised %v", elapsed, blobReaderCloseBound)
		}
	case <-time.After(blobReaderCloseBound):
		t.Fatalf("Close did not return within the advertised %v while a Read was blocked; Close must not wait for a Read", blobReaderCloseBound)
	}
	if closeErr != nil && !errors.Is(closeErr, context.Canceled) {
		t.Logf("Close on a stalled body returned %v", closeErr)
	}

	select {
	case outcome := <-blocked:
		if outcome.n != 0 {
			t.Errorf("the blocked Read returned %d bytes after Close, want 0", outcome.n)
		}
		if outcome.err == nil || errors.Is(outcome.err, io.EOF) {
			t.Errorf("the blocked Read terminal error = %v, want a non-EOF error", outcome.err)
		}
	case <-time.After(blobReaderCloseBound):
		t.Fatalf("the blocked Read did not return within the advertised %v after Close; Close does not unblock provider I/O and this module may not claim the capability", blobReaderCloseBound)
	}

	// Repeated Close is latched, and every later Read is terminal and non-EOF.
	if second := reader.Close(); !errorsEquivalent(second, closeErr) {
		t.Errorf("second Close = %v, first = %v; the classification must be stable", second, closeErr)
	}
	for attempt := range 3 {
		var probe [8]byte
		n, err := reader.Read(probe[:])
		if n != 0 || err == nil || errors.Is(err, io.EOF) || !errors.Is(err, fs.ErrClosed) {
			t.Errorf("Read after Close %d = %d, %v; want 0 and a non-EOF fs.ErrClosed error", attempt, n, err)
		}
	}
}

// TestReadAfterCloseIsTerminalOnAFullyBufferedStream is the row the stalled
// probe cannot supply: a reader whose verifier has ALREADY latched io.EOF
// through a clean end of stream. Without it, "no Read after Close returns EOF"
// would be tested only where no EOF was ever reached, which is the case the
// rule is not about.
func TestReadAfterCloseIsTerminalOnAFullyBufferedStream(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const key = "blobs/drained"
	if err := store.Put(ctx, key, strings.NewReader("drained bytes")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	reader, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	// The verifier has now latched io.EOF. A closed check placed after that
	// latch would return io.EOF here, which sessionstore records as verified
	// content.
	first := reader.Close()
	var probe [8]byte
	n, err := reader.Read(probe[:])
	if n != 0 || err == nil || errors.Is(err, io.EOF) || !errors.Is(err, fs.ErrClosed) {
		t.Errorf("Read after Close on a drained stream = %d, %v; want 0 and a non-EOF fs.ErrClosed error", n, err)
	}
	if second := reader.Close(); !errorsEquivalent(second, first) {
		t.Errorf("second Close = %v, first = %v; the classification must be stable", second, first)
	}
}

func errorsEquivalent(left, right error) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	return left == nil || errors.Is(left, right) || errors.Is(right, left)
}
