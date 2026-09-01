//go:build integration

package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/s3store/internal/testserver"
	"github.com/looprig/storage"
	"github.com/looprig/storage/storetest"
)

func TestBlobsIntegrationConformance(t *testing.T) {
	storetest.TestBlobs(t, func(t *testing.T) storage.Blobs {
		server := testserver.New()
		t.Cleanup(server.Close)
		return newIntegrationStore(t, server, nil)
	})
}

func TestPutVerifiesCommittedLengthAndDigestBeforePublishing(t *testing.T) {
	for _, tt := range []struct {
		mode string
		body []byte
	}{{mode: "truncate", body: []byte("committed bytes")}, {mode: "flip", body: []byte("committed bytes")}, {mode: "extend", body: nil}} {
		t.Run(tt.mode, func(t *testing.T) {
			server := testserver.New()
			t.Cleanup(server.Close)
			server.CorruptNextPayload(tt.mode)
			store := newIntegrationStore(t, server, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := store.Put(ctx, "blobs/corrupt", bytes.NewReader(tt.body))
			var integrity *BlobIntegrityError
			if !errors.As(err, &integrity) {
				t.Fatalf("Put with %s committed payload error = %T %v, want *BlobIntegrityError", tt.mode, err, err)
			}
			_, err = store.Get(ctx, "blobs/corrupt")
			var notFound *storage.BlobNotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("Get after rejected corrupt Put = %T %v, want *BlobNotFoundError", err, err)
			}
		})
	}
}

func TestPutRejectsResolvedMaxAccountedObjectSizeOnBothSides(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	store.options.maxAccountedObjectSize = 8
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Put(ctx, "blobs/at-limit", strings.NewReader("12345678")); err != nil {
		t.Fatalf("Put at maxAccountedObjectSize: %v", err)
	}
	err := store.Put(ctx, "blobs/above-limit", strings.NewReader("123456789"))
	var tooLarge *ObjectTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("Put above maxAccountedObjectSize error = %T %v, want *ObjectTooLargeError", err, err)
	}
	if server.Count("PutManifest") != 1 {
		t.Fatalf("manifest PUT count = %d, want only the at-limit object published", server.Count("PutManifest"))
	}
}

func TestConcurrentPutIsAtomicAndImmutable(t *testing.T) {
	t.Run("identical", func(t *testing.T) {
		server := testserver.New()
		t.Cleanup(server.Close)
		store := newIntegrationStore(t, server, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		errs := concurrentPuts(ctx, store, "blobs/concurrent-identical", []string{"same", "same"})
		for index, err := range errs {
			if err != nil {
				t.Errorf("Put %d: %v", index, err)
			}
		}
	})
	t.Run("different", func(t *testing.T) {
		server := testserver.New()
		t.Cleanup(server.Close)
		store := newIntegrationStore(t, server, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		errs := concurrentPuts(ctx, store, "blobs/concurrent-different", []string{"alpha", "bravo"})
		successes, conflicts := 0, 0
		for _, err := range errs {
			var conflict *storage.BlobConflictError
			switch {
			case err == nil:
				successes++
			case errors.As(err, &conflict):
				conflicts++
			default:
				t.Fatalf("concurrent Put error = %T %v", err, err)
			}
		}
		if successes != 1 || conflicts != 1 {
			t.Fatalf("concurrent outcomes = %d success, %d conflict; want 1 and 1", successes, conflicts)
		}
		got := readIntegrationBlob(t, ctx, store, "blobs/concurrent-different")
		if string(got) != "alpha" && string(got) != "bravo" {
			t.Fatalf("committed bytes = %q, want one complete contender", got)
		}
	})
}

func TestListPagesPastMalformedRow(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	server.SetPageLimit(2)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	want := []string{"blobs/a", "blobs/b", "blobs/c", "blobs/d", "blobs/e"}
	for _, key := range want {
		if err := store.Put(ctx, key, strings.NewReader(key)); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}
	server.PutRaw("looprig-test", "deployments/test/blobs/v1/not-a-digest/bad", []byte("foreign"))
	got, err := store.List(ctx, "blobs/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("List = %v, want %v", got, want)
	}
	if pages := server.Count("ListObjectsV2"); pages < 3 {
		t.Fatalf("list page requests = %d, want at least 3", pages)
	}
}

func TestPutCancellationStopsBlockedRequest(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	started := server.BlockNext("PutPayload")
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- store.Put(ctx, "blobs/cancel", strings.NewReader("bytes")) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("payload request never reached blocked fixture")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Put cancellation error = %T %v, want context.DeadlineExceeded", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Put ignored context cancellation")
	}
}

func TestPutPreservesPayloadWhenManifestCommitAcknowledgementIsLost(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	committed := server.CommitThenBlockNext("PutManifest")
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := store.Put(ctx, "blobs/lost-ack", strings.NewReader("committed bytes"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Put after lost manifest acknowledgement = %T %v, want context.DeadlineExceeded", err, err)
	}
	select {
	case <-committed:
	default:
		t.Fatal("fixture did not commit the manifest before cancellation")
	}

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer retryCancel()
	if err := store.Put(retryCtx, "blobs/lost-ack", strings.NewReader("committed bytes")); err != nil {
		t.Fatalf("idempotent retry after lost acknowledgement: %v", err)
	}
	if got := readIntegrationBlob(t, retryCtx, store, "blobs/lost-ack"); string(got) != "committed bytes" {
		t.Fatalf("bytes after lost acknowledgement = %q", got)
	}
}

func TestPutPreservesPayloadWhenManifestPublicationIsAmbiguous(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	server.CommitThenFailNext("PutManifest", http.StatusBadRequest, "AmbiguousFailure")
	server.FailNext("HeadManifest", 3, http.StatusInternalServerError, "InternalError")
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := store.Put(ctx, "blobs/ambiguous-publish", strings.NewReader("committed bytes"))
	var backend *BackendError
	if !errors.As(err, &backend) {
		t.Fatalf("Put after ambiguous manifest publication = %T %v, want *BackendError", err, err)
	}
	if err := store.Put(ctx, "blobs/ambiguous-publish", strings.NewReader("committed bytes")); err != nil {
		t.Fatalf("idempotent retry after ambiguous publication: %v", err)
	}
	if got := readIntegrationBlob(t, ctx, store, "blobs/ambiguous-publish"); string(got) != "committed bytes" {
		t.Fatalf("bytes after ambiguous publication = %q", got)
	}
}

func TestPutNeverDeletesPayloadItDidNotCreate(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	server.FailNext("PutPayload", 1, http.StatusPreconditionFailed, "PreconditionFailed")
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Put(ctx, "blobs/payload-collision", strings.NewReader("bytes")); err == nil {
		t.Fatal("Put after payload conditional collision returned nil")
	}
	if deletes := server.Count("DeletePayload"); deletes != 0 {
		t.Fatalf("DeletePayload requests = %d, want 0 for a payload this writer never created", deletes)
	}
}

func TestPutCleanupDoesNotOutliveCallerDeadline(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	putStarted := server.BlockNext("PutPayload")
	deleteStarted := server.BlockNext("DeletePayload")
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- store.Put(ctx, "blobs/cleanup-deadline", strings.NewReader("bytes")) }()
	select {
	case <-putStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("payload request never reached blocked fixture")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Put cancellation error = %T %v, want context.DeadlineExceeded", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("Put cleanup outlived the caller deadline")
	}
	select {
	case <-deleteStarted:
		t.Fatal("Put attempted to delete a payload it did not confirm creating")
	default:
	}
}

func TestPutVerificationCancellationDoesNotStartDetachedCleanup(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	verificationStarted := server.BlockNext("HeadPayload")
	deleteStarted := server.BlockNext("DeletePayload")
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- store.Put(ctx, "blobs/verification-cancel", strings.NewReader("bytes")) }()
	select {
	case <-verificationStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("payload verification never reached blocked fixture")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Put verification cancellation error = %T %v, want context.DeadlineExceeded", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("detached cleanup outlived the caller deadline")
	}
	select {
	case <-deleteStarted:
		t.Fatal("Put started detached payload cleanup after the caller deadline")
	default:
	}
}

func TestReadOperationsSurfaceCancellation(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		call      func(context.Context, *Store) error
	}{
		{name: "Get", operation: "HeadManifest", call: func(ctx context.Context, store *Store) error {
			_, err := store.Get(ctx, "blobs/cancel")
			return err
		}},
		{name: "Delete", operation: "HeadManifest", call: func(ctx context.Context, store *Store) error {
			return store.Delete(ctx, "blobs/cancel")
		}},
		{name: "List", operation: "ListObjectsV2", call: func(ctx context.Context, store *Store) error {
			_, err := store.List(ctx, "blobs/")
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := testserver.New()
			t.Cleanup(server.Close)
			started := server.BlockNext(tt.operation)
			store := newIntegrationStore(t, server, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- tt.call(ctx, store) }()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatalf("%s request never reached fixture", tt.operation)
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("%s cancellation error = %T %v, want context.DeadlineExceeded", tt.name, err, err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s ignored cancellation", tt.name)
			}
		})
	}
}

func TestRetryUsesStandardClassificationAndReplayableBodies(t *testing.T) {
	t.Run("classified transient", func(t *testing.T) {
		server := testserver.New()
		t.Cleanup(server.Close)
		server.FailNext("PutPayload", 1, http.StatusInternalServerError, "InternalError")
		store := newIntegrationStore(t, server, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.Put(ctx, "blobs/retry", strings.NewReader("replayable")); err != nil {
			t.Fatalf("Put after transient failure: %v", err)
		}
		if attempts := server.Count("PutPayload"); attempts != 2 {
			t.Fatalf("PutPayload attempts = %d, want 2", attempts)
		}
	})
	t.Run("unclassified client failure", func(t *testing.T) {
		server := testserver.New()
		t.Cleanup(server.Close)
		server.FailNext("PutPayload", 1, http.StatusBadRequest, "InvalidRequest")
		store := newIntegrationStore(t, server, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.Put(ctx, "blobs/no-retry", strings.NewReader("bytes")); err == nil {
			t.Fatal("Put after unclassified 400 returned nil")
		}
		if attempts := server.Count("PutPayload"); attempts != 1 {
			t.Fatalf("PutPayload attempts = %d, want 1", attempts)
		}
	})
	t.Run("multipart part is replayable", func(t *testing.T) {
		server := testserver.New()
		t.Cleanup(server.Close)
		server.FailNext("UploadPart", 1, http.StatusInternalServerError, "InternalError")
		store := newIntegrationStore(t, server, func(options *Options) {
			options.MultipartThreshold = minMultipartBytes
			options.MultipartPartSize = minMultipartBytes
			options.Concurrency = 1
			options.MaxConcurrentTransfers = 1
		})
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		body := bytes.Repeat([]byte("m"), int(minMultipartBytes+1))
		if err := store.Put(ctx, "blobs/multipart-retry", bytes.NewReader(body)); err != nil {
			t.Fatalf("multipart Put: %v", err)
		}
		if attempts := server.Count("UploadPart"); attempts != 3 {
			t.Fatalf("UploadPart attempts = %d, want 3 (two parts plus one retry)", attempts)
		}
	})
}

func TestSourceReadFailureIsNotRetriedOrUploaded(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := store.Put(ctx, "blobs/source-failure", sourceFailureReader{})
	if err == nil {
		t.Fatal("Put with source read failure returned nil")
	}
	if strings.Contains(err.Error(), "tenant-secret") {
		t.Fatalf("Put error disclosed source detail: %q", err)
	}
	if requests := server.Count("PutPayload"); requests != 0 {
		t.Fatalf("PutPayload requests = %d, want 0 because the non-replayable source failed before any request", requests)
	}
}

func TestPayloadVerificationRangesAreBoundedAtEnds(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body := bytes.Repeat([]byte("r"), int(verificationRangeBytes+1))
	if err := store.Put(ctx, "blobs/ranges", bytes.NewReader(body)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ranges := server.Ranges()
	wantFirst := fmt.Sprintf("bytes=0-%d", verificationRangeBytes-1)
	wantSecond := fmt.Sprintf("bytes=%d-%d", verificationRangeBytes, verificationRangeBytes)
	if len(ranges) < 2 || ranges[0] != wantFirst || ranges[1] != wantSecond {
		t.Fatalf("verification ranges = %v, want first two [%q %q]", ranges, wantFirst, wantSecond)
	}
	ifMatches := server.RangeIfMatches()
	if len(ifMatches) < 2 || ifMatches[0] == "" || ifMatches[1] == "" || ifMatches[0] != ifMatches[1] {
		t.Fatalf("range If-Match headers = %v, want the same non-empty HEAD ETag", ifMatches)
	}
	if heads := server.Count("HeadPayload"); heads != 1 {
		t.Fatalf("payload HEAD requests = %d, want 1", heads)
	}
}

func TestOpenGetReaderHoldsTransferSlotUntilClose(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, func(options *Options) { options.MaxConcurrentTransfers = 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Put(ctx, "blobs/open-reader", strings.NewReader("bytes")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	reader, err := store.Get(ctx, "blobs/open-reader")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	blocked, cancelBlocked := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBlocked()
	if _, err := store.List(blocked, ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("List with open Get reader error = %T %v, want context.DeadlineExceeded", err, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := store.List(ctx, ""); err != nil {
		t.Fatalf("List after Close: %v", err)
	}
}

func concurrentPuts(ctx context.Context, store *Store, key string, bodies []string) []error {
	start := make(chan struct{})
	errs := make([]error, len(bodies))
	var wait sync.WaitGroup
	for index, body := range bodies {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errs[index] = store.Put(ctx, key, strings.NewReader(body))
		}()
	}
	close(start)
	wait.Wait()
	return errs
}

func newIntegrationStore(t *testing.T, server *testserver.Server, mutate func(*Options)) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	options := validOptions()
	options.Endpoint = server.URL()
	options.AllowInsecureLocalhostOnly = true
	options.AddressingStyle = AddressingPath
	options.Credentials = signableCredentials{}
	if mutate != nil {
		mutate(&options)
	}
	store, err := Open(ctx, options)
	if err != nil {
		t.Fatalf("Open integration store: %v", err)
	}
	return store
}

func readIntegrationBlob(t *testing.T, ctx context.Context, store *Store, key string) []byte {
	t.Helper()
	reader, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll(%q): %v", key, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close(%q): %v", key, err)
	}
	return body
}

type sourceFailureReader struct{}

func (sourceFailureReader) Read([]byte) (int, error) {
	return 0, errors.New("source failure with tenant-secret")
}
