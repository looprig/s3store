//go:build integration

package s3store

import (
	"bytes"
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/looprig/s3store/internal/testserver"
)

// multipartOptions configures the smallest upload the SDK will split, so the
// multipart path is exercised without a large fixture body.
func multipartOptions(options *Options) {
	options.MultipartThreshold = minMultipartBytes
	options.MultipartPartSize = minMultipartBytes
	options.Concurrency = 1
	options.MaxConcurrentTransfers = 1
}

func multipartBody() []byte {
	return bytes.Repeat([]byte("m"), int(minMultipartBytes+1))
}

// TestFailedMultipartUploadIsAbortedAndLeavesNoObject covers the abort half of
// the orphan question: a multipart upload whose parts fail past the bounded
// retry classifier must be aborted, must leave no in-progress upload behind,
// and must commit neither a payload nor a manifest.
//
// The abort asserted here is the transfer's own, issued while the upload is
// still owned by the failing Put. It is not orphan collection: nothing here
// looks for, or acts on, an upload from another operation or another process.
func TestFailedMultipartUploadIsAbortedAndLeavesNoObject(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	// Three failures exhaust the pinned three-attempt retry budget for one part.
	server.FailNext("UploadPart", 3, http.StatusInternalServerError, "InternalError")
	store := newIntegrationStore(t, server, multipartOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := store.Put(ctx, "blobs/aborted-multipart", bytes.NewReader(multipartBody())); err == nil {
		t.Fatal("Put with an exhausted retry budget returned nil")
	}
	created := server.CreatedUploads()
	if len(created) == 0 {
		t.Fatal("no multipart upload was created; the abort assertions would be vacuous")
	}
	aborted := server.AbortedUploads()
	for _, uploadID := range created {
		if !slices.Contains(aborted, uploadID) {
			t.Errorf("upload %q was created and never aborted; a failed transfer must not leave an upload behind", uploadID)
		}
	}
	if active := server.ActiveUploads(); active != 0 {
		t.Errorf("active multipart uploads = %d, want 0", active)
	}
	if keys := server.Keys("looprig-test"); len(keys) != 0 {
		t.Errorf("committed objects = %v, want none from a failed multipart upload", keys)
	}
	// The payload was never committed, so there is nothing this Put may delete.
	//
	// Measured scope: the mutation this row kills is "cleanup requires
	// something committed" -- the guard fired unconditionally. It does NOT
	// separate the two conjuncts of that guard: a failed multipart upload
	// leaves payloadOwned and cleanupSafe both false, so weakening either one
	// alone survives here, as the campaign records. The conjuncts are
	// separated by TestPutNeverDeletesPayloadItDidNotCreate instead.
	if deletes := server.Count("DeletePayload"); deletes != 0 {
		t.Errorf("payload deletes = %d, want 0; a failed upload owns no committed payload to reclaim", deletes)
	}
}

// TestMultipartAbortNamesOnlyUploadsThisTransferCreated is the precondition
// step 3 states: an abort may only name an upload this operation owns. The
// failing row proves aborts happen at all; the succeeding row proves they are
// not issued speculatively. Together they pin abort IDs to a subset of the IDs
// the fixture issued to this Store, which is the strongest form of "proven
// unreferenced" available inside a provider that cannot see a retention
// process.
func TestMultipartAbortNamesOnlyUploadsThisTransferCreated(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		partFaults  int
		wantSuccess bool
		wantAborts  int
	}{
		{name: "failed transfer aborts its own upload", partFaults: 3, wantAborts: 1},
		{name: "successful transfer aborts nothing", wantSuccess: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := testserver.New()
			t.Cleanup(server.Close)
			if testCase.partFaults > 0 {
				server.FailNext("UploadPart", testCase.partFaults, http.StatusInternalServerError, "InternalError")
			}
			store := newIntegrationStore(t, server, multipartOptions)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := store.Put(ctx, "blobs/abort-scope", bytes.NewReader(multipartBody()))
			if testCase.wantSuccess != (err == nil) {
				t.Fatalf("Put = %v, want success = %v", err, testCase.wantSuccess)
			}
			created, aborted := server.CreatedUploads(), server.AbortedUploads()
			if len(created) == 0 {
				t.Fatal("no multipart upload was created; the scope assertion would be vacuous")
			}
			if len(aborted) != testCase.wantAborts {
				t.Errorf("aborted uploads = %v, want %d", aborted, testCase.wantAborts)
			}
			for _, uploadID := range aborted {
				if !slices.Contains(created, uploadID) {
					t.Errorf("abort named upload %q, which this fixture never issued to this Store", uploadID)
				}
			}
		})
	}
}

// TestNoOrphanCollectionRequestIsIssued states the deferral as behaviour rather
// than as a comment: across a successful transfer, a failed transfer, and a
// Delete, this module never enumerates uploads and never touches an object
// outside the one logical key it was given.
func TestNoOrphanCollectionRequestIsIssued(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, multipartOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// An upload the fixture holds open and that this Store does not own. A
	// module that swept for orphans would find and abort it.
	server.OpenForeignUpload("looprig-test", "tenants/other/payloads/v1/foreign")
	foreign := server.CreatedUploads()
	if len(foreign) != 1 {
		t.Fatalf("foreign uploads = %v, want exactly one", foreign)
	}

	if err := store.Put(ctx, "blobs/orphan-scope", bytes.NewReader(multipartBody())); err != nil {
		t.Fatalf("successful Put: %v", err)
	}
	server.FailNext("UploadPart", 3, http.StatusInternalServerError, "InternalError")
	if err := store.Put(ctx, "blobs/orphan-scope-failed", bytes.NewReader(multipartBody())); err == nil {
		t.Fatal("Put with an exhausted retry budget returned nil")
	}
	if err := store.Delete(ctx, "blobs/orphan-scope"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if listings := server.Count("ListMultipartUploads"); listings != 0 {
		t.Errorf("ListMultipartUploads requests = %d, want 0; enumerating uploads is the first step of a sweep this module must not perform", listings)
	}
	for _, uploadID := range server.AbortedUploads() {
		if uploadID == foreign[0] {
			t.Errorf("aborted foreign upload %q; an upload may be aborted only by the transfer that created it", uploadID)
		}
	}
	if active := server.ActiveUploads(); active != 1 {
		t.Errorf("active uploads = %d, want the 1 foreign upload left untouched", active)
	}
}
