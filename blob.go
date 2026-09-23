package s3store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/looprig/s3store/internal/guard"
	"github.com/looprig/storage"
)

const (
	verificationRangeBytes int64 = 1 << 20
	listPageSize                 = 1000
)

// BackendError reports an S3 failure without retaining an SDK error whose text
// may contain tenant keys, endpoints, or credential-provider details.
type BackendError struct {
	Operation string
}

func (e *BackendError) Error() string {
	return "s3store: backend operation failed during " + e.Operation
}

// Put stages a uniquely-owned payload, hashes it during upload, verifies the
// committed bytes, then atomically publishes the logical manifest.
func (s *Store) Put(ctx context.Context, key string, source io.Reader) error {
	ctx, cancel, err := guard.Bound(ctx, "Blobs.Put", s.options.operationTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	if err := validateBlobKey(key); err != nil {
		return err
	}
	if source == nil {
		return &BackendError{Operation: "upload source"}
	}
	if err := s.acquireTransfer(ctx, "Blobs.Put"); err != nil {
		return err
	}
	defer s.releaseTransfer()

	manifestKey, err := manifestObjectKey(s.options.deploymentPrefix, key)
	if err != nil {
		return err
	}
	payloadKey, err := newPayloadObjectKey(s.options.deploymentPrefix, key)
	if err != nil {
		return err
	}
	// cleanupSafe carries the whole precondition: this Put uploaded payloadKey
	// AND nothing durable references it. It starts false because nothing has
	// been uploaded, and it is narrowed again once publication begins. There is
	// deliberately no second "did we upload it" flag: one was tried, and it was
	// set at the same statement as this one and never narrowed anywhere, so it
	// was a structurally dead conjunct that no test could separate.
	//
	// ctx.Err() is NOT part of that precondition and is not held by any probe.
	// It is a request-saving short-circuit: deletePayloadBestEffort passes ctx
	// straight to DeleteObject, so on a canceled context the request fails
	// without reaching the service anyway, and removing this conjunct changes
	// no outcome a test can observe. It is kept because issuing a request that
	// is known to fail is worse than not issuing it, and it is named here
	// rather than probed because there is nothing to probe. What DOES matter --
	// that cleanup never runs on a detached context that outlives the caller --
	// is a separate rule, held by
	// TestPutVerificationCancellationDoesNotStartDetachedCleanup.
	cleanupSafe := false
	defer func() {
		if cleanupSafe && ctx.Err() == nil {
			s.deletePayloadBestEffort(ctx, payloadKey)
		}
	}()

	accounted := newAccountedHashReader(source, s.options.maxAccountedObjectSize)
	uploadInput := &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.options.bucket), Key: aws.String(payloadKey), Body: accounted,
		IfNoneMatch: aws.String("*"),
	}
	s.applyTransferEncryption(uploadInput)
	uploaded, err := s.transfers.UploadObject(ctx, uploadInput)
	if err != nil {
		var tooLarge *ObjectTooLargeError
		if errors.As(err, &tooLarge) {
			return tooLarge
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &BackendError{Operation: "payload upload"}
	}
	cleanupSafe = true
	size := accounted.Size()
	digest := accounted.Digest()
	if uploaded.ContentLength == nil || *uploaded.ContentLength != size {
		return integrityError("upload length")
	}
	if err := s.verifyPayload(ctx, payloadKey, size, digest); err != nil {
		return err
	}
	manifest := blobManifest{LogicalKey: key, PayloadKey: payloadKey, Size: size, Digest: digest}
	encoded, err := encodeManifest(manifest)
	if err != nil {
		return err
	}
	// A logical key above 191 bytes may already hold a v0.1.1 row under that
	// release's single-segment key. Blobs are immutable, so that row decides
	// the outcome exactly as a manifest under the current key would; this
	// Put's staged payload is unreferenced either way, so cleanup stays safe.
	if settled, err := s.settleAgainstLegacyManifest(ctx, key, manifest); settled || err != nil {
		return err
	}
	// Once publication starts, an error can mean that S3 committed the
	// manifest but its acknowledgement was lost. Preserve this payload until a
	// successful read proves that the manifest points somewhere else.
	cleanupSafe = false
	created, publishErr := s.publishManifest(ctx, manifestKey, encoded)
	if publishErr == nil && created {
		return nil
	}
	if publishErr != nil && !isConditionalConflict(publishErr) {
		// A lost acknowledgement is ambiguous. Read the logical object before
		// reporting failure; a matching manifest proves this Put committed.
		existing, getErr := s.readManifest(ctx, manifestKey, key)
		if getErr == nil {
			cleanupSafe = existing.PayloadKey != payloadKey
			if manifestsMatch(existing, manifest) {
				return nil
			}
			return &storage.BlobConflictError{Key: key}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &BackendError{Operation: "manifest publish"}
	}
	existing, getErr := s.readManifest(ctx, manifestKey, key)
	if getErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &BackendError{Operation: "manifest conflict resolution"}
	}
	cleanupSafe = existing.PayloadKey != payloadKey
	if manifestsMatch(existing, manifest) {
		return nil
	}
	return &storage.BlobConflictError{Key: key}
}

// Get returns a stream whose EOF is conditional on exact length and digest.
// Its Store-wide slot remains held until the reader terminates or is closed.
//
// The returned reader supports bounded shutdown: Close returns within
// BlobReaderCloseBound even with a Read blocked on the network, and after Close
// returns every Read yields zero bytes and an error matching fs.ErrClosed,
// never io.EOF.
//
// One reader, one reading goroutine. Close may be called concurrently with a
// Read, which is what storage.BlobReaderLifecycle requires, but Read is NOT
// safe against another Read: the verifier behind the reader keeps its offset,
// hash, and terminal error unsynchronized. This differs from memstore, whose
// single mutex serializes everything and so tolerates concurrent Reads, so a
// consumer that reads one blob from several goroutines works there and races
// here.
//
// The stream is bounded by this call's context. The payload request is issued
// on a child of ctx, so a reader obtained under a short Get deadline is torn
// down when that deadline passes; pass a context whose deadline covers the
// read.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	// A default bound covers the returned stream too, so it is released only
	// when the reader terminates or closes, never when Get returns.
	ctx, cancelBound, err := guard.Bound(ctx, "Blobs.Get", s.options.operationTimeout)
	if err != nil {
		return nil, err
	}
	handedOff := false
	defer func() {
		if !handedOff {
			cancelBound()
		}
	}()
	if err := validateBlobKey(key); err != nil {
		return nil, err
	}
	if err := s.acquireTransfer(ctx, "Blobs.Get"); err != nil {
		return nil, err
	}
	release := true
	defer func() {
		if release {
			s.releaseTransfer()
		}
	}()
	_, manifest, err := s.locateManifest(ctx, key)
	if err != nil {
		return nil, err
	}
	// The payload request gets its own cancelable child of the caller context.
	// The caller's cancellation still reaches the stream, and Close gains an
	// unconditional way to abort a Read blocked on the socket, which is what
	// BlobReaderCloseBound promises. readCtx is canceled on every path that
	// does not hand the body to a blobReader.
	readCtx, abortRead := context.WithCancel(ctx)
	output, err := s.client.GetObject(readCtx, &s3.GetObjectInput{
		Bucket: aws.String(s.options.bucket), Key: aws.String(manifest.PayloadKey),
	})
	if err != nil {
		abortRead()
		if isHTTPStatus(err, 404) {
			return nil, integrityError("payload lookup")
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &BackendError{Operation: "payload get"}
	}
	if output.Body == nil || output.ContentLength == nil || *output.ContentLength != manifest.Size {
		if output.Body != nil {
			_ = output.Body.Close()
		}
		abortRead()
		return nil, integrityError("payload head")
	}
	verifier := newVerifyingBlobReader(output.Body, manifest.Size, manifest.Digest)
	release = false
	handedOff = true
	abort := func() {
		abortRead()
		cancelBound()
	}
	releaseStream := func() {
		s.releaseTransfer()
		cancelBound()
	}
	return newBlobReader(verifier, abort, releaseStream), nil
}

// Delete removes the logical manifest first. Payload cleanup cannot make the
// deleted key present again, so an unreachable payload is not a logical error.
func (s *Store) Delete(ctx context.Context, key string) error {
	ctx, cancel, err := guard.Bound(ctx, "Blobs.Delete", s.options.operationTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	if err := validateBlobKey(key); err != nil {
		return err
	}
	if err := s.acquireTransfer(ctx, "Blobs.Delete"); err != nil {
		return err
	}
	defer s.releaseTransfer()
	candidates, err := manifestCandidates(s.options.deploymentPrefix, key)
	if err != nil {
		return err
	}
	// Both encodings are removed: a key above 191 bytes can hold a v0.1.1 row,
	// and deleting only the current one would let the old value reappear.
	for index, manifestKey := range candidates {
		manifest, err := s.readManifestAt(ctx, manifestKey, key, index > 0)
		if err != nil {
			var notFound *storage.BlobNotFoundError
			if errors.As(err, &notFound) {
				continue
			}
			return err
		}
		_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.options.bucket), Key: aws.String(manifestKey),
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return &BackendError{Operation: "manifest delete"}
		}
		s.deletePayloadBestEffort(ctx, manifest.PayloadKey)
	}
	return nil
}

// manifestCandidates lists every backend key a logical key's manifest may
// live under: the current encoding first, then (index 1, when it differs)
// v0.1.1's legacy encoding.
func manifestCandidates(deploymentPrefix, key string) ([]string, error) {
	current, err := manifestObjectKey(deploymentPrefix, key)
	if err != nil {
		return nil, err
	}
	if legacy, differs := legacyManifestObjectKey(deploymentPrefix, key); differs {
		return []string{current, legacy}, nil
	}
	return []string{current}, nil
}

// locateManifest reads a logical key's manifest under the current encoding,
// then under v0.1.1's encoding when that differs. Absent from both is
// *storage.BlobNotFoundError.
func (s *Store) locateManifest(ctx context.Context, key string) (string, blobManifest, error) {
	candidates, err := manifestCandidates(s.options.deploymentPrefix, key)
	if err != nil {
		return "", blobManifest{}, err
	}
	var lastErr error
	for index, manifestKey := range candidates {
		manifest, err := s.readManifestAt(ctx, manifestKey, key, index > 0)
		if err == nil {
			return manifestKey, manifest, nil
		}
		var notFound *storage.BlobNotFoundError
		if !errors.As(err, &notFound) {
			return "", blobManifest{}, err
		}
		lastErr = err
	}
	return "", blobManifest{}, lastErr
}

// settleAgainstLegacyManifest decides a Put by an existing v0.1.1 row, if the
// key can have one. settled reports that the row exists and err is the Put's
// outcome (nil for identical content, a conflict otherwise).
//
// A v0.1.1 writer running concurrently with this release could still create
// such a row after this check; mixed-version writers are not supported for
// keys above 191 bytes, and on MinIO v0.1.1 cannot write them at all.
func (s *Store) settleAgainstLegacyManifest(ctx context.Context, key string, manifest blobManifest) (bool, error) {
	legacyKey, differs := legacyManifestObjectKey(s.options.deploymentPrefix, key)
	if !differs {
		return false, nil
	}
	existing, err := s.readLegacyManifest(ctx, legacyKey, key)
	if err != nil {
		var notFound *storage.BlobNotFoundError
		if errors.As(err, &notFound) {
			return false, nil
		}
		return true, err
	}
	if manifestsMatch(existing, manifest) {
		return true, nil
	}
	return true, &storage.BlobConflictError{Key: key}
}

// List validates each manifest row independently so one undecodable object
// cannot disable the rest of a tenant's page.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	ctx, cancel, err := guard.Bound(ctx, "Blobs.List", s.options.operationTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := validateListPrefix(prefix); err != nil {
		return nil, err
	}
	if err := s.acquireTransfer(ctx, "Blobs.List"); err != nil {
		return nil, err
	}
	defer s.releaseTransfer()
	backendPrefix := namespaceRoot(s.options.deploymentPrefix, manifestNamespace)
	seen := make(map[string]struct{})
	var continuation *string
	for {
		output, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.options.bucket), Prefix: aws.String(backendPrefix),
			ContinuationToken: continuation, MaxKeys: aws.Int32(listPageSize),
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, &BackendError{Operation: "manifest list"}
		}
		for _, row := range output.Contents {
			if row.Key == nil {
				continue
			}
			logical, ok := logicalKeyFromManifestObject(s.options.deploymentPrefix, *row.Key)
			if !ok || !strings.HasPrefix(logical, prefix) {
				continue
			}
			seen[logical] = struct{}{}
		}
		if !aws.ToBool(output.IsTruncated) {
			break
		}
		if output.NextContinuationToken == nil || *output.NextContinuationToken == "" ||
			(continuation != nil && *output.NextContinuationToken == *continuation) {
			return nil, integrityError("manifest listing page")
		}
		continuation = output.NextContinuationToken
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil, nil
	}
	return keys, nil
}

func (s *Store) publishManifest(ctx context.Context, key string, encoded []byte) (bool, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.options.bucket), Key: aws.String(key), Body: bytes.NewReader(encoded),
		ContentLength: aws.Int64(int64(len(encoded))), IfNoneMatch: aws.String("*"),
	}
	s.applyEncryption(input)
	_, err := s.client.PutObject(ctx, input)
	return err == nil, err
}

func (s *Store) readManifest(ctx context.Context, objectKey, logicalKey string) (blobManifest, error) {
	return s.readManifestAt(ctx, objectKey, logicalKey, false)
}

// readLegacyManifest reads v0.1.1's single-segment row. A service that limits
// segment length (MinIO) answers that key 400 rather than 404; such a service
// could never have stored the row, so the refusal proves it absent. A 400 is
// read that way ONLY for this key, never for one this release writes.
func (s *Store) readLegacyManifest(ctx context.Context, objectKey, logicalKey string) (blobManifest, error) {
	return s.readManifestAt(ctx, objectKey, logicalKey, true)
}

func (s *Store) readManifestAt(ctx context.Context, objectKey, logicalKey string, legacy bool) (blobManifest, error) {
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.options.bucket), Key: aws.String(objectKey),
	})
	if err != nil {
		if isHTTPStatus(err, 404) || (legacy && isHTTPStatus(err, 400)) {
			return blobManifest{}, &storage.BlobNotFoundError{Key: logicalKey}
		}
		if ctx.Err() != nil {
			return blobManifest{}, ctx.Err()
		}
		return blobManifest{}, &BackendError{Operation: "manifest head"}
	}
	if head.ContentLength == nil || *head.ContentLength < int64(manifestHeaderBytes) || *head.ContentLength > int64(maxManifestBytes) {
		return blobManifest{}, integrityError("manifest head")
	}
	length := *head.ContentLength
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.options.bucket), Key: aws.String(objectKey),
		Range: aws.String(fmt.Sprintf("bytes=0-%d", length-1)), IfMatch: head.ETag,
	})
	if err != nil {
		if isHTTPStatus(err, 404) {
			return blobManifest{}, integrityError("manifest vanished after head")
		}
		if ctx.Err() != nil {
			return blobManifest{}, ctx.Err()
		}
		return blobManifest{}, &BackendError{Operation: "manifest get"}
	}
	if output.Body == nil || output.ContentLength == nil || *output.ContentLength != length {
		if output.Body != nil {
			_ = output.Body.Close()
		}
		return blobManifest{}, integrityError("manifest range")
	}
	manifest, decodeErr := decodeManifest(output.Body, length)
	closeErr := output.Body.Close()
	if decodeErr != nil {
		return blobManifest{}, decodeErr
	}
	if closeErr != nil {
		return blobManifest{}, &BackendError{Operation: "manifest body close"}
	}
	if manifest.LogicalKey != logicalKey || !payloadObjectKeyMatches(s.options.deploymentPrefix, logicalKey, manifest.PayloadKey) {
		return blobManifest{}, integrityError("manifest binding")
	}
	return manifest, nil
}

func (s *Store) verifyPayload(ctx context.Context, key string, size int64, digest [sha256.Size]byte) error {
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.options.bucket), Key: aws.String(key),
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &BackendError{Operation: "payload verification head"}
	}
	if head.ContentLength == nil || *head.ContentLength != size {
		return integrityError("payload verification length")
	}
	hasher := sha256.New()
	for offset := int64(0); offset < size; offset += verificationRangeBytes {
		end := min(offset+verificationRangeBytes-1, size-1)
		expected := end - offset + 1
		output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.options.bucket), Key: aws.String(key),
			Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)), IfMatch: head.ETag,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return &BackendError{Operation: "payload verification range"}
		}
		if output.Body == nil || output.ContentLength == nil || *output.ContentLength != expected {
			if output.Body != nil {
				_ = output.Body.Close()
			}
			return integrityError("payload verification range length")
		}
		written, copyErr := io.CopyN(hasher, output.Body, expected)
		var probe [1]byte
		extra, probeErr := output.Body.Read(probe[:])
		closeErr := output.Body.Close()
		if copyErr != nil || written != expected || extra != 0 || (probeErr != nil && probeErr != io.EOF) {
			return integrityError("payload verification range body")
		}
		if closeErr != nil {
			return &BackendError{Operation: "payload verification body close"}
		}
	}
	var actual [sha256.Size]byte
	copy(actual[:], hasher.Sum(nil))
	if !equalDigest(actual[:], digest[:]) {
		return integrityError("payload verification digest")
	}
	return nil
}

func (s *Store) deletePayloadBestEffort(ctx context.Context, payloadKey string) {
	_, _ = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.options.bucket), Key: aws.String(payloadKey),
	})
}

func (s *Store) applyTransferEncryption(input *transfermanager.UploadObjectInput) {
	switch s.options.encryption {
	case EncryptionAES256:
		input.ServerSideEncryption = tmtypes.ServerSideEncryptionAes256
	case EncryptionKMS:
		input.ServerSideEncryption = tmtypes.ServerSideEncryptionAwsKms
		input.SSEKMSKeyID = aws.String(s.options.kmsKeyID)
	}
}

func (s *Store) applyEncryption(input *s3.PutObjectInput) {
	switch s.options.encryption {
	case EncryptionAES256:
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	case EncryptionKMS:
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		input.SSEKMSKeyId = aws.String(s.options.kmsKeyID)
	}
}

func manifestsMatch(left, right blobManifest) bool {
	return left.LogicalKey == right.LogicalKey && left.Size == right.Size && equalDigest(left.Digest[:], right.Digest[:])
}

// payloadRoot is the single derivation of one logical key's payload directory.
// Staging and matching share it so a payload cannot be written outside the
// deployment root while still matching the check that is supposed to catch it.
func payloadRoot(deploymentPrefix, logicalKey string) string {
	digest := sha256.Sum256([]byte(logicalKey))
	return namespaceRoot(deploymentPrefix, payloadNamespace) + hex.EncodeToString(digest[:]) + "/"
}

func newPayloadObjectKey(deploymentPrefix, logicalKey string) (string, error) {
	if err := storage.ValidateName(deploymentPrefix); err != nil {
		return "", err
	}
	if err := storage.ValidateName(logicalKey); err != nil {
		return "", err
	}
	var attempt [16]byte
	if _, err := rand.Read(attempt[:]); err != nil {
		return "", &BackendError{Operation: "payload identity"}
	}
	key := payloadRoot(deploymentPrefix, logicalKey) + hex.EncodeToString(attempt[:])
	if len(key) > maxS3ObjectKeyBytes {
		return "", invalidOption("DeploymentPrefix", "exceeds the S3 object-key limit")
	}
	return key, nil
}

func payloadObjectKeyMatches(deploymentPrefix, logicalKey, payloadKey string) bool {
	wantPrefix := payloadRoot(deploymentPrefix, logicalKey)
	if !strings.HasPrefix(payloadKey, wantPrefix) || len(payloadKey) != len(wantPrefix)+32 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(payloadKey, wantPrefix))
	return err == nil
}

func isConditionalConflict(err error) bool {
	return isHTTPStatus(err, 409) || isHTTPStatus(err, 412)
}

func isHTTPStatus(err error, status int) bool {
	var responseError *awshttp.ResponseError
	return errors.As(err, &responseError) && responseError.HTTPStatusCode() == status
}
