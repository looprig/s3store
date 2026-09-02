package s3store

import (
	"errors"

	"github.com/looprig/s3store/internal/guard"
	"github.com/looprig/storage"
)

// redactedText is returned for any error this package cannot classify. An
// unclassified error may embed a tenant/session identifier, a bucket key, or
// SDK-supplied detail, so recording it verbatim is never safe.
const redactedText = "s3store: error withheld from the record"

// RedactedErrorText renders err for recording without tenant- or
// session-scoped identifiers.
//
// It exists because the two requirements genuinely conflict. Storage's shared
// Blobs conformance suite requires an invalid key to surface as a
// *storage.InvalidNameError whose Name is exactly the offending key, so
// validateBlobKey must not discard it and the returned error is not safe to
// record. Anything that records, logs, or emits an s3store error as telemetry
// must pass it through here first; the returned string names the failure class
// and the grammar rule only.
func RedactedErrorText(err error) string {
	if err == nil {
		return ""
	}

	// Typed package errors are constructed without interpolating any caller
	// value, so their own text is already safe.
	var optionsErr *OptionsError
	if errors.As(err, &optionsErr) {
		return optionsErr.Error()
	}
	var deadlineErr *guard.DeadlineRequiredError
	if errors.As(err, &deadlineErr) {
		return deadlineErr.Error()
	}
	var notImplemented *guard.NotImplementedError
	if errors.As(err, &notImplemented) {
		return notImplemented.Error()
	}
	var unconstructed *guard.UnconstructedStoreError
	if errors.As(err, &unconstructed) {
		return unconstructed.Error()
	}
	var readerClosed *BlobReaderClosedError
	if errors.As(err, &readerClosed) {
		return readerClosed.Error()
	}
	var encryptionPolicy *EncryptionPolicyError
	if errors.As(err, &encryptionPolicy) {
		return encryptionPolicy.Error()
	}
	var backend *BackendError
	if errors.As(err, &backend) {
		return backend.Error()
	}
	var integrity *BlobIntegrityError
	if errors.As(err, &integrity) {
		return integrity.Error()
	}
	var tooLarge *ObjectTooLargeError
	if errors.As(err, &tooLarge) {
		return tooLarge.Error()
	}

	// InvalidNameError deliberately retains the offending key. Keep the
	// grammar rule, which is a fixed vocabulary, and drop the key.
	var invalidName *storage.InvalidNameError
	if errors.As(err, &invalidName) {
		return "s3store: invalid storage name: " + invalidName.Rule
	}

	return redactedText
}
