package s3store

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/looprig/storage"
)

func TestRedactedErrorTextDropsTenantScopedIdentifiers(t *testing.T) {
	t.Parallel()
	const secretKey = "tenant-acme/session-1234/object-super-secret"

	// The returned error must keep the key: storage's Blobs conformance suite
	// asserts InvalidNameError.Name equals the offending key.
	returned := validateBlobKey(secretKey + "/../escape")
	var invalidName *storage.InvalidNameError
	if !errors.As(returned, &invalidName) {
		t.Fatalf("validateBlobKey error = %T %v, want *storage.InvalidNameError", returned, returned)
	}
	if !strings.Contains(returned.Error(), "super-secret") {
		t.Fatal("returned error dropped the key; storage/storetest requires InvalidNameError.Name to equal it")
	}

	recorded := RedactedErrorText(returned)
	if recorded == "" {
		t.Fatal("RedactedErrorText returned an empty string for a non-nil error")
	}
	for _, identifier := range []string{"tenant-acme", "session-1234", "super-secret", "escape"} {
		if strings.Contains(recorded, identifier) {
			t.Errorf("recorded text disclosed %q: %q", identifier, recorded)
		}
	}
	if !strings.Contains(recorded, invalidName.Rule) {
		t.Errorf("recorded text = %q, want the grammar rule %q retained", recorded, invalidName.Rule)
	}

	// Wrapping must not defeat the classification. Asserting only the absence
	// of the key is not enough: falling through to redactedText also contains
	// no key, so the negative alone is satisfied by the very failure it means
	// to exclude. Assert the positive -- a wrapped error classifies exactly as
	// the unwrapped one -- or P2.2's wrapped errors silently degrade to
	// "withheld" and lose their failure class.
	wrapped := fmt.Errorf("Blobs.Put: %w", returned)
	if got, want := RedactedErrorText(wrapped), RedactedErrorText(returned); got != want {
		t.Errorf("wrapped error text = %q, want the unwrapped classification %q", got, want)
	}
	if strings.Contains(RedactedErrorText(wrapped), "super-secret") {
		t.Errorf("wrapped error disclosed the key: %q", RedactedErrorText(wrapped))
	}
}

func TestRedactedErrorTextClassifiesPackageErrors(t *testing.T) {
	t.Parallel()
	if got := RedactedErrorText(nil); got != "" {
		t.Errorf("RedactedErrorText(nil) = %q, want the empty string", got)
	}
	options := validOptions()
	options.Endpoint = "https://access:super-secret@s3.example.test"
	_, optionsErr := options.resolve()
	if got := RedactedErrorText(optionsErr); got != optionsErr.Error() || strings.Contains(got, "super-secret") {
		t.Errorf("OptionsError text = %q, want the already-redacted %q", got, optionsErr.Error())
	}
	if got := RedactedErrorText(&DeadlineRequiredError{Operation: "Blobs.Put"}); !strings.Contains(got, "Blobs.Put") {
		t.Errorf("DeadlineRequiredError text = %q, want the operation name", got)
	}
	if got := RedactedErrorText(&NotImplementedError{Operation: "Blobs.Get"}); !strings.Contains(got, "Blobs.Get") {
		t.Errorf("NotImplementedError text = %q, want the operation name", got)
	}
	if got := RedactedErrorText(&UnconstructedStoreError{Operation: "Blobs.Put"}); !strings.Contains(got, "Blobs.Put") {
		t.Errorf("UnconstructedStoreError text = %q, want the operation name", got)
	}
	for _, packageErr := range []error{
		&BackendError{Operation: "payload upload"},
		&BlobIntegrityError{Operation: "payload digest"},
		&ObjectTooLargeError{Maximum: 42},
	} {
		if got := RedactedErrorText(packageErr); got != packageErr.Error() {
			t.Errorf("RedactedErrorText(%T) = %q, want the typed classification %q", packageErr, got, packageErr.Error())
		}
	}
}

// TestRedactedErrorTextFailsClosedOnUnknownErrors is the important half: an
// error this package cannot classify may carry an identifier or SDK detail, so
// it must not be recorded verbatim.
func TestRedactedErrorTextFailsClosedOnUnknownErrors(t *testing.T) {
	t.Parallel()
	unknown := errors.New("s3: AccessDenied for tenant-acme/session-1234 using AKIA_TEST_KEY")
	got := RedactedErrorText(unknown)
	if got != redactedText {
		t.Fatalf("unclassified error text = %q, want the fixed %q", got, redactedText)
	}
	for _, identifier := range []string{"tenant-acme", "session-1234", "AKIA_TEST_KEY"} {
		if strings.Contains(got, identifier) {
			t.Errorf("unclassified error disclosed %q", identifier)
		}
	}
}
