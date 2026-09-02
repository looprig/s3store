package s3store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestOpenRefusesUnconfirmableEncryptionWhenPolicyRequiresIt covers both
// directions of the deployment-policy rule, and both directions of the posture
// it is applied to, so neither the requirement nor the exemption can be deleted
// unnoticed. The refusal is asserted through the exported constructor because
// that is where a deployment meets it.
func TestOpenRefusesUnconfirmableEncryptionWhenPolicyRequiresIt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		required     bool
		mode         EncryptionMode
		kmsKeyID     string
		wantRefusal  bool
		wantModeText string
	}{
		{name: "policy requires and s3store configures AES256", required: true, mode: EncryptionAES256},
		{name: "policy requires and s3store configures KMS", required: true, mode: EncryptionKMS, kmsKeyID: "alias/looprig"},
		{
			name: "policy requires but the bucket-default posture is unconfirmable", required: true,
			mode: EncryptionBucketDefault, wantRefusal: true, wantModeText: "bucket-default",
		},
		{name: "no policy requirement leaves the bucket-default posture accepted", mode: EncryptionBucketDefault},
		{name: "no policy requirement leaves AES256 accepted", mode: EncryptionAES256},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			options := validOptions()
			options.Encryption = testCase.mode
			options.KMSKeyID = testCase.kmsKeyID
			options.RequireConfirmedEncryption = testCase.required
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store, err := Open(ctx, options)
			if !testCase.wantRefusal {
				if err != nil {
					t.Fatalf("Open = %v, want a Store", err)
				}
				if store == nil {
					t.Fatal("Open returned nil Store and nil error")
				}
				return
			}
			if store != nil {
				t.Fatalf("Open returned a Store for a posture the deployment policy forbids")
			}
			var policy *EncryptionPolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("Open = %v (%T), want *EncryptionPolicyError", err, err)
			}
			if policy.Mode != testCase.mode {
				t.Errorf("EncryptionPolicyError.Mode = %v, want %v", policy.Mode, testCase.mode)
			}
			if !strings.Contains(policy.Error(), testCase.wantModeText) {
				t.Errorf("EncryptionPolicyError text = %q, want it to name the %q posture", policy.Error(), testCase.wantModeText)
			}
		})
	}
}

// TestEncryptionPolicyErrorIsRecordableWithoutIdentifiers pins the two
// properties that make the refusal safe to put in a deployment's startup log:
// it interpolates no caller value, and RedactedErrorText classifies it rather
// than withholding it as an unknown error.
func TestEncryptionPolicyErrorIsRecordableWithoutIdentifiers(t *testing.T) {
	t.Parallel()
	options := validOptions()
	options.Bucket = "secret-bucket-name"
	options.DeploymentPrefix = "tenants/secret-tenant"
	options.Endpoint = "https://secret-endpoint.example.test"
	options.Encryption = EncryptionBucketDefault
	options.RequireConfirmedEncryption = true
	_, err := options.resolve()
	var policy *EncryptionPolicyError
	if !errors.As(err, &policy) {
		t.Fatalf("resolve = %v (%T), want *EncryptionPolicyError", err, err)
	}
	recorded := RedactedErrorText(policy)
	if recorded != policy.Error() {
		t.Errorf("RedactedErrorText(*EncryptionPolicyError) = %q, want the typed classification %q", recorded, policy.Error())
	}
	for _, identifier := range []string{"secret-bucket-name", "secret-tenant", "secret-endpoint"} {
		if strings.Contains(recorded, identifier) {
			t.Errorf("recorded text %q disclosed %q", recorded, identifier)
		}
	}
	if errors.Unwrap(policy) != nil {
		t.Errorf("EncryptionPolicyError unwraps to %v, want no retained cause", errors.Unwrap(policy))
	}
}

// TestEncryptionModeNamesEveryDeclaredPosture keeps the mode vocabulary used in
// the refusal exhaustive. It is the fixture-default check for the table above:
// that table names only the bucket-default posture, so without this row a new
// unconfirmable posture could be added with no name and no policy decision.
func TestEncryptionModeNamesEveryDeclaredPosture(t *testing.T) {
	t.Parallel()
	want := map[EncryptionMode]string{
		EncryptionUnspecified:   "unspecified",
		EncryptionBucketDefault: "bucket-default",
		EncryptionAES256:        "aes256",
		EncryptionKMS:           "kms",
	}
	for mode := EncryptionUnspecified; mode <= EncryptionKMS; mode++ {
		if got := mode.String(); got != want[mode] {
			t.Errorf("EncryptionMode(%d).String() = %q, want %q", mode, got, want[mode])
		}
	}
	if got := EncryptionMode(EncryptionKMS + 1).String(); !strings.Contains(got, "unknown") {
		t.Errorf("EncryptionMode(%d).String() = %q, want it to read as unknown", EncryptionKMS+1, got)
	}
	// confirmedByThisModule must be decided for every declared posture, and
	// exactly the postures s3store itself sets a header for may be confirmed.
	confirmable := map[EncryptionMode]bool{
		EncryptionUnspecified:   false,
		EncryptionBucketDefault: false,
		EncryptionAES256:        true,
		EncryptionKMS:           true,
	}
	for mode, want := range confirmable {
		if got := mode.confirmedByThisModule(); got != want {
			t.Errorf("EncryptionMode(%v).confirmedByThisModule() = %v, want %v", mode, got, want)
		}
	}
}
