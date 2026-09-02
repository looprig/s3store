package s3store

// EncryptionPolicyError reports that the deployment requires confirmed
// server-side encryption but the selected posture is one this module can
// neither configure nor confirm. It names the posture and the reason and
// retains no caller value and no cause, so a deployment may record it verbatim.
type EncryptionPolicyError struct {
	Mode   EncryptionMode
	Reason string
}

func (e *EncryptionPolicyError) Error() string {
	return "s3store: encryption posture " + e.Mode.String() + " does not satisfy the deployment policy: " + e.Reason
}

// String names a posture for an error or a record. The vocabulary is fixed and
// contains no caller-supplied value.
func (m EncryptionMode) String() string {
	switch m {
	case EncryptionUnspecified:
		return "unspecified"
	case EncryptionBucketDefault:
		return "bucket-default"
	case EncryptionAES256:
		return "aes256"
	case EncryptionKMS:
		return "kms"
	default:
		return "unknown"
	}
}

// confirmedByThisModule reports whether s3store itself puts the posture on the
// wire. Only AES256 and KMS qualify: for those two, every object-creating
// request this module issues carries the corresponding server-side encryption
// header, which is a property of this module's own code and is asserted by
// TestEveryObjectCreatingRequestCarriesTheConfiguredEncryptionHeader.
//
// EncryptionBucketDefault does not qualify. It delegates to a bucket policy
// enforced outside this module, and Open deliberately performs no S3 request,
// so nothing observable here distinguishes a bucket that enforces encryption
// from one that does not. That is a statement about Open, not about the bucket:
// a correctly configured bucket-default deployment is still refused under a
// confirmation requirement, because the confirmation is what is missing.
func (m EncryptionMode) confirmedByThisModule() bool {
	return m == EncryptionAES256 || m == EncryptionKMS
}

// requireConfirmedEncryption applies the deployment policy. It runs after the
// posture itself has been validated, so EncryptionUnspecified has already been
// rejected as an *OptionsError and never reaches here.
func requireConfirmedEncryption(required bool, mode EncryptionMode) error {
	if !required || mode.confirmedByThisModule() {
		return nil
	}
	return &EncryptionPolicyError{
		Mode: mode,
		Reason: "RequireConfirmedEncryption is set, but this posture is enforced by an " +
			"external bucket policy that Open cannot confirm without a service request",
	}
}
