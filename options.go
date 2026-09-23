package s3store

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/looprig/storage"
)

const (
	maxDeploymentPrefixBytes            = 256
	minMultipartBytes             int64 = 5 << 20
	maxMultipartBytes             int64 = 5 << 30
	defaultMultipartThreshold     int64 = 64 << 20
	defaultMultipartPartSize      int64 = 16 << 20
	defaultConcurrency                  = 3
	maxConcurrency                      = 64
	defaultMaxConcurrentTransfers       = 4
	maxConcurrentTransfers              = 32
	maxAggregateTransferMemory    int64 = 512 << 20
	maxKMSKeyIDBytes                    = 2048
	minOperationTimeout                 = time.Millisecond
	// maxUploadParts is the transfer manager's hard ceiling on parts per
	// upload; it rejects any value above it. It is pinned here rather than
	// inherited so the accounted part size below is derived from a value this
	// module chose.
	maxUploadParts int64 = 10000
)

// DefaultOperationTimeout bounds an operation whose context carries no
// deadline, when Options.DefaultOperationTimeout is zero.
const DefaultOperationTimeout = 30 * time.Second

// AddressingStyle selects how bucket names are placed into S3 requests.
type AddressingStyle uint8

const (
	// AddressingVirtualHosted places the bucket in the request hostname.
	AddressingVirtualHosted AddressingStyle = iota
	// AddressingPath places the bucket in the URL path for compatible services.
	AddressingPath
)

// EncryptionMode controls the server-side encryption request policy.
type EncryptionMode uint8

const (
	// EncryptionUnspecified is the zero value and is always rejected. An unset
	// field must not read as an accepted encryption posture and must not
	// license Open to succeed; the caller states the posture explicitly.
	EncryptionUnspecified EncryptionMode = iota
	// EncryptionBucketDefault delegates to an externally enforced bucket
	// policy. P2.1 and P2.2 cannot confirm that policy; P2.3 owns verifying it
	// against a live service and failing startup when it cannot be confirmed.
	// Until then this mode is a declared intent, not a guarantee.
	EncryptionBucketDefault
	EncryptionAES256
	EncryptionKMS
)

// Options configures an S3-compatible immutable blob store.
type Options struct {
	Endpoint         string
	Region           string
	Bucket           string
	DeploymentPrefix string

	AddressingStyle AddressingStyle
	Encryption      EncryptionMode
	KMSKeyID        string

	// RequireConfirmedEncryption states that the deployment policy requires
	// server-side encryption. When it is set, Open accepts only a posture this
	// module itself puts on the wire, and refuses EncryptionBucketDefault with
	// a typed *EncryptionPolicyError: that posture is enforced by a bucket
	// policy Open cannot confirm, because Open issues no S3 request. Leaving
	// it false is the pre-existing behaviour and does not weaken any other
	// check; EncryptionUnspecified is still rejected either way.
	RequireConfirmedEncryption bool

	MultipartThreshold int64
	MultipartPartSize  int64
	Concurrency        int
	// MaxConcurrentTransfers bounds simultaneous Store operations and live Get
	// response bodies.
	MaxConcurrentTransfers int

	// Credentials injects a provider. Nil selects the AWS SDK's standard secure
	// credential chain. Raw access-key and secret-key fields are intentionally absent.
	Credentials aws.CredentialsProvider

	// DefaultOperationTimeout bounds every call (Open, Put, Get and its
	// returned stream, Delete, List) whose context has no deadline. The
	// Storage contract does not require callers to supply one, so the Store
	// supplies it: an unanswered request must not hang forever. A caller's
	// own deadline always wins, shorter or longer, and cancellation of the
	// caller's context is honoured either way. Zero selects the 30s
	// DefaultOperationTimeout; a negative or sub-millisecond value is rejected.
	DefaultOperationTimeout time.Duration

	// AllowInsecureLocalhostOnly permits HTTP solely for an explicitly selected
	// loopback test endpoint. Production and remote endpoints require HTTPS.
	AllowInsecureLocalhostOnly bool
}

// OptionsError reports one invalid field without retaining its value or an
// underlying parser/SDK cause.
type OptionsError struct {
	Field  string
	Reason string
}

func (e *OptionsError) Error() string {
	return "s3store: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
}

type resolvedOptions struct {
	endpoint           string
	region             string
	bucket             string
	deploymentPrefix   string
	addressingStyle    AddressingStyle
	encryption         EncryptionMode
	kmsKeyID           string
	multipartThreshold int64
	multipartPartSize  int64
	// maxAccountedObjectSize is the largest object for which multipartPartSize
	// still holds. Above it the transfer manager silently inflates part size to
	// objectSize/maxUploadParts+1 and the transfer-memory arithmetic below stops
	// describing reality, so Put rejects larger uploads.
	maxAccountedObjectSize int64
	concurrency            int
	maxConcurrentTransfers int
	credentials            aws.CredentialsProvider
	operationTimeout       time.Duration
}

func (o Options) resolve() (resolvedOptions, error) {
	endpoint, err := resolveEndpoint(o.Endpoint, o.AllowInsecureLocalhostOnly)
	if err != nil {
		return resolvedOptions{}, err
	}
	if !validRegion(o.Region) {
		if strings.TrimSpace(o.Region) == "" {
			return resolvedOptions{}, invalidOption("Region", "must be set")
		}
		return resolvedOptions{}, invalidOption("Region", "must contain only letters, digits, and hyphens and be at most 63 bytes")
	}
	if !validBucket(o.Bucket) {
		if strings.TrimSpace(o.Bucket) == "" {
			return resolvedOptions{}, invalidOption("Bucket", "must be set")
		}
		return resolvedOptions{}, invalidOption("Bucket", "must be a DNS-compatible S3 bucket name")
	}
	if o.DeploymentPrefix == "" {
		return resolvedOptions{}, invalidOption("DeploymentPrefix", "must be set")
	}
	if len(o.DeploymentPrefix) > maxDeploymentPrefixBytes {
		return resolvedOptions{}, invalidOption("DeploymentPrefix", "must be at most 256 bytes")
	}
	if storage.ValidateName(o.DeploymentPrefix) != nil {
		return resolvedOptions{}, invalidOption("DeploymentPrefix", "must be a canonical Storage name")
	}
	if o.AddressingStyle > AddressingPath {
		return resolvedOptions{}, invalidOption("AddressingStyle", "has an unknown mode")
	}
	if o.Encryption == EncryptionUnspecified {
		return resolvedOptions{}, invalidOption("Encryption", "must be set explicitly; the zero value is not an accepted posture")
	}
	if o.Encryption > EncryptionKMS {
		return resolvedOptions{}, invalidOption("Encryption", "has an unknown mode")
	}
	if err := validateKMSKey(o.Encryption, o.KMSKeyID); err != nil {
		return resolvedOptions{}, err
	}
	if err := requireConfirmedEncryption(o.RequireConfirmedEncryption, o.Encryption); err != nil {
		return resolvedOptions{}, err
	}

	threshold, err := resolveTransferSize("MultipartThreshold", o.MultipartThreshold, defaultMultipartThreshold)
	if err != nil {
		return resolvedOptions{}, err
	}
	partSize, err := resolveTransferSize("MultipartPartSize", o.MultipartPartSize, defaultMultipartPartSize)
	if err != nil {
		return resolvedOptions{}, err
	}
	if threshold < partSize {
		return resolvedOptions{}, invalidOption("MultipartThreshold", "must not be less than MultipartPartSize")
	}
	concurrency := o.Concurrency
	if concurrency < 0 {
		return resolvedOptions{}, invalidOption("Concurrency", "must be positive or zero for the default")
	}
	if concurrency == 0 {
		concurrency = defaultConcurrency
	}
	if concurrency > maxConcurrency {
		return resolvedOptions{}, invalidOption("Concurrency", "must be at most 64")
	}
	maxTransfers := o.MaxConcurrentTransfers
	if maxTransfers < 0 {
		return resolvedOptions{}, invalidOption("MaxConcurrentTransfers", "must be positive or zero for the default")
	}
	if maxTransfers == 0 {
		maxTransfers = defaultMaxConcurrentTransfers
	}
	if maxTransfers > maxConcurrentTransfers {
		return resolvedOptions{}, invalidOption("MaxConcurrentTransfers", "must be at most 32")
	}
	operationTimeout := o.DefaultOperationTimeout
	if operationTimeout == 0 {
		operationTimeout = DefaultOperationTimeout
	}
	if operationTimeout < minOperationTimeout {
		return resolvedOptions{}, invalidOption("DefaultOperationTimeout", "must be at least one millisecond or zero for the default")
	}
	partBuffers := int64(concurrency+1) * partSize
	perTransferMemory := threshold + partBuffers
	if perTransferMemory*int64(maxTransfers) > maxAggregateTransferMemory {
		return resolvedOptions{}, invalidOption("TransferMemoryBudget", "multipart sizes, concurrency, and MaxConcurrentTransfers must require at most 512 MiB")
	}

	return resolvedOptions{
		endpoint: endpoint, region: o.Region, bucket: o.Bucket,
		deploymentPrefix: o.DeploymentPrefix, addressingStyle: o.AddressingStyle,
		encryption: o.Encryption, kmsKeyID: o.KMSKeyID,
		multipartThreshold: threshold, multipartPartSize: partSize,
		maxAccountedObjectSize: accountedObjectSize(partSize),
		concurrency:            concurrency, maxConcurrentTransfers: maxTransfers,
		credentials: o.Credentials, operationTimeout: operationTimeout,
	}, nil
}

// accountedObjectSize returns the largest object size that does not trigger the
// transfer manager's part-size inflation. Inflation fires when
// objectSize/partSize >= maxUploadParts, so the last safe size is one byte
// below that product.
func accountedObjectSize(partSize int64) int64 {
	return partSize*maxUploadParts - 1
}

func invalidOption(field, reason string) *OptionsError {
	return &OptionsError{Field: field, Reason: reason}
}

func resolveEndpoint(raw string, allowInsecureLocal bool) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", invalidOption("Endpoint", "must be set")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.Opaque != "" {
		return "", invalidOption("Endpoint", "must be a valid URL with an absolute host")
	}
	if parsed.User != nil {
		return "", invalidOption("Endpoint", "must not contain userinfo")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", invalidOption("Endpoint", "must not contain a query")
	}
	if parsed.Fragment != "" || parsed.RawFragment != "" {
		return "", invalidOption("Endpoint", "must not contain a fragment")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", invalidOption("Endpoint", "must use the root path")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
	case "http":
		if !allowInsecureLocal {
			return "", invalidOption("Endpoint", "must use HTTPS; HTTP is test-only")
		}
		if !isLoopbackHost(parsed.Hostname()) {
			return "", invalidOption("Endpoint", "HTTP test endpoints must use a loopback host")
		}
	default:
		return "", invalidOption("Endpoint", "must use HTTPS")
	}
	return strings.ToLower(parsed.Scheme) + "://" + parsed.Host, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validRegion(region string) bool {
	if len(region) == 0 || len(region) > 63 || region[0] == '-' || region[len(region)-1] == '-' {
		return false
	}
	for i := 0; i < len(region); i++ {
		char := region[i]
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' {
			return false
		}
	}
	return true
}

func validBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || strings.Contains(bucket, "..") {
		return false
	}
	if ip := net.ParseIP(bucket); ip != nil {
		return false
	}
	for i := 0; i < len(bucket); i++ {
		char := bucket[i]
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '.' {
			return false
		}
		if (i == 0 || i == len(bucket)-1) && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func validateKMSKey(mode EncryptionMode, keyID string) error {
	if mode == EncryptionKMS {
		if keyID == "" {
			return invalidOption("KMSKeyID", "must be set when EncryptionKMS is selected")
		}
		if len(keyID) > maxKMSKeyIDBytes {
			return invalidOption("KMSKeyID", "must be at most 2048 bytes")
		}
		if !utf8.ValidString(keyID) || strings.IndexFunc(keyID, unicode.IsControl) >= 0 {
			return invalidOption("KMSKeyID", "must be valid UTF-8 without control characters")
		}
		return nil
	}
	if keyID != "" {
		return invalidOption("KMSKeyID", "may be set only when EncryptionKMS is selected")
	}
	return nil
}

func resolveTransferSize(field string, requested, fallback int64) (int64, error) {
	if requested == 0 {
		return fallback, nil
	}
	if requested < minMultipartBytes {
		return 0, invalidOption(field, "must be at least 5 MiB or zero for the default")
	}
	if requested > maxMultipartBytes {
		return 0, invalidOption(field, "must be at most 5 GiB")
	}
	return requested, nil
}
