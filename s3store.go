// Package s3store provides S3-compatible immutable blob storage.
package s3store

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/looprig/s3store/internal/guard"
)

type DeadlineRequiredError = guard.DeadlineRequiredError
type NotImplementedError = guard.NotImplementedError
type UnconstructedStoreError = guard.UnconstructedStoreError

// Store implements storage.Blobs and the optional storage.BlobReaderLifecycle
// capability. Structured primitives and SessionStore composition deliberately
// live in other modules.
type Store struct {
	client    *s3.Client
	transfers *transfermanager.Client
	options   resolvedOptions
	// transferSlots is the Store-wide operation and live-response-body bound.
	transferSlots chan struct{}
}

type configLoader func(context.Context, resolvedOptions) (aws.Config, error)

var loadConfig configLoader = defaultLoadConfig

// Open validates configuration, requires a caller deadline, and constructs
// lazy SDK clients. It performs no request, bucket probe, or mutation.
func Open(ctx context.Context, options Options) (*Store, error) {
	resolved, err := options.resolve()
	if err != nil {
		return nil, err
	}
	if err := guard.RequireDeadline(ctx, "Open"); err != nil {
		return nil, err
	}
	awsConfig, err := loadConfig(ctx, resolved)
	if err != nil {
		// SDK configuration errors can contain provider details. Do not wrap,
		// retain, or expose them.
		return nil, invalidOption("Credentials", "AWS configuration could not be loaded securely")
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(resolved.endpoint)
		options.UsePathStyle = resolved.addressingStyle == AddressingPath
	})
	transfers := transfermanager.New(client, func(options *transfermanager.Options) {
		options.MultipartUploadThreshold = resolved.multipartThreshold
		options.PartSizeBytes = resolved.multipartPartSize
		options.Concurrency = resolved.concurrency
		options.GetObjectBufferSize = int64(resolved.concurrency) * resolved.multipartPartSize
		// Pin the part ceiling instead of inheriting it. This is a no-op
		// today: the SDK's default is the same 10000, which is also its hard
		// maximum, so removing this line changes nothing observable and no
		// mutation of it can fail. It exists against a future change to that
		// default, which TestMaxUploadPartsPinMatchesTheSDKDefault detects and
		// which would make maxAccountedObjectSize wrong.
		options.MaxUploadParts = maxUploadParts
	})
	return newStore(client, transfers, resolved), nil
}

// newStore is the only constructor of Store. Building a Store any other way
// leaves transferSlots nil, and a send on a nil channel blocks forever, so the
// invariant is enforced by TestStoreIsBuiltOnlyByItsConstructor rather than
// left to convention.
func newStore(client *s3.Client, transfers *transfermanager.Client, resolved resolvedOptions) *Store {
	return &Store{
		client: client, transfers: transfers, options: resolved,
		transferSlots: make(chan struct{}, resolved.maxConcurrentTransfers),
	}
}

func defaultLoadConfig(ctx context.Context, options resolvedOptions) (aws.Config, error) {
	loadOptions := []func(*config.LoadOptions) error{
		config.WithRegion(options.region),
		// s3store verifies committed payloads with its own full-object SHA-256.
		// Requiring SDK checksums avoids redundant aws-chunked bodies and warnings
		// from compatible services that do not return optional S3 checksums.
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		// Standard retry classification is a function of Smithy error/status
		// classes. Pin its finite attempt count rather than maintaining a code
		// denylist or retrying unclassified failures.
		config.WithRetryMaxAttempts(3),
	}
	if options.credentials != nil {
		loadOptions = append(loadOptions, config.WithCredentialsProvider(options.credentials))
	}
	return config.LoadDefaultConfig(ctx, loadOptions...)
}
