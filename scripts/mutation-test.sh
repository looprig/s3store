#!/bin/sh
set -eu

group=${1:-all}
shard=${2:-0}
shards=${3:-1}
mutation_index=0
killed=0
snapshot_dir="/private/tmp/s3store-mutation-snapshot-${UID:-codex}"
files="go.mod options.go s3store.go blob.go blob_io.go blob_reader.go manifest.go key.go transfer.go redact.go encryption.go blob_test.go internal/guard/guard.go internal/testserver/server.go"

restore_snapshot() {
	for snapshot_file in $files; do
		if test -f "$snapshot_dir/$snapshot_file"; then
			cp "$snapshot_dir/$snapshot_file" "$snapshot_file"
		fi
	done
}

# Recover first from a prior interrupted run. This occurs before the new batch
# snapshot because an uncatchable kill cannot run EXIT cleanup.
if test -d "$snapshot_dir"; then
	restore_snapshot
	rm -rf "$snapshot_dir"
fi

mkdir -p "$snapshot_dir"
for file in $files; do
	mkdir -p "$snapshot_dir/$(dirname "$file")"
	cp "$file" "$snapshot_dir/$file"
done
trap 'restore_snapshot; rm -rf "$snapshot_dir"' EXIT HUP INT TERM

run_mutation() {
	mutation_group=$1
	name=$2
	file=$3
	old=$4
	new=$5
	test_name=$6
	want=$7
	tags=${8:-}

	if test "$group" != all && test "$group" != "$mutation_group"; then
		return
	fi
	current_index=$mutation_index
	mutation_index=$((mutation_index + 1))
	if test $((current_index % shards)) -ne "$shard"; then
		return
	fi
	restore_snapshot
	GOWORK=off GOCACHE=/private/tmp/s3store-gocache go test $tags -list "^${test_name}$" . | grep -qx "$test_name"
	if ! grep -Fq "$old" "$file"; then
		echo "mutation pattern not found: $name"
		exit 1
	fi
	MUT_OLD=$old MUT_NEW=$new perl -0pi -e 's/\Q$ENV{"MUT_OLD"}\E/$ENV{"MUT_NEW"}/' "$file"

	set +e
	output=$(GOWORK=off GOCACHE=/private/tmp/s3store-gocache go test $tags -run "^${test_name}$" -count=1 . 2>&1)
	status=$?
	set -e
	if test "$status" -eq 0; then
		echo "SURVIVED|$name|$test_name|test passed"
		exit 1
	fi
	if printf '%s\n' "$output" | grep -Eq '\[build failed\]|\[setup failed\]|undefined:|syntax error'; then
		echo "INVALID|$name|$test_name|compile/setup failure"
		printf '%s\n' "$output"
		exit 1
	fi
	if ! printf '%s\n' "$output" | grep -Fq "$want"; then
		echo "WRONG_FAILURE|$name|$test_name|missing: $want"
		printf '%s\n' "$output"
		exit 1
	fi
	echo "KILLED|$name|$test_name|$want"
	killed=$((killed + 1))
}

run_mutation options "missing endpoint" options.go 'if strings.TrimSpace(raw) == "" {' 'if false && strings.TrimSpace(raw) == "" {' TestOptionsResolve 'reason containing "must be set"'
run_mutation options "endpoint absolute host" options.go 'parsed.Host == ""' 'false && parsed.Host == ""' TestOptionsResolve 'resolve error = <nil> <nil>, want *OptionsError'
run_mutation options "endpoint userinfo" options.go 'if parsed.User != nil {' 'if false && parsed.User != nil {' TestOptionsResolve 'endpoint_userinfo'
run_mutation options "endpoint query" options.go 'if parsed.RawQuery != "" || parsed.ForceQuery {' 'if false && (parsed.RawQuery != "" || parsed.ForceQuery) {' TestOptionsResolve 'endpoint_query'
run_mutation options "endpoint fragment" options.go 'if parsed.Fragment != "" || parsed.RawFragment != "" {' 'if false && (parsed.Fragment != "" || parsed.RawFragment != "") {' TestOptionsResolve 'endpoint_fragment'
run_mutation options "endpoint root path" options.go 'if parsed.Path != "" && parsed.Path != "/" {' 'if false && parsed.Path != "" && parsed.Path != "/" {' TestOptionsResolve 'endpoint_path'
# The opt-in must be killed by a LOOPBACK fixture. On a non-loopback fixture the
# next guard still rejects, so the test would pass for the wrong reason and the
# opt-in could be deleted unnoticed.
run_mutation options "HTTPS opt-in required" options.go 'if !allowInsecureLocal {' 'if false && !allowInsecureLocal {' TestOptionsResolve 'loopback_plaintext_without_opt-in'
run_mutation options "HTTPS opt-in not implied by loopback" options.go 'if !allowInsecureLocal {' 'if !allowInsecureLocal && !isLoopbackHost(parsed.Hostname()) {' TestOptionsResolve 'loopback_plaintext_without_opt-in'
run_mutation options "remote HTTP loopback" options.go 'if !isLoopbackHost(parsed.Hostname()) {' 'if false && !isLoopbackHost(parsed.Hostname()) {' TestOptionsResolveRejectsRemotePlaintextDespiteTestOptIn 'want loopback-only *OptionsError'
run_mutation options "endpoint scheme" options.go 'return "", invalidOption("Endpoint", "must use HTTPS")' 'return parsed.Scheme + "://" + parsed.Host, nil' TestOptionsResolve 'unsupported_endpoint_scheme'
run_mutation options "localhost classification" options.go 'if strings.EqualFold(host, "localhost") {' 'if false && strings.EqualFold(host, "localhost") {' TestOptionsResolveAllowsExplicitLocalPlaintext 'resolve:'
run_mutation options "loopback IP classification" options.go 'return ip != nil && ip.IsLoopback()' 'return false && ip != nil && ip.IsLoopback()' TestOptionsResolveAllowsExplicitLocalPlaintext 'resolve:'

run_mutation options "region boundary" options.go 'if !validRegion(o.Region) {' 'if false && !validRegion(o.Region) {' TestOptionsResolve 'missing_region'
run_mutation options "region byte maximum" options.go 'len(region) > 63' 'false && len(region) > 63' TestOptionsResolve 'oversized_region'
run_mutation options "region edge hyphen" options.go "region[0] == '-' || region[len(region)-1] == '-'" "false && (region[0] == '-' || region[len(region)-1] == '-')" TestOptionsResolve 'leading-hyphen_region'
run_mutation options "region alphabet" options.go "if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' {" 'if char == char && false {' TestOptionsResolve 'invalid_region'

run_mutation options "bucket boundary" options.go 'if !validBucket(o.Bucket) {' 'if false && !validBucket(o.Bucket) {' TestOptionsResolve 'missing_bucket'
run_mutation options "bucket minimum" options.go 'len(bucket) < 3' 'false && len(bucket) < 3' TestOptionsResolve 'short_bucket'
run_mutation options "bucket maximum" options.go 'len(bucket) > 63' 'false && len(bucket) > 63' TestOptionsResolve 'oversized_bucket'
run_mutation options "bucket adjacent dots" options.go 'strings.Contains(bucket, "..")' 'false && strings.Contains(bucket, "..")' TestOptionsResolve 'adjacent-dot_bucket'
run_mutation options "bucket IP form" options.go 'if ip := net.ParseIP(bucket); ip != nil {' 'if ip := net.ParseIP(bucket); false && ip != nil {' TestOptionsResolve 'IP_bucket'
run_mutation options "bucket alphabet" options.go "if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '.' {" 'if char == char && false {' TestOptionsResolve 'uppercase_bucket'
run_mutation options "bucket edge byte" options.go "if (i == 0 || i == len(bucket)-1) && (char < 'a' || char > 'z') && (char < '0' || char > '9') {" 'if false {' TestOptionsResolve 'leading-hyphen_bucket'

run_mutation options "deployment prefix required" options.go 'if o.DeploymentPrefix == "" {' 'if false && o.DeploymentPrefix == "" {' TestOptionsResolve 'missing_deployment_prefix'
run_mutation options "deployment prefix maximum" options.go 'if len(o.DeploymentPrefix) > maxDeploymentPrefixBytes {' 'if false && len(o.DeploymentPrefix) > maxDeploymentPrefixBytes {' TestOptionsResolve 'oversized_deployment_prefix'
run_mutation options "deployment prefix canonical" options.go 'if storage.ValidateName(o.DeploymentPrefix) != nil {' 'if false && storage.ValidateName(o.DeploymentPrefix) != nil {' TestOptionsResolve 'escaping_deployment_prefix'
run_mutation options "addressing enum" options.go 'if o.AddressingStyle > AddressingPath {' 'if false && o.AddressingStyle > AddressingPath {' TestOptionsResolve 'unknown_addressing_style'
run_mutation options "encryption enum" options.go 'if o.Encryption > EncryptionKMS {' 'if false && o.Encryption > EncryptionKMS {' TestOptionsResolve 'unknown_encryption'

run_mutation options "KMS mode" options.go 'if mode == EncryptionKMS {' 'if false && mode == EncryptionKMS {' TestOptionsResolve 'KMS_missing_key'
run_mutation options "KMS key required" options.go 'if keyID == "" {' 'if false && keyID == "" {' TestOptionsResolve 'KMS_missing_key'
run_mutation options "KMS key maximum" options.go 'if len(keyID) > maxKMSKeyIDBytes {' 'if false && len(keyID) > maxKMSKeyIDBytes {' TestOptionsResolve 'oversized_KMS_key'
run_mutation options "KMS key controls" options.go 'if !utf8.ValidString(keyID) || strings.IndexFunc(keyID, unicode.IsControl) >= 0 {' 'if false && (!utf8.ValidString(keyID) || strings.IndexFunc(keyID, unicode.IsControl) >= 0) {' TestOptionsResolve 'KMS_key_control_byte'
run_mutation options "KMS key mode confinement" options.go 'if keyID != "" {' 'if false && keyID != "" {' TestOptionsResolve 'KMS_key_on_default_encryption'

run_mutation options "multipart default" options.go 'if requested == 0 {' 'if false && requested == 0 {' TestOptionsResolve 'valid_defaults'
run_mutation options "multipart minimum" options.go 'if requested < minMultipartBytes {' 'if false && requested < minMultipartBytes {' TestOptionsResolve 'multipart_threshold_below_minimum'
run_mutation options "multipart maximum" options.go 'if requested > maxMultipartBytes {' 'if false && requested > maxMultipartBytes {' TestOptionsResolve 'multipart_threshold_above_maximum'
run_mutation options "threshold bounds part" options.go 'if threshold < partSize {' 'if false && threshold < partSize {' TestOptionsResolve 'threshold_below_part_size'
run_mutation options "concurrency nonnegative" options.go 'if concurrency < 0 {' 'if false && concurrency < 0 {' TestOptionsResolve 'negative_concurrency'
run_mutation options "concurrency default" options.go 'if concurrency == 0 {' 'if false && concurrency == 0 {' TestOptionsResolve 'resolved defaults'
run_mutation options "concurrency maximum" options.go 'if concurrency > maxConcurrency {' 'if false && concurrency > maxConcurrency {' TestOptionsResolve 'excess_concurrency'
run_mutation options "concurrent transfers nonnegative" options.go 'if maxTransfers < 0 {' 'if false && maxTransfers < 0 {' TestOptionsResolve 'negative_concurrent_transfers'
run_mutation options "concurrent transfers default" options.go 'if maxTransfers == 0 {' 'if false && maxTransfers == 0 {' TestOptionsResolve 'resolved defaults'
run_mutation options "concurrent transfers maximum" options.go 'if maxTransfers > maxConcurrentTransfers {' 'if false && maxTransfers > maxConcurrentTransfers {' TestOptionsResolve 'excess_concurrent_transfers'
run_mutation options "part buffer multiplication" options.go 'partBuffers := int64(concurrency+1) * partSize' 'partBuffers := int64(1) * partSize' TestOptionsResolve 'parts_exceed_aggregate_memory_budget'
run_mutation options "per-transfer part memory" options.go 'perTransferMemory := threshold + partBuffers' 'perTransferMemory := threshold + 0*partBuffers' TestOptionsResolve 'parts_exceed_aggregate_memory_budget'
run_mutation options "aggregate transfer multiplication" options.go 'perTransferMemory*int64(maxTransfers)' 'perTransferMemory*int64(maxTransfers-maxTransfers+1)' TestOptionsResolve 'threshold_exceeds_aggregate_memory_budget'
run_mutation options "aggregate memory ceiling" options.go 'if perTransferMemory*int64(maxTransfers) > maxAggregateTransferMemory {' 'if false && perTransferMemory*int64(maxTransfers) > maxAggregateTransferMemory {' TestOptionsResolve 'threshold_exceeds_aggregate_memory_budget'
run_mutation memory "additive transfer memory" options.go 'perTransferMemory := threshold + partBuffers' 'perTransferMemory := max(threshold, partBuffers)' TestOptionsResolve 'additive_aggregate_memory_budget'

run_mutation open "Open option short circuit" s3store.go 'if err != nil {' 'if false && err != nil {' TestOpenRejectsOptionsBeforeSDKConstruction 'want nil, error'
run_mutation open "Open deadline" s3store.go 'guard.RequireDeadline(ctx, "Open")' 'guard.NotImplemented("Open")' TestOpenRequiresDeadline 'want *DeadlineRequiredError'
run_mutation open "SDK loader failure" s3store.go 'awsConfig, err := loadConfig(ctx, resolved)
	if err != nil {' 'awsConfig, err := loadConfig(ctx, resolved)
	if false && err != nil {' TestOpenRedactsEverySDKLoaderFailure 'Open returned Store with SDK loader error'
run_mutation open "SDK failure redaction" s3store.go 'invalidOption("Credentials", "AWS configuration could not be loaded securely")' 'invalidOption("Credentials", "AWS configuration could not be loaded securely: "+err.Error())' TestOpenRedactsEverySDKLoaderFailure 'want non-unwrapping redacted error'
run_mutation open "endpoint wiring" s3store.go 'options.BaseEndpoint = aws.String(resolved.endpoint)' 'options.BaseEndpoint = aws.String("https://wrong.example.test")' TestOpenWiresBlobScaffoldWithoutNetworkIO 'S3 client BaseEndpoint'
run_mutation open "path addressing wiring" s3store.go 'options.UsePathStyle = resolved.addressingStyle == AddressingPath' 'options.UsePathStyle = false' TestOpenWiresBlobScaffoldWithoutNetworkIO 'S3 client UsePathStyle = false'
run_mutation open "transfer threshold bound" s3store.go 'options.MultipartUploadThreshold = resolved.multipartThreshold' 'options.MultipartUploadThreshold = resolved.multipartThreshold + 1' TestOpenWiresBlobScaffoldWithoutNetworkIO 'transfer threshold ='
run_mutation open "transfer part bound" s3store.go 'options.PartSizeBytes = resolved.multipartPartSize' 'options.PartSizeBytes = resolved.multipartPartSize + 1' TestOpenWiresBlobScaffoldWithoutNetworkIO 'transfer part size ='
run_mutation open "transfer concurrency bound" s3store.go 'options.Concurrency = resolved.concurrency' 'options.Concurrency = resolved.concurrency + 1' TestOpenWiresBlobScaffoldWithoutNetworkIO 'transfer concurrency ='
run_mutation open "Get buffer bound" s3store.go 'options.GetObjectBufferSize = int64(resolved.concurrency) * resolved.multipartPartSize' 'options.GetObjectBufferSize = int64(resolved.concurrency+1) * resolved.multipartPartSize' TestOpenWiresBlobScaffoldWithoutNetworkIO 'transfer Get buffer ='
run_mutation open "Store-wide transfer gate" s3store.go 'make(chan struct{}, resolved.maxConcurrentTransfers)' 'make(chan struct{}, resolved.maxConcurrentTransfers+1)' TestOpenWiresBlobScaffoldWithoutNetworkIO 'Store transfer slots ='
run_mutation open "injected credential provider" s3store.go 'if options.credentials != nil {' 'if false && options.credentials != nil {' TestDefaultLoadConfigUsesInjectedCredentials 'Retrieve:'
run_mutation open "standard credential chain" s3store.go 'config.WithRegion(options.region),' 'config.WithRegion(options.region), config.WithCredentialsProvider(aws.AnonymousCredentials{}),' TestDefaultLoadConfigUsesStandardCredentialChain 'standard credential chain Retrieve:'
run_mutation open "request checksum policy" s3store.go 'config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),' 'config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenSupported),' TestDefaultLoadConfigPinsChecksumAndRetryPolicy 'request checksum calculation ='
run_mutation open "response checksum policy" s3store.go 'config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),' 'config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenSupported),' TestDefaultLoadConfigPinsChecksumAndRetryPolicy 'response checksum validation ='
run_mutation open "bounded standard retry attempts" s3store.go 'config.WithRetryMaxAttempts(3),' 'config.WithRetryMaxAttempts(4),' TestDefaultLoadConfigPinsChecksumAndRetryPolicy 'standard retry max attempts = 4, want 3'

run_mutation security "endpoint userinfo redaction" options.go 'invalidOption("Endpoint", "must not contain userinfo")' 'invalidOption("Endpoint", "must not contain userinfo: "+raw)' TestOptionsErrorNeverUnwrapsOrRetainsSensitiveValue 'want non-unwrapping redacted error'
run_mutation security "presigned query redaction" options.go 'invalidOption("Endpoint", "must not contain a query")' 'invalidOption("Endpoint", "must not contain a query: "+raw)' TestOptionsResolve 'error disclosed credential material "super-secret"'
run_mutation security "KMS identifier redaction" options.go 'invalidOption("KMSKeyID", "may be set only when EncryptionKMS is selected")' 'invalidOption("KMSKeyID", "may be set only when EncryptionKMS is selected: "+keyID)' TestOptionsResolve 'error disclosed credential material "secret-key"'

run_mutation blob "nil context" internal/guard/guard.go 'if ctx == nil {' 'if false && ctx == nil {' TestBlobOperationsRejectNilContext 'panic:'
run_mutation blob "context deadline" internal/guard/guard.go 'if _, ok := ctx.Deadline(); !ok {' 'if _, ok := ctx.Deadline(); ok && false {' TestOpenRequiresDeadline 'Open returned a Store without a caller deadline'
run_mutation blob "blob key validation" key.go 'return storage.ValidateName(key)' 'return nil' TestBlobOperationsValidateKeysBeforeStubResult 'InvalidNameError.Name ='
run_mutation blob "empty list prefix" key.go 'if prefix == "" {' 'if false && prefix == "" {' TestListPrefixValidation 'validateListPrefix("")'
run_mutation blob "list trailing slash" key.go 'strings.TrimSuffix(prefix, "/")' 'strings.TrimSuffix(prefix, "\\x00")' TestListPrefixValidation 'validateListPrefix("blobs/")'

for operation in Put Get Delete List; do
	run_mutation blob "$operation deadline call" blob.go "guard.RequireDeadline(ctx, \"Blobs.$operation\")" "guard.NotImplemented(\"Blobs.$operation\")" TestBlobOperationMethodsCallDeadlineGuard 'does not call guard.RequireDeadline'
done

run_mutation deps "replace directive" go.mod 'go 1.26.6' 'go 1.26.6

replace example.test/absent-module v1.0.0 => example.test/absent-module v1.0.1' TestDependencyBoundary 'replace directives, want none'
run_mutation deps "extra direct module" go.mod 'github.com/aws/smithy-go v1.28.1 // indirect' 'github.com/aws/smithy-go v1.28.1' TestDependencyBoundary 'direct modules ='
run_mutation deps "logging import" s3store.go '"context"' '"context"
	_ "log/slog"' TestDependencyBoundary 'imports logging package "log/slog"'

run_mutation options "encryption posture must be explicit" options.go 'if o.Encryption == EncryptionUnspecified {' 'if false && o.Encryption == EncryptionUnspecified {' TestOptionsResolve 'unset_encryption'
run_mutation memory "upload part pin within SDK limit" options.go 'maxUploadParts int64 = 10000' 'maxUploadParts int64 = 20000' TestMaxUploadPartsIsWithinTheSDKHardLimit 'want a value in (0, 10000]'
run_mutation memory "upload part pin tracks SDK default" options.go 'maxUploadParts int64 = 10000' 'maxUploadParts int64 = 5000' TestMaxUploadPartsPinMatchesTheSDKDefault 'the pin is no longer a no-op'
run_mutation memory "accounted object size boundary" options.go 'return partSize*maxUploadParts - 1' 'return partSize * maxUploadParts' TestAccountedObjectSizeMarksThePartInflationBoundary 'already inflates'
run_mutation memory "accounted object size retained" options.go 'maxAccountedObjectSize: accountedObjectSize(partSize),' 'maxAccountedObjectSize: accountedObjectSize(partSize) + 1,' TestAccountedObjectSizeMarksThePartInflationBoundary 'retained accounted object size ='

run_mutation open "no network I/O during Open" s3store.go 'return newStore(client, transfers, resolved), nil' '_, _ = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(resolved.bucket)})
	return newStore(client, transfers, resolved), nil' TestOpenWiresBlobScaffoldWithoutNetworkIO 'Open performed network I/O'
run_mutation open "upload part ceiling wiring" s3store.go 'options.MaxUploadParts = maxUploadParts' 'options.MaxUploadParts = maxUploadParts - 1' TestOpenWiresBlobScaffoldWithoutNetworkIO 'transfer MaxUploadParts ='
run_mutation open "Store constructor invariant (literal)" s3store.go 'return newStore(client, transfers, resolved), nil' 'return &Store{client: client, transfers: transfers, options: resolved, transferSlots: make(chan struct{}, resolved.maxConcurrentTransfers)}, nil' TestStoreIsBuiltOnlyByItsConstructor 'via a composite literal'
run_mutation open "Store constructor invariant (new)" s3store.go 'func defaultLoadConfig' 'func unconstructedNew() *Store { return new(Store) }

func defaultLoadConfig' TestStoreIsBuiltOnlyByItsConstructor 'via new(Store)'
run_mutation open "Store constructor invariant (var)" s3store.go 'func defaultLoadConfig' 'func unconstructedVar() *Store { var s Store; return &s }

func defaultLoadConfig' TestStoreIsBuiltOnlyByItsConstructor 'via a var declaration'

run_mutation gate "transfer gate deadline" transfer.go 'if err := guard.RequireDeadline(ctx, operation); err != nil {' 'if err := error(nil); err != nil {' TestAcquireTransferRequiresDeadline 'want *DeadlineRequiredError'
run_mutation gate "transfer gate nil-channel hang" transfer.go 'if s.transferSlots == nil {' 'if false && s.transferSlots == nil {' TestAcquireTransferFailsClosedOnUnconstructedStore 'blocked on a nil transferSlots channel'
run_mutation gate "transfer gate cancellation" transfer.go 'case <-ctx.Done():' 'case <-make(chan struct{}):' TestAcquireTransferBoundsConcurrentTransfers 'ignored ctx.Done and hung'
run_mutation gate "transfer gate release" transfer.go 'func (s *Store) releaseTransfer() {
	<-s.transferSlots
}' 'func (s *Store) releaseTransfer() {
}' TestAcquireTransferBoundsConcurrentTransfers 'acquireTransfer after release'

# Ledger IS reachable by a compiling mutation: its Delete signature is
# byte-identical to Blobs.Delete, so only Append/Read/Tip need adding. KV and
# OrderedIndex are not reachable -- both redeclare Get -- so three of the five
# exclusions are mutation-proved and two are compile-time impossibilities.
run_mutation interfaces "Ledger exclusion" blob.go '// Put stages' 'func (s *Store) Append(context.Context, string, uint64, []byte) error { return nil }
func (s *Store) Read(context.Context, string, uint64) (storage.Cursor, error) { return nil, nil }
func (s *Store) Tip(context.Context, string) (uint64, error) { return 0, nil }

// Put stages' TestStoreImplementsBlobsAndTheReaderLifecycleOnly 'Store implements storage.Ledger'
run_mutation interfaces "Leaser exclusion" blob.go '// Put stages' 'func (s *Store) Acquire(context.Context, string) (storage.Lease, error) { return nil, nil }

// Put stages' TestStoreImplementsBlobsAndTheReaderLifecycleOnly 'Store implements storage.Leaser'

run_mutation security "invalid-name key redaction" redact.go 'return "s3store: invalid storage name: " + invalidName.Rule' 'return err.Error()' TestRedactedErrorTextDropsTenantScopedIdentifiers 'recorded text disclosed'
run_mutation security "classification survives wrapping" redact.go 'var invalidName *storage.InvalidNameError
	if errors.As(err, &invalidName) {' 'if invalidName, ok := err.(*storage.InvalidNameError); ok {' TestRedactedErrorTextDropsTenantScopedIdentifiers 'want the unwrapped classification'
run_mutation security "unclassified error fails closed" redact.go 'return redactedText
}' 'return err.Error()
}' TestRedactedErrorTextFailsClosedOnUnknownErrors 'unclassified error text ='
run_mutation security "backend error classification" redact.go 'if errors.As(err, &backend) {' 'if false && errors.As(err, &backend) {' TestRedactedErrorTextClassifiesPackageErrors 'RedactedErrorText(*s3store.BackendError) ='
run_mutation security "integrity error classification" redact.go 'if errors.As(err, &integrity) {' 'if false && errors.As(err, &integrity) {' TestRedactedErrorTextClassifiesPackageErrors 'RedactedErrorText(*s3store.BlobIntegrityError) ='
run_mutation security "object-size error classification" redact.go 'if errors.As(err, &tooLarge) {' 'if false && errors.As(err, &tooLarge) {' TestRedactedErrorTextClassifiesPackageErrors 'RedactedErrorText(*s3store.ObjectTooLargeError) ='

run_mutation deps "standard stream write" s3store.go '	"github.com/looprig/s3store/internal/guard"
)' '	"github.com/looprig/s3store/internal/guard"

	"fmt"
	"os"
)

func stderrLeak(v any) { fmt.Fprintln(os.Stderr, v) }' TestDependencyBoundary 'writes to os.Stderr'
run_mutation deps "builtin println" s3store.go 'func defaultLoadConfig' 'func printlnLeak(v string) { println(v) }

func defaultLoadConfig' TestDependencyBoundary 'calls the builtin println'

# P2.2 key derivation and per-row decoding.
run_mutation keys "manifest deployment prefix validation" key.go 'if err := storage.ValidateName(deploymentPrefix); err != nil {
		return "", err
	}' 'if err := storage.ValidateName(deploymentPrefix); false && err != nil {
		return "", err
	}' TestBackendManifestKeyRejectsPrefixAndObjectInjection 'want *storage.InvalidNameError'
run_mutation keys "manifest logical key validation" key.go 'if err := storage.ValidateName(logicalKey); err != nil {
		return "", err
	}' 'if err := storage.ValidateName(logicalKey); false && err != nil {
		return "", err
	}' TestBackendManifestKeyRejectsPrefixAndObjectInjection 'want *storage.InvalidNameError'
run_mutation keys "manifest logical hash" key.go 'digest := sha256.Sum256([]byte(logicalKey))' 'digest := sha256.Sum256([]byte("wrong"))' TestBackendManifestKeyIsCanonicalAndReversible 'want versioned prefix and hash/encoding suffix'
run_mutation keys "S3 object-key maximum" key.go 'if len(objectKey) > maxS3ObjectKeyBytes {' 'if false && len(objectKey) > maxS3ObjectKeyBytes {' TestLogicalObjectKeyMaximumOnBothSides 'want *OptionsError'
run_mutation keys "listed row hash binding" key.go 'if !equalDigest(digest, want[:]) {' 'if false && !equalDigest(digest, want[:]) {' TestLogicalKeyFromManifestObjectFailsClosedPerRow 'decoded rows ='

# P2.2 strict manifest framing.
run_mutation manifest "manifest negative size encode" manifest.go 'if manifest.Size < 0 {' 'if false && manifest.Size < 0 {' TestManifestEncodeRejectsInvalidFields 'encodeManifest returned nil error'
run_mutation manifest "manifest maximum read bound" manifest.go 'contentLength > int64(maxManifestBytes)' 'false && contentLength > int64(maxManifestBytes)' TestManifestDecodeRejectsAmbiguousOrCorruptObjects 'want *BlobIntegrityError'
run_mutation manifest "manifest trailing byte" manifest.go 'if n, _ := reader.Read(trailing[:]); n != 0 {' 'if n, _ := reader.Read(trailing[:]); false && n != 0 {' TestManifestDecodeRejectsAmbiguousOrCorruptObjects 'want *BlobIntegrityError'
run_mutation manifest "manifest magic" manifest.go 'if string(encoded[:8]) != manifestMagic {' 'if false && string(encoded[:8]) != manifestMagic {' TestManifestDecodeRejectsAmbiguousOrCorruptObjects 'want *BlobIntegrityError'
run_mutation manifest "manifest exact framing length" manifest.go 'if manifestHeaderBytes+logicalLength+payloadLength != len(encoded) {' 'if false && manifestHeaderBytes+logicalLength+payloadLength != len(encoded) {' TestManifestDecodeRejectsAmbiguousOrCorruptObjects 'panic:'
run_mutation manifest "manifest decoded size sign" manifest.go 'if rawSize > math.MaxInt64 {' 'if false && rawSize > math.MaxInt64 {' TestManifestDecodeRejectsAmbiguousOrCorruptObjects 'want *BlobIntegrityError'

# P2.2 streaming accounting and terminal read verification.
run_mutation stream "upload maximum fence" blob_io.go 'return &accountedHashReader{source: source, maximum: maximum, hash: sha256.New()}' 'return &accountedHashReader{source: source, maximum: maximum + 1, hash: sha256.New()}' TestAccountedHashReaderDrivesMaxObjectSizeOnBothSides 'read bytes ='
run_mutation stream "upload excess-byte probe" blob_io.go 'if n > 0 {
			return 0, &ObjectTooLargeError{Maximum: r.maximum}
		}' 'if false && n > 0 {
			return 0, &ObjectTooLargeError{Maximum: r.maximum}
		}' TestAccountedHashReaderDrivesMaxObjectSizeOnBothSides 'ObjectTooLarge=false want true'
run_mutation stream "upload streamed digest" blob_io.go 'r.hash.Write(buffer[:n])' 'r.hash.Write(buffer[:0])' TestAccountedHashReaderDrivesMaxObjectSizeOnBothSides 'digest ='
run_mutation stream "download excess-byte probe" blob_io.go 'if n > 0 {
			r.terminal = integrityError("payload read")' 'if false && n > 0 {
			r.terminal = integrityError("payload read")' TestVerifyingBlobReaderRequiresExactLengthAndDigest 'integrity=false want true'
run_mutation stream "download exact length" blob_io.go 'if r.read != r.expectedSize {' 'if false && r.read != r.expectedSize {' TestVerifyingBlobReaderRequiresExactLengthAndDigest 'integrity=false want true'
run_mutation stream "download digest" blob_io.go 'if !equalDigest(actual[:], r.expectedDigest[:]) {' 'if false && !equalDigest(actual[:], r.expectedDigest[:]) {' TestVerifyingBlobReaderRequiresExactLengthAndDigest 'integrity=false want true'

# P2.2 live atomic protocol. These mutations compile the integration suite and
# are run against its disposable loopback service.
run_mutation protocol "Put resolved maximum" blob.go 'newAccountedHashReader(source, s.options.maxAccountedObjectSize)' 'newAccountedHashReader(source, s.options.maxAccountedObjectSize + 1)' TestPutRejectsResolvedMaxAccountedObjectSizeOnBothSides 'want *ObjectTooLargeError' -tags=integration
run_mutation protocol "committed payload verification" blob.go 'if err := s.verifyPayload(ctx, payloadKey, size, digest); err != nil {' 'if err := error(nil); err != nil {' TestPutVerifiesCommittedLengthAndDigestBeforePublishing 'want *BlobIntegrityError' -tags=integration
run_mutation protocol "atomic manifest create" blob.go 'ContentLength: aws.Int64(int64(len(encoded))), IfNoneMatch: aws.String("*"),' 'ContentLength: aws.Int64(int64(len(encoded))), IfNoneMatch: nil,' TestConcurrentPutIsAtomicAndImmutable 'concurrent outcomes =' -tags=integration
run_mutation protocol "payload committed length" blob.go 'if head.ContentLength == nil || *head.ContentLength != size {' 'if false && (head.ContentLength == nil || *head.ContentLength != size) {' TestPutVerifiesCommittedLengthAndDigestBeforePublishing 'want *BlobIntegrityError' -tags=integration
run_mutation protocol "payload committed digest" blob.go 'if !equalDigest(actual[:], digest[:]) {' 'if false && !equalDigest(actual[:], digest[:]) {' TestPutVerifiesCommittedLengthAndDigestBeforePublishing 'want *BlobIntegrityError' -tags=integration
run_mutation protocol "bounded verification range" blob.go 'end := min(offset+verificationRangeBytes-1, size-1)' 'end := min(offset+verificationRangeBytes, size-1)' TestPayloadVerificationRangesAreBoundedAtEnds 'Put: s3store: blob integrity verification failed during payload verification digest' -tags=integration
run_mutation protocol "immutable range ETag" blob.go 'Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)), IfMatch: head.ETag,' 'Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)), IfMatch: nil,' TestPayloadVerificationRangesAreBoundedAtEnds 'want the same non-empty HEAD ETag' -tags=integration
run_mutation protocol "list every page" blob.go 'if !aws.ToBool(output.IsTruncated) {' 'if true || !aws.ToBool(output.IsTruncated) {' TestListPagesPastMalformedRow 'List =' -tags=integration
run_mutation protocol "Get holds transfer slot" blob.go 'release = false' 'release = true' TestOpenGetReaderHoldsTransferSlotUntilClose 'want context.DeadlineExceeded' -tags=integration
run_mutation protocol "conflict digest comparison" blob.go 'left.Size == right.Size && equalDigest(left.Digest[:], right.Digest[:])' 'left.Size == right.Size' TestBlobsIntegrationConformance 'want *BlobConflictError' -tags=integration
run_mutation protocol "unowned payload cleanup exclusion" blob.go 'if cleanupSafe && ctx.Err() == nil {' 'if !cleanupSafe && ctx.Err() == nil {' TestPutNeverDeletesPayloadItDidNotCreate 'want 0 for a payload this writer never created' -tags=integration
run_mutation protocol "ambiguous manifest payload preservation" blob.go 'cleanupSafe = false
	created, publishErr := s.publishManifest' 'cleanupSafe = true
	created, publishErr := s.publishManifest' TestPutPreservesPayloadWhenManifestPublicationIsAmbiguous 'blob integrity verification failed during payload lookup' -tags=integration
run_mutation protocol "cleanup caller deadline" blob.go 'if cleanupSafe && ctx.Err() == nil {
			s.deletePayloadBestEffort(ctx, payloadKey)
		}' 'if cleanupSafe {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5_000_000_000)
			defer cancel()
			s.deletePayloadBestEffort(cleanupCtx, payloadKey)
		}' TestPutVerificationCancellationDoesNotStartDetachedCleanup 'detached cleanup outlived the caller deadline' -tags=integration

# P2.3 step 1: tenant isolation. Two mutants, two tenancy arrangements, each
# killing exactly one test. M1 drops the tenant from the ONE derivation every
# key path shares, so it is self-consistent across write, read, list, and
# reverse -- a single-deployment suite cannot tell it from the original -- and
# it is killed by the two-deployment collision. M2 drops the logical prefix
# filter, which the two-deployment test cannot see because it lists with the
# empty prefix. Each is the other's control one position over.
run_mutation tenancy "deployment prefix dropped from every key path" key.go 'func namespaceRoot(deploymentPrefix, namespace string) string {
	return deploymentPrefix + "/" + namespace
}' 'func namespaceRoot(deploymentPrefix, namespace string) string {
	_ = deploymentPrefix
	return namespace
}' TestTenantDeploymentsShareOneBucketWithoutCrossing 'conflicted with the alpha tenant' -tags=integration
# A third arrangement the first two cannot see: payloads staged outside the
# deployment root. Both tenants still behave correctly on write, read, list and
# delete -- payload keys carry a random suffix, so they never collide -- and the
# whole pre-existing suite passes. Only the backend disjointness row catches it,
# which is why that row is in the test at all.
run_mutation tenancy "payloads rooted in the deployment prefix" blob.go '	return namespaceRoot(deploymentPrefix, payloadNamespace) + hex.EncodeToString(digest[:]) + "/"' '	return payloadNamespace + hex.EncodeToString(digest[:]) + "/"' TestTenantDeploymentsShareOneBucketWithoutCrossing 'belongs to 0 tenant roots' -tags=integration
run_mutation tenancy "list logical prefix filter" blob.go 'if !ok || !strings.HasPrefix(logical, prefix) {' 'if !ok {' TestTenantsInOneDeploymentPrefixDoNotCross 'want exactly [tenants/alpha/sessions/s-1701/blobs/o-42]' -tags=integration

# P2.3 step 2: the startup refusal, in both directions, plus the mode
# vocabulary the refusal names and the recording classification.
run_mutation encryption "confirmation requirement enforced" encryption.go 'if !required || mode.confirmedByThisModule() {' 'if true {' TestOpenRefusesUnconfirmableEncryptionWhenPolicyRequiresIt 'Open returned a Store for a posture the deployment policy forbids'
run_mutation encryption "confirmation requirement is conditional" encryption.go 'if !required || mode.confirmedByThisModule() {' 'if mode.confirmedByThisModule() {' TestOpenRefusesUnconfirmableEncryptionWhenPolicyRequiresIt 'want a Store'
run_mutation encryption "bucket default is not self-confirmable" encryption.go 'return m == EncryptionAES256 || m == EncryptionKMS' 'return m != EncryptionUnspecified' TestOpenRefusesUnconfirmableEncryptionWhenPolicyRequiresIt 'Open returned a Store for a posture the deployment policy forbids'
# The refusal must be reachable by errors.As from the exported constructor, not
# merely non-nil. This mutant refuses correctly but with the package's other
# option error, which only the typed row can see.
run_mutation encryption "refusal is typed" encryption.go '	return &EncryptionPolicyError{
		Mode: mode,' '	if true {
		return invalidOption("Encryption", "requires confirmation")
	}
	return &EncryptionPolicyError{
		Mode: mode,' TestOpenRefusesUnconfirmableEncryptionWhenPolicyRequiresIt 'want *EncryptionPolicyError'
run_mutation encryption "posture vocabulary" encryption.go '	case EncryptionBucketDefault:
		return "bucket-default"' '	case EncryptionBucketDefault:
		return "default"' TestEncryptionModeNamesEveryDeclaredPosture 'want "bucket-default"'
run_mutation encryption "policy error recording classification" redact.go 'var encryptionPolicy *EncryptionPolicyError
	if errors.As(err, &encryptionPolicy) {' 'var encryptionPolicy *EncryptionPolicyError
	if false && errors.As(err, &encryptionPolicy) {' TestEncryptionPolicyErrorIsRecordableWithoutIdentifiers 'want the typed classification'

# P2.3 step 2: the header evidence behind confirmedByThisModule, on both the
# single-part and the multipart write path.
run_mutation encryption "single-part encryption header" blob.go '		input.ServerSideEncryption = types.ServerSideEncryptionAes256' '		_ = types.ServerSideEncryptionAes256' TestEveryObjectCreatingRequestCarriesTheConfiguredEncryptionHeader 'X-Amz-Server-Side-Encryption = ""' -tags=integration
run_mutation encryption "multipart encryption header" blob.go '		input.ServerSideEncryption = tmtypes.ServerSideEncryptionAes256' '		_ = tmtypes.ServerSideEncryptionAes256' TestEveryObjectCreatingRequestCarriesTheConfiguredEncryptionHeader 'X-Amz-Server-Side-Encryption = ""' -tags=integration
run_mutation encryption "KMS key identifier header" blob.go '		input.SSEKMSKeyId = aws.String(s.options.kmsKeyID)' '		input.SSEKMSKeyId = nil' TestEveryObjectCreatingRequestCarriesTheConfiguredEncryptionHeader 'Aws-Kms-Key-Id = ""' -tags=integration
# The multipart path has its own KMS field, spelled SSEKMSKeyID rather than
# SSEKMSKeyId. It had no recorded probe; this is it.
run_mutation encryption "multipart KMS key identifier header" blob.go '		input.SSEKMSKeyID = aws.String(s.options.kmsKeyID)' '		input.SSEKMSKeyID = nil' TestEveryObjectCreatingRequestCarriesTheConfiguredEncryptionHeader 'Aws-Kms-Key-Id = ""' -tags=integration

# P2.3 step 3: the orphan precondition. A failed transfer owns no committed
# payload, so it may delete nothing; and no operation may enumerate uploads.
# The guard is now one condition, so this mutant is the whole of it: cleanup
# firing when nothing was committed.
run_mutation multipart "cleanup requires something committed" blob.go 'if cleanupSafe && ctx.Err() == nil {' 'if cleanupSafe || ctx.Err() == nil {' TestFailedMultipartUploadIsAbortedAndLeavesNoObject 'payload deletes = ' -tags=integration
run_mutation multipart "no upload sweep" blob.go '	accounted := newAccountedHashReader(source, s.options.maxAccountedObjectSize)' '	if listed, listErr := s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(s.options.bucket)}); listErr == nil {
		for _, stale := range listed.Uploads {
			_, _ = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(s.options.bucket), Key: stale.Key, UploadId: stale.UploadId})
		}
	}
	accounted := newAccountedHashReader(source, s.options.maxAccountedObjectSize)' TestNoOrphanCollectionRequestIsIssued 'ListMultipartUploads requests = ' -tags=integration
# The abort itself is the AWS transfer manager's, not this module's, so no
# in-repo mutation of the subject can suppress it. This fixture-side mutation
# proves only that the assertion detects an upload left in progress; it does
# NOT prove that s3store causes the abort. That limitation is reported.
run_mutation multipart "abort assertion detects a surviving upload" internal/testserver/server.go '	s.abortedUploads = append(s.abortedUploads, uploadID)
	delete(s.uploads, uploadID)' '	s.abortedUploads = append(s.abortedUploads, uploadID)' TestFailedMultipartUploadIsAbortedAndLeavesNoObject 'active multipart uploads = ' -tags=integration

# P2.3 follow-up: the bounded reader lifecycle. Each rule is probed at the layer
# that can SEE it. Over a real HTTP body every one of these mutations was
# measured and SURVIVED the whole integration suite: aborting the request and
# closing the body each unblock a stalled Read alone, net/http's body Close is
# already idempotent, and the two closed checks are interchangeable once Close
# has returned. The integration probe below therefore holds only the property
# that is genuinely emergent -- that a stalled Read is released at all -- and
# the parts are held by synthetic fixtures in blob_reader_test.go.
run_mutation lifecycle "Close aborts the request" blob_reader.go '		r.abort()
		r.closeErr = r.verifier.Close()' '		r.closeErr = r.verifier.Close()' TestCloseInvokesBothReleaseMechanismsExactlyOnce 'request aborts = 0, want exactly 1'
run_mutation lifecycle "Close closes the body" blob_reader.go '		r.closeErr = r.verifier.Close()' '		r.closeErr = nil' TestCloseInvokesBothReleaseMechanismsExactlyOnce 'body closes = 0, want exactly 1'
run_mutation lifecycle "Close neither aborts nor closes" blob_reader.go '		r.abort()
		r.closeErr = r.verifier.Close()' '		r.closeErr = nil' TestCloseUnblocksAStalledPayloadRead 'did not return within the advertised' -tags=integration
run_mutation lifecycle "Close is latched" blob_reader.go '	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.abort()
		r.closeErr = r.verifier.Close()
		r.releaseOnce.Do(r.release)
	})
	return r.closeErr' '	r.closed.Store(true)
	r.abort()
	err := r.verifier.Close()
	r.releaseOnce.Do(r.release)
	return err' TestCloseClassificationIsLatchedAcrossAnUnstableBody 'want the latched'
run_mutation lifecycle "closed check precedes the verifier" blob_reader.go '	if r.closed.Load() {
		return 0, errBlobReaderClosed
	}
	n, err := r.verifier.Read(buffer)' '	n, err := r.verifier.Read(buffer)' TestReadAfterCloseDoesNotConsultAnUnresponsiveBody 'it reached a body that never answers'
run_mutation lifecycle "closed check follows the verifier" blob_reader.go '	n, err := r.verifier.Read(buffer)
	if r.closed.Load() {' '	n, err := r.verifier.Read(buffer)
	if false && r.closed.Load() {' TestReadInFlightWhenCloseBeginsIsTerminal 'bytes after Close, want 0'
# "Close shares no lock with Read" is the rule sessionstore's
# completeTermination depends on most directly, and it is the one that cannot be
# probed with a single-line edit: memstore's design needs a lock in BOTH
# methods, so the hunk spans them. It is worth the verbosity, because inline the
# mutant deadlocks and surfaces only as the go test timeout -- the harness would
# report a hang, and a kill by hang is not an assertion kill. The test now
# receives Close under a bound, so this dies on an assertion.
run_mutation lifecycle "Read and Close share no lock" blob_reader.go 'func (r *blobReader) Read(buffer []byte) (int, error) {
	if r.closed.Load() {
		return 0, errBlobReaderClosed
	}
	n, err := r.verifier.Read(buffer)
	if r.closed.Load() {
		// Close began while this Read was in flight. Whatever the aborted body
		// produced -- bytes, nil, io.EOF, or a transport error -- is not a
		// terminal result for this stream, and the bytes are discarded.
		return 0, errBlobReaderClosed
	}
	if err != nil {
		r.releaseOnce.Do(r.release)
	}
	return n, err
}

// Close is idempotent with a stable classification: the first call computes the
// result under a sync.Once and every later call returns that same value rather
// than closing the body a second time.
func (r *blobReader) Close() error {
	r.closeOnce.Do(func() {' 'var mutantReadCloseSerialization sync.Mutex

func (r *blobReader) Read(buffer []byte) (int, error) {
	mutantReadCloseSerialization.Lock()
	defer mutantReadCloseSerialization.Unlock()
	if r.closed.Load() {
		return 0, errBlobReaderClosed
	}
	n, err := r.verifier.Read(buffer)
	if r.closed.Load() {
		// Close began while this Read was in flight. Whatever the aborted body
		// produced -- bytes, nil, io.EOF, or a transport error -- is not a
		// terminal result for this stream, and the bytes are discarded.
		return 0, errBlobReaderClosed
	}
	if err != nil {
		r.releaseOnce.Do(r.release)
	}
	return n, err
}

// Close is idempotent with a stable classification: the first call computes the
// result under a sync.Once and every later call returns that same value rather
// than closing the body a second time.
func (r *blobReader) Close() error {
	mutantReadCloseSerialization.Lock()
	defer mutantReadCloseSerialization.Unlock()
	r.closeOnce.Do(func() {' TestReadInFlightWhenCloseBeginsIsTerminal 'Close must share no lock with Read'

# There is deliberately no mutation for the ORDER of the closed store against
# the teardown inside Close. It was probed (deferring the store to the end of
# Close) and SURVIVED, and it should have: the contract constrains Reads after
# Close RETURNS, and the flag is set before Close returns either way. A test for
# it would have to race the teardown against the blocked Read.
run_mutation lifecycle "positive close bound" blob_reader.go 'const blobReaderCloseBound = 5 * time.Second' 'const blobReaderCloseBound = 0' TestBlobReaderLifecycleConformance 'want positive duration' -tags=integration
run_mutation lifecycle "closed error is not EOF" blob_reader.go 'var errBlobReaderClosed error = &BlobReaderClosedError{}' 'var errBlobReaderClosed error = io.EOF' TestReadAfterCloseNeverReportsEOFOnADrainedStream 'want 0 and a non-EOF fs.ErrClosed error'
run_mutation lifecycle "transfer slot released once" blob_reader.go '	r.releaseOnce.Do(r.release)
	})' '	r.release()
	})' TestReadAfterCloseNeverReportsEOFOnADrainedStream 'want exactly 1 across terminal read and Close'
run_mutation lifecycle "closed error recording classification" redact.go '	var readerClosed *BlobReaderClosedError
	if errors.As(err, &readerClosed) {' '	var readerClosed *BlobReaderClosedError
	if false && errors.As(err, &readerClosed) {' TestBlobReaderClosedErrorIsRecordable 'want the typed classification'

restore_snapshot
rm -rf "$snapshot_dir"
trap - EXIT HUP INT TERM
echo "MEASURED|$killed|killed mutations"
