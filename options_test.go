package s3store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type testCredentialsProvider struct{}

func (testCredentialsProvider) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: "not-retrieved-by-scaffold"}, nil
}

func TestOptionsResolve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(*Options)
		wantField  string
		wantReason string
	}{
		{name: "valid defaults"},
		{name: "missing endpoint", mutate: func(o *Options) { o.Endpoint = "" }, wantField: "Endpoint", wantReason: "must be set"},
		{name: "malformed endpoint", mutate: func(o *Options) { o.Endpoint = "://bad" }, wantField: "Endpoint", wantReason: "valid URL"},
		{name: "endpoint missing host", mutate: func(o *Options) { o.Endpoint = "https:/" }, wantField: "Endpoint", wantReason: "valid URL"},
		{name: "endpoint userinfo", mutate: func(o *Options) { o.Endpoint = "https://access:super-secret@s3.example.test" }, wantField: "Endpoint", wantReason: "userinfo"},
		{name: "endpoint query", mutate: func(o *Options) { o.Endpoint = "https://s3.example.test/?X-Amz-Signature=super-secret" }, wantField: "Endpoint", wantReason: "query"},
		{name: "endpoint fragment", mutate: func(o *Options) { o.Endpoint = "https://s3.example.test/#super-secret" }, wantField: "Endpoint", wantReason: "fragment"},
		{name: "endpoint path", mutate: func(o *Options) { o.Endpoint = "https://s3.example.test/base" }, wantField: "Endpoint", wantReason: "root path"},
		{name: "remote plaintext", mutate: func(o *Options) { o.Endpoint = "http://s3.example.test" }, wantField: "Endpoint", wantReason: "HTTPS"},
		{name: "unsupported endpoint scheme", mutate: func(o *Options) { o.Endpoint = "ftp://s3.example.test" }, wantField: "Endpoint", wantReason: "HTTPS"},
		{name: "missing region", mutate: func(o *Options) { o.Region = "" }, wantField: "Region", wantReason: "must be set"},
		{name: "invalid region", mutate: func(o *Options) { o.Region = "us east/1" }, wantField: "Region", wantReason: "letters"},
		{name: "oversized region", mutate: func(o *Options) { o.Region = strings.Repeat("a", 64) }, wantField: "Region", wantReason: "63 bytes"},
		{name: "leading-hyphen region", mutate: func(o *Options) { o.Region = "-us-east-1" }, wantField: "Region", wantReason: "letters"},
		{name: "trailing-hyphen region", mutate: func(o *Options) { o.Region = "us-east-1-" }, wantField: "Region", wantReason: "letters"},
		{name: "missing bucket", mutate: func(o *Options) { o.Bucket = "" }, wantField: "Bucket", wantReason: "must be set"},
		{name: "short bucket", mutate: func(o *Options) { o.Bucket = "ab" }, wantField: "Bucket", wantReason: "S3 bucket name"},
		{name: "oversized bucket", mutate: func(o *Options) { o.Bucket = strings.Repeat("a", 64) }, wantField: "Bucket", wantReason: "S3 bucket name"},
		{name: "uppercase bucket", mutate: func(o *Options) { o.Bucket = "looPrig" }, wantField: "Bucket", wantReason: "S3 bucket name"},
		{name: "IP bucket", mutate: func(o *Options) { o.Bucket = "127.0.0.1" }, wantField: "Bucket", wantReason: "S3 bucket name"},
		{name: "adjacent-dot bucket", mutate: func(o *Options) { o.Bucket = "looprig..test" }, wantField: "Bucket", wantReason: "S3 bucket name"},
		{name: "leading-hyphen bucket", mutate: func(o *Options) { o.Bucket = "-looprig" }, wantField: "Bucket", wantReason: "S3 bucket name"},
		{name: "trailing-dot bucket", mutate: func(o *Options) { o.Bucket = "looprig." }, wantField: "Bucket", wantReason: "S3 bucket name"},
		{name: "missing deployment prefix", mutate: func(o *Options) { o.DeploymentPrefix = "" }, wantField: "DeploymentPrefix", wantReason: "must be set"},
		{name: "escaping deployment prefix", mutate: func(o *Options) { o.DeploymentPrefix = "deployments/../tenant" }, wantField: "DeploymentPrefix", wantReason: "canonical"},
		{name: "percent deployment prefix", mutate: func(o *Options) { o.DeploymentPrefix = "deployments/%2e%2e" }, wantField: "DeploymentPrefix", wantReason: "canonical"},
		{name: "unicode deployment prefix", mutate: func(o *Options) { o.DeploymentPrefix = "deployments/tenant-雪" }, wantField: "DeploymentPrefix", wantReason: "canonical"},
		{name: "oversized deployment prefix", mutate: func(o *Options) { o.DeploymentPrefix = strings.Repeat("a", maxDeploymentPrefixBytes+1) }, wantField: "DeploymentPrefix", wantReason: "256 bytes"},
		{name: "unknown addressing style", mutate: func(o *Options) { o.AddressingStyle = AddressingStyle(99) }, wantField: "AddressingStyle", wantReason: "unknown"},
		{name: "unknown encryption", mutate: func(o *Options) { o.Encryption = EncryptionMode(99) }, wantField: "Encryption", wantReason: "unknown"},
		{name: "KMS missing key", mutate: func(o *Options) { o.Encryption = EncryptionKMS }, wantField: "KMSKeyID", wantReason: "must be set"},
		{name: "KMS key on default encryption", mutate: func(o *Options) { o.KMSKeyID = "alias/secret-key" }, wantField: "KMSKeyID", wantReason: "only"},
		{name: "KMS key control byte", mutate: func(o *Options) { o.Encryption = EncryptionKMS; o.KMSKeyID = "alias/key\nsecret" }, wantField: "KMSKeyID", wantReason: "control"},
		{name: "oversized KMS key", mutate: func(o *Options) { o.Encryption = EncryptionKMS; o.KMSKeyID = strings.Repeat("k", maxKMSKeyIDBytes+1) }, wantField: "KMSKeyID", wantReason: "2048 bytes"},
		{name: "multipart threshold below minimum", mutate: func(o *Options) { o.MultipartThreshold = minMultipartBytes - 1 }, wantField: "MultipartThreshold", wantReason: "at least"},
		{name: "multipart threshold above maximum", mutate: func(o *Options) { o.MultipartThreshold = maxMultipartBytes + 1 }, wantField: "MultipartThreshold", wantReason: "at most"},
		{name: "part size below minimum", mutate: func(o *Options) { o.MultipartPartSize = minMultipartBytes - 1 }, wantField: "MultipartPartSize", wantReason: "at least"},
		{name: "part size above maximum", mutate: func(o *Options) { o.MultipartPartSize = maxMultipartBytes + 1 }, wantField: "MultipartPartSize", wantReason: "at most"},
		{name: "threshold below part size", mutate: func(o *Options) { o.MultipartThreshold = 8 << 20; o.MultipartPartSize = 16 << 20 }, wantField: "MultipartThreshold", wantReason: "MultipartPartSize"},
		{name: "negative concurrency", mutate: func(o *Options) { o.Concurrency = -1 }, wantField: "Concurrency", wantReason: "positive"},
		{name: "excess concurrency", mutate: func(o *Options) { o.Concurrency = maxConcurrency + 1 }, wantField: "Concurrency", wantReason: "at most"},
		{name: "negative concurrent transfers", mutate: func(o *Options) { o.MaxConcurrentTransfers = -1 }, wantField: "MaxConcurrentTransfers", wantReason: "positive"},
		{name: "excess concurrent transfers", mutate: func(o *Options) { o.MaxConcurrentTransfers = maxConcurrentTransfers + 1 }, wantField: "MaxConcurrentTransfers", wantReason: "at most"},
		{name: "threshold exceeds aggregate memory budget", mutate: func(o *Options) { o.MultipartThreshold = 256 << 20; o.MaxConcurrentTransfers = 3 }, wantField: "TransferMemoryBudget", wantReason: "512 MiB"},
		{name: "parts exceed aggregate memory budget", mutate: func(o *Options) {
			o.MultipartThreshold = 64 << 20
			o.MultipartPartSize = 64 << 20
			o.MaxConcurrentTransfers = 2
		}, wantField: "TransferMemoryBudget", wantReason: "512 MiB"},
		{name: "additive aggregate memory budget", mutate: func(o *Options) {
			o.MultipartThreshold = 128 << 20
			o.MultipartPartSize = 32 << 20
			o.Concurrency = 3
			o.MaxConcurrentTransfers = 3
		}, wantField: "TransferMemoryBudget", wantReason: "512 MiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := validOptions()
			if tt.mutate != nil {
				tt.mutate(&options)
			}
			resolved, err := options.resolve()
			if tt.wantField == "" {
				if err != nil {
					t.Fatalf("resolve: %v", err)
				}
				if resolved.multipartThreshold != defaultMultipartThreshold || resolved.multipartPartSize != defaultMultipartPartSize || resolved.concurrency != defaultConcurrency || resolved.maxConcurrentTransfers != defaultMaxConcurrentTransfers {
					t.Fatalf("resolved defaults = threshold %d part %d concurrency %d transfers %d", resolved.multipartThreshold, resolved.multipartPartSize, resolved.concurrency, resolved.maxConcurrentTransfers)
				}
				return
			}
			var optionsErr *OptionsError
			if !errors.As(err, &optionsErr) {
				t.Fatalf("resolve error = %T %v, want *OptionsError", err, err)
			}
			if optionsErr.Field != tt.wantField || !strings.Contains(optionsErr.Reason, tt.wantReason) {
				t.Errorf("OptionsError = field %q reason %q, want field %q reason containing %q", optionsErr.Field, optionsErr.Reason, tt.wantField, tt.wantReason)
			}
			for _, secret := range []string{"super-secret", "secret-key", "secret-signature"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error disclosed credential material %q: %q", secret, err)
				}
			}
		})
	}
}

func TestOptionsResolveAppliesCustomValues(t *testing.T) {
	t.Parallel()
	provider := testCredentialsProvider{}
	options := validOptions()
	options.AddressingStyle = AddressingPath
	options.Encryption = EncryptionKMS
	options.KMSKeyID = "alias/looprig-test"
	options.MultipartThreshold = 32 << 20
	options.MultipartPartSize = 8 << 20
	options.Concurrency = 7
	options.MaxConcurrentTransfers = 2
	options.Credentials = provider

	resolved, err := options.resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.addressingStyle != AddressingPath || resolved.encryption != EncryptionKMS || resolved.kmsKeyID != options.KMSKeyID {
		t.Fatalf("resolved policy = addressing %d encryption %d key %q", resolved.addressingStyle, resolved.encryption, resolved.kmsKeyID)
	}
	if resolved.multipartThreshold != options.MultipartThreshold || resolved.multipartPartSize != options.MultipartPartSize || resolved.concurrency != options.Concurrency || resolved.maxConcurrentTransfers != options.MaxConcurrentTransfers {
		t.Fatalf("resolved transfer bounds = threshold %d part %d concurrency %d transfers %d", resolved.multipartThreshold, resolved.multipartPartSize, resolved.concurrency, resolved.maxConcurrentTransfers)
	}
	if resolved.credentials != provider {
		t.Fatal("resolved credentials provider differs from injected provider")
	}
}

func TestDefaultTransferMemoryBudgetFitsCeiling(t *testing.T) {
	t.Parallel()
	resolved, err := validOptions().resolve()
	if err != nil {
		t.Fatalf("resolve defaults: %v", err)
	}
	perTransfer := resolved.multipartThreshold + int64(resolved.concurrency+1)*resolved.multipartPartSize
	aggregate := perTransfer * int64(resolved.maxConcurrentTransfers)
	if aggregate > maxAggregateTransferMemory {
		t.Fatalf("default worst-case transfer memory = %d MiB, exceeds %d MiB ceiling", aggregate>>20, maxAggregateTransferMemory>>20)
	}
}

func TestDefaultLoadConfigUsesInjectedCredentials(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	options := validOptions()
	options.Credentials = testCredentialsProvider{}
	resolved, err := options.resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	configuration, err := defaultLoadConfig(ctx, resolved)
	if err != nil {
		t.Fatalf("defaultLoadConfig: %v", err)
	}
	credentials, err := configuration.Credentials.Retrieve(ctx)
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if credentials.AccessKeyID != "not-retrieved-by-scaffold" {
		t.Errorf("injected AccessKeyID = %q, want test provider value", credentials.AccessKeyID)
	}
}

func TestDefaultLoadConfigUsesStandardCredentialChain(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "standard-chain-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "standard-chain-secret")
	t.Setenv("AWS_SESSION_TOKEN", "standard-chain-token")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	resolved, err := validOptions().resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	configuration, err := defaultLoadConfig(ctx, resolved)
	if err != nil {
		t.Fatalf("defaultLoadConfig: %v", err)
	}
	credentials, err := configuration.Credentials.Retrieve(ctx)
	if err != nil {
		t.Fatalf("standard credential chain Retrieve: %v", err)
	}
	if credentials.AccessKeyID != "standard-chain-access" || credentials.SessionToken != "standard-chain-token" {
		t.Fatal("defaultLoadConfig did not select controlled standard-chain credentials")
	}
}

func TestOptionsResolveAllowsExplicitLocalPlaintext(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []string{"http://localhost:9000", "http://127.0.0.1:9000", "http://[::1]:9000"} {
		t.Run(endpoint, func(t *testing.T) {
			options := validOptions()
			options.Endpoint = endpoint
			options.AllowInsecureLocalhostOnly = true
			if _, err := options.resolve(); err != nil {
				t.Fatalf("resolve: %v", err)
			}
		})
	}
}

func TestOptionsResolveRejectsRemotePlaintextDespiteTestOptIn(t *testing.T) {
	t.Parallel()
	options := validOptions()
	options.Endpoint = "http://s3.example.test"
	options.AllowInsecureLocalhostOnly = true
	_, err := options.resolve()
	var optionsErr *OptionsError
	if !errors.As(err, &optionsErr) || optionsErr.Field != "Endpoint" || !strings.Contains(optionsErr.Reason, "loopback") {
		t.Fatalf("resolve error = %T %v, want loopback-only *OptionsError", err, err)
	}
}

func TestOptionsErrorNeverUnwrapsOrRetainsSensitiveValue(t *testing.T) {
	t.Parallel()
	options := validOptions()
	options.Endpoint = "https://access:super-secret@s3.example.test"
	_, err := options.resolve()
	if errors.Unwrap(err) != nil || strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), options.Endpoint) {
		t.Fatalf("OptionsError = %T %v, want non-unwrapping redacted error", err, err)
	}
}
