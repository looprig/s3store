package s3store

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/looprig/storage"
)

// TestOpenWithoutDeadlineAppliesTheDefaultBound holds D2: the Storage contract
// does not require a caller deadline, so Open supplies the configured default
// instead of refusing, and a caller's own deadline always wins.
func TestOpenWithoutDeadlineAppliesTheDefaultBound(t *testing.T) {
	var observed time.Time
	var hadDeadline bool
	original := loadConfig
	loadConfig = func(ctx context.Context, options resolvedOptions) (aws.Config, error) {
		observed, hadDeadline = ctx.Deadline()
		return aws.Config{Region: options.region, Credentials: aws.NewCredentialsCache(signableCredentials{})}, nil
	}
	t.Cleanup(func() { loadConfig = original })

	options := validOptions()
	options.DefaultOperationTimeout = 7 * time.Second
	started := time.Now()
	store, err := Open(context.Background(), options)
	if err != nil || store == nil {
		t.Fatalf("Open without a deadline = (%v, %v), want a Store", store, err)
	}
	if !hadDeadline {
		t.Fatal("Open ran its SDK loader on an unbounded context")
	}
	if remaining := observed.Sub(started); remaining < 6*time.Second || remaining > 8*time.Second {
		t.Fatalf("default bound = %v, want about 7s", remaining)
	}

	// A caller deadline LONGER than the default still wins: the default only
	// fills an absent deadline and never shortens a caller's.
	callerDeadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()
	if _, err := Open(ctx, options); err != nil {
		t.Fatalf("Open with a caller deadline: %v", err)
	}
	if !observed.Equal(callerDeadline) {
		t.Fatalf("loader deadline = %v, want the caller's %v", observed, callerDeadline)
	}
}

func TestDefaultOperationTimeoutOption(t *testing.T) {
	resolved, err := validOptions().resolve()
	if err != nil {
		t.Fatal(err)
	}
	if resolved.operationTimeout != DefaultOperationTimeout {
		t.Fatalf("zero DefaultOperationTimeout resolves to %v, want %v", resolved.operationTimeout, DefaultOperationTimeout)
	}
	if DefaultOperationTimeout != 30*time.Second {
		t.Fatalf("DefaultOperationTimeout = %v, want the documented 30s", DefaultOperationTimeout)
	}
	for _, bad := range []time.Duration{-time.Second, time.Microsecond} {
		options := validOptions()
		options.DefaultOperationTimeout = bad
		_, err := options.resolve()
		var optionsErr *OptionsError
		if !errors.As(err, &optionsErr) || optionsErr.Field != "DefaultOperationTimeout" {
			t.Errorf("DefaultOperationTimeout %v error = %T %v, want *OptionsError for the field", bad, err, err)
		}
	}
}

// TestStoreImplementsBlobsAndTheReaderLifecycleOnly asserts the whole cross
// product the README and CLAUDE.md claim, not one representative of it, so no
// later task can widen the surface without failing here.
//
// BlobReaderLifecycle moved from the excluded side to the required side. It was
// only ever excluded as part of this scope guard -- nothing in the module gave
// an S3-specific reason to refuse it, unlike fsstore, whose refusal is an
// inability. Its exclusion is now a positive claim with its own conformance
// suite and its own blocked-I/O probe, so exclusion here would be a silent
// regression rather than a narrowing.
func TestStoreImplementsBlobsAndTheReaderLifecycleOnly(t *testing.T) {
	var _ storage.Blobs = (*Store)(nil)
	var _ storage.BlobReaderLifecycle = (*Store)(nil)
	store := any((*Store)(nil))
	lifecycle, ok := store.(storage.BlobReaderLifecycle)
	if !ok {
		t.Fatal("Store does not implement storage.BlobReaderLifecycle; sessionstore.Open rejects such a provider before any I/O")
	}
	// The bound is a constant of the type, not of an instance, so the typed-nil
	// Store above answers it. That also keeps this test out of the
	// unconstructed-Store guard, which forbids building a Store any other way
	// than through newStore.
	if bound := lifecycle.BlobReaderCloseBound(); bound <= 0 {
		t.Errorf("BlobReaderCloseBound() = %v, want a positive documented bound", bound)
	}
	excluded := []struct {
		name  string
		check func(any) bool
	}{
		{"storage.Ledger", func(v any) bool { _, ok := v.(storage.Ledger); return ok }},
		{"storage.Leaser", func(v any) bool { _, ok := v.(storage.Leaser); return ok }},
		{"storage.KV", func(v any) bool { _, ok := v.(storage.KV); return ok }},
		{"storage.OrderedIndex", func(v any) bool { _, ok := v.(storage.OrderedIndex); return ok }},
	}
	for _, interfaceCase := range excluded {
		if interfaceCase.check(store) {
			t.Errorf("Store implements %s; s3store provides Blobs and the reader lifecycle only", interfaceCase.name)
		}
	}
	typeOfStore := reflect.TypeOf((*Store)(nil))
	var methods []string
	for index := 0; index < typeOfStore.NumMethod(); index++ {
		methods = append(methods, typeOfStore.Method(index).Name)
	}
	wantMethods := []string{"BlobReaderCloseBound", "Delete", "Get", "List", "Put"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("exported Store methods = %v, want exactly %v; signed/public URL APIs belong in Factory", methods, wantMethods)
	}
}

func TestOpenRejectsOptionsBeforeSDKConstruction(t *testing.T) {
	original := loadConfig
	called := false
	loadConfig = func(context.Context, resolvedOptions) (aws.Config, error) {
		called = true
		return aws.Config{}, nil
	}
	t.Cleanup(func() { loadConfig = original })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	options := validOptions()
	options.Endpoint = "https://access:super-secret@s3.example.test"
	store, err := Open(ctx, options)
	if store != nil || err == nil {
		t.Fatalf("Open = (%v, %v), want nil, error", store, err)
	}
	if called {
		t.Fatal("SDK config loader called after invalid options")
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("Open error disclosed credential: %q", err)
	}
}

func TestOpenRedactsEverySDKLoaderFailure(t *testing.T) {
	tests := []struct {
		name   string
		secret string
	}{
		{name: "credential provider failure", secret: "AKIA_TEST:super-secret"},
		{name: "presigned URL failure", secret: "https://example.test/key?X-Amz-Credential=secret-scope&X-Amz-Signature=secret-signature"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := loadConfig
			loadConfig = func(context.Context, resolvedOptions) (aws.Config, error) {
				return aws.Config{}, errors.New(tt.secret)
			}
			t.Cleanup(func() { loadConfig = original })

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			store, err := Open(ctx, validOptions())
			if store != nil {
				t.Fatal("Open returned Store with SDK loader error")
			}
			if err == nil || strings.Contains(err.Error(), tt.secret) || errors.Unwrap(err) != nil {
				t.Fatalf("Open error = %T %v, want non-unwrapping redacted error", err, err)
			}
		})
	}
}

// forbiddenHTTPClient fails the test on any outbound request. It is installed
// on the stub aws.Config so "performs no S3 request" is enforced by the
// transport rather than asserted by the test's name.
type forbiddenHTTPClient struct {
	t        *testing.T
	attempts atomic.Int64
}

func (c *forbiddenHTTPClient) Do(request *http.Request) (*http.Response, error) {
	c.attempts.Add(1)
	c.t.Errorf("Open performed network I/O: %s %s; P2.1 must contact no endpoint", request.Method, request.URL.Host)
	return nil, errors.New("s3store test: network I/O is forbidden during Open")
}

// signableCredentials resolves complete credentials so a mutated Open reaches
// the transport instead of failing earlier in the signer, which would let a
// network-I/O mutation survive.
type signableCredentials struct{}

func (signableCredentials) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret", Source: "s3store test"}, nil
}

func TestOpenWiresBlobScaffoldWithoutNetworkIO(t *testing.T) {
	transport := &forbiddenHTTPClient{t: t}
	original := loadConfig
	loadConfig = func(_ context.Context, options resolvedOptions) (aws.Config, error) {
		return aws.Config{
			Region:      options.region,
			Credentials: aws.NewCredentialsCache(signableCredentials{}),
			HTTPClient:  transport,
			Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
		}, nil
	}
	t.Cleanup(func() { loadConfig = original })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	options := validOptions()
	options.AddressingStyle = AddressingPath
	options.MultipartThreshold = 32 << 20
	options.MultipartPartSize = 8 << 20
	options.Concurrency = 7
	options.MaxConcurrentTransfers = 2
	store, err := Open(ctx, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if store == nil || store.client == nil || store.transfers == nil {
		t.Fatalf("incomplete scaffold: store=%v client=%v transfers=%v",
			store == nil, store == nil || store.client == nil, store == nil || store.transfers == nil)
	}
	clientOptions := reflect.ValueOf(store.client).Elem().FieldByName("options")
	if !clientOptions.FieldByName("UsePathStyle").Bool() {
		t.Error("S3 client UsePathStyle = false, want true")
	}
	baseEndpoint := clientOptions.FieldByName("BaseEndpoint")
	if baseEndpoint.IsNil() || baseEndpoint.Elem().String() != options.Endpoint {
		t.Errorf("S3 client BaseEndpoint = %v, want %q", baseEndpoint, options.Endpoint)
	}
	transferOptions := reflect.ValueOf(store.transfers).Elem().FieldByName("options")
	if got := transferOptions.FieldByName("MultipartUploadThreshold").Int(); got != options.MultipartThreshold {
		t.Errorf("transfer threshold = %d, want %d", got, options.MultipartThreshold)
	}
	if got := transferOptions.FieldByName("PartSizeBytes").Int(); got != options.MultipartPartSize {
		t.Errorf("transfer part size = %d, want %d", got, options.MultipartPartSize)
	}
	if got := transferOptions.FieldByName("Concurrency").Int(); got != int64(options.Concurrency) {
		t.Errorf("transfer concurrency = %d, want %d", got, options.Concurrency)
	}
	if got, want := transferOptions.FieldByName("GetObjectBufferSize").Int(), int64(options.Concurrency)*options.MultipartPartSize; got != want {
		t.Errorf("transfer Get buffer = %d, want %d", got, want)
	}
	// This detects a wrong pinned value but cannot detect the assignment's
	// removal: the SDK default is the same 10000. That gap is deliberate and
	// is covered by TestMaxUploadPartsPinMatchesTheSDKDefault instead.
	if got := transferOptions.FieldByName("MaxUploadParts").Int(); got != maxUploadParts {
		t.Errorf("transfer MaxUploadParts = %d, want the pinned %d", got, maxUploadParts)
	}
	if store.options.maxAccountedObjectSize != accountedObjectSize(options.MultipartPartSize) {
		t.Errorf("retained accounted object size = %d, want %d",
			store.options.maxAccountedObjectSize, accountedObjectSize(options.MultipartPartSize))
	}
	if store.options.multipartThreshold != options.MultipartThreshold || store.options.bucket != options.Bucket || store.options.deploymentPrefix != options.DeploymentPrefix {
		t.Errorf("retained scaffold policy = threshold %d bucket %q prefix %q", store.options.multipartThreshold, store.options.bucket, store.options.deploymentPrefix)
	}
	if cap(store.transferSlots) != options.MaxConcurrentTransfers {
		t.Errorf("Store transfer slots = %d, want %d", cap(store.transferSlots), options.MaxConcurrentTransfers)
	}
	if attempts := transport.attempts.Load(); attempts != 0 {
		t.Fatalf("Open issued %d HTTP request(s); P2.1 performs no S3 request", attempts)
	}
}

func TestBlobOperationsRejectNilContext(t *testing.T) {
	store := newScaffoldStore()
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{name: "Put", call: func(ctx context.Context) error { return store.Put(ctx, "blobs/key", bytes.NewReader(nil)) }},
		{name: "Get", call: func(ctx context.Context) error { _, err := store.Get(ctx, "blobs/key"); return err }},
		{name: "Delete", call: func(ctx context.Context) error { return store.Delete(ctx, "blobs/key") }},
		{name: "List", call: func(ctx context.Context) error { _, err := store.List(ctx, "blobs/"); return err }},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			err := operation.call(nil)
			var deadlineErr *DeadlineRequiredError
			if !errors.As(err, &deadlineErr) {
				t.Fatalf("%s error = %T %v, want *DeadlineRequiredError", operation.name, err, err)
			}
			if deadlineErr.Operation != "Blobs."+operation.name {
				t.Errorf("operation = %q, want %q", deadlineErr.Operation, "Blobs."+operation.name)
			}
		})
	}
}

func TestBlobOperationsValidateKeysBeforeStubResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := newScaffoldStore()
	operations := []struct {
		name string
		call func(string) error
	}{
		{name: "Put", call: func(key string) error { return store.Put(ctx, key, bytes.NewReader(nil)) }},
		{name: "Get", call: func(key string) error { _, err := store.Get(ctx, key); return err }},
		{name: "Delete", call: func(key string) error { return store.Delete(ctx, key) }},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			const invalidKey = "../escape"
			err := operation.call(invalidKey)
			var invalid *storage.InvalidNameError
			if !errors.As(err, &invalid) {
				t.Fatalf("%s error = %T %v, want *storage.InvalidNameError", operation.name, err, err)
			}
			if invalid.Name != invalidKey {
				t.Fatalf("%s InvalidNameError.Name = %q, want caller key %q", operation.name, invalid.Name, invalidKey)
			}
		})
	}
}

func TestListPrefixValidation(t *testing.T) {
	for _, prefix := range []string{"", "blobs", "blobs/"} {
		if err := validateListPrefix(prefix); err != nil {
			t.Errorf("validateListPrefix(%q) = %v, want nil", prefix, err)
		}
	}
	err := validateListPrefix("../")
	var invalid *storage.InvalidNameError
	if !errors.As(err, &invalid) {
		t.Fatalf("invalid prefix error = %T %v, want *storage.InvalidNameError", err, err)
	}
}

// newScaffoldStore builds a client-free Store through the package constructor,
// so every test shares the transferSlots invariant that Open establishes.
func newScaffoldStore() *Store {
	return newStore(nil, nil, resolvedOptions{maxConcurrentTransfers: 1})
}

func validOptions() Options {
	return Options{
		Endpoint:         "https://s3.example.test",
		Region:           "us-east-1",
		Bucket:           "looprig-test",
		DeploymentPrefix: "deployments/test",
		Encryption:       EncryptionAES256,
	}
}
