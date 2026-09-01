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

func TestOpenRequiresDeadline(t *testing.T) {
	store, err := Open(context.Background(), validOptions())
	if store != nil {
		t.Fatal("Open returned a Store without a caller deadline")
	}
	var deadlineErr *DeadlineRequiredError
	if !errors.As(err, &deadlineErr) {
		t.Fatalf("Open error = %T %v, want *DeadlineRequiredError", err, err)
	}
}

// TestStoreImplementsOnlyBlobs asserts the whole cross product the README and
// CLAUDE.md claim, not one representative of it, so P2.2 cannot widen the
// surface without failing here.
func TestStoreImplementsOnlyBlobs(t *testing.T) {
	var _ storage.Blobs = (*Store)(nil)
	store := any((*Store)(nil))
	excluded := []struct {
		name  string
		check func(any) bool
	}{
		{"storage.Ledger", func(v any) bool { _, ok := v.(storage.Ledger); return ok }},
		{"storage.Leaser", func(v any) bool { _, ok := v.(storage.Leaser); return ok }},
		{"storage.KV", func(v any) bool { _, ok := v.(storage.KV); return ok }},
		{"storage.OrderedIndex", func(v any) bool { _, ok := v.(storage.OrderedIndex); return ok }},
		{"storage.BlobReaderLifecycle", func(v any) bool { _, ok := v.(storage.BlobReaderLifecycle); return ok }},
	}
	for _, interfaceCase := range excluded {
		if interfaceCase.check(store) {
			t.Errorf("Store implements %s; s3store is Blobs-only and claims no optional capability", interfaceCase.name)
		}
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

func TestBlobOperationsRequireDeadlineBeforeStubResult(t *testing.T) {
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
			err := operation.call(context.Background())
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

func TestBlobOperationsRejectNilContext(t *testing.T) {
	store := newScaffoldStore()
	//lint:ignore SA1012 This test exercises the public nil-context rejection guard.
	err := store.Put(nil, "blobs/key", bytes.NewReader(nil))
	var deadlineErr *DeadlineRequiredError
	if !errors.As(err, &deadlineErr) {
		t.Fatalf("Put error = %T %v, want *DeadlineRequiredError", err, err)
	}
}

func TestBlobOperationsReturnHonestNotImplementedResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := newScaffoldStore()

	if err := store.Put(ctx, "blobs/key", bytes.NewReader(nil)); !isNotImplemented(err, "Blobs.Put") {
		t.Errorf("Put error = %T %v, want Blobs.Put *NotImplementedError", err, err)
	}
	reader, err := store.Get(ctx, "blobs/key")
	if reader != nil || !isNotImplemented(err, "Blobs.Get") {
		t.Errorf("Get = (%v, %T %v), want nil, Blobs.Get *NotImplementedError", reader, err, err)
	}
	if err := store.Delete(ctx, "blobs/key"); !isNotImplemented(err, "Blobs.Delete") {
		t.Errorf("Delete error = %T %v, want Blobs.Delete *NotImplementedError", err, err)
	}
	keys, err := store.List(ctx, "blobs/")
	if keys != nil || !isNotImplemented(err, "Blobs.List") {
		t.Errorf("List = (%v, %T %v), want nil, Blobs.List *NotImplementedError", keys, err, err)
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
			err := operation.call("../escape")
			var invalid *storage.InvalidNameError
			if !errors.As(err, &invalid) {
				t.Fatalf("%s error = %T %v, want *storage.InvalidNameError", operation.name, err, err)
			}
		})
	}
}

func TestListPrefixValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := newScaffoldStore()
	for _, prefix := range []string{"", "blobs", "blobs/"} {
		keys, err := store.List(ctx, prefix)
		if keys != nil || !isNotImplemented(err, "Blobs.List") {
			t.Errorf("List(%q) = (%v, %T %v), want nil, *NotImplementedError", prefix, keys, err, err)
		}
	}
	_, err := store.List(ctx, "../")
	var invalid *storage.InvalidNameError
	if !errors.As(err, &invalid) {
		t.Fatalf("List invalid prefix error = %T %v, want *storage.InvalidNameError", err, err)
	}
}

func isNotImplemented(err error, operation string) bool {
	var notImplemented *NotImplementedError
	return errors.As(err, &notImplemented) && notImplemented.Operation == operation
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
