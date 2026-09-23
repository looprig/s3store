package s3store

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// hungService accepts every request and never answers until the test ends:
// the failure a deadline exists to bound (a wedged endpoint or middlebox).
func hungService(t *testing.T) string {
	t.Helper()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case <-release:
		case <-request.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); server.Close() })
	return server.URL
}

func openAgainst(t *testing.T, endpoint string, defaultTimeout time.Duration) *Store {
	t.Helper()
	options := validOptions()
	options.Endpoint = endpoint
	options.AllowInsecureLocalhostOnly = true
	options.AddressingStyle = AddressingPath
	options.Credentials = signableCredentials{}
	options.DefaultOperationTimeout = defaultTimeout
	store, err := Open(context.Background(), options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return store
}

func blobOperations(store *Store) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"Put":    func(ctx context.Context) error { return store.Put(ctx, "blobs/key", bytes.NewReader([]byte("x"))) },
		"Get":    func(ctx context.Context) error { _, err := store.Get(ctx, "blobs/key"); return err },
		"Delete": func(ctx context.Context) error { return store.Delete(ctx, "blobs/key") },
		"List":   func(ctx context.Context) error { _, err := store.List(ctx, "blobs/"); return err },
	}
}

// TestHungBackendReturnsWithinTheDefaultBound: a call with no caller deadline
// against a service that never answers returns at the default bound with the
// deadline error, rather than hanging.
func TestHungBackendReturnsWithinTheDefaultBound(t *testing.T) {
	t.Parallel()
	const bound = 300 * time.Millisecond
	store := openAgainst(t, hungService(t), bound)
	for name, call := range blobOperations(store) {
		t.Run(name, func(t *testing.T) {
			started := time.Now()
			done := make(chan error, 1)
			go func() { done <- call(context.Background()) }()
			select {
			case err := <-done:
				elapsed := time.Since(started)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("%s error = %T %v, want context.DeadlineExceeded", name, err, err)
				}
				if elapsed < bound/2 {
					t.Fatalf("%s returned after %v, before the %v default bound", name, elapsed, bound)
				}
			case <-time.After(bound + 3*time.Second):
				t.Fatalf("%s did not return within the %v default bound", name, bound)
			}
		})
	}
}

// TestCallerDeadlineWinsOverTheDefault: a caller's deadline is used as is,
// whether it is shorter or longer than the default.
func TestCallerDeadlineWinsOverTheDefault(t *testing.T) {
	t.Parallel()
	endpoint := hungService(t)
	t.Run("shorter", func(t *testing.T) {
		t.Parallel()
		store := openAgainst(t, endpoint, time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		started := time.Now()
		if err := store.Delete(ctx, "blobs/key"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Delete error = %T %v, want context.DeadlineExceeded", err, err)
		}
		if elapsed := time.Since(started); elapsed > 3*time.Second {
			t.Fatalf("Delete returned after %v; the caller's 200ms deadline was not used", elapsed)
		}
	})
	t.Run("longer", func(t *testing.T) {
		t.Parallel()
		store := openAgainst(t, endpoint, 100*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
		defer cancel()
		started := time.Now()
		if err := store.Delete(ctx, "blobs/key"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Delete error = %T %v, want context.DeadlineExceeded", err, err)
		}
		if elapsed := time.Since(started); elapsed < time.Second {
			t.Fatalf("Delete returned after %v; the 100ms default overrode the caller's 1.2s deadline", elapsed)
		}
	})
}

// TestCancellationWithoutDeadlineIsHonoured: the default bound is a child of
// the caller's context, so cancelling that context still ends the call.
func TestCancellationWithoutDeadlineIsHonoured(t *testing.T) {
	t.Parallel()
	store := openAgainst(t, hungService(t), time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	started := time.Now()
	err := store.Delete(ctx, "blobs/key")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete error = %T %v, want context.Canceled", err, err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("Delete returned %v after cancellation", elapsed)
	}
}
