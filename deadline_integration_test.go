//go:build integration

package s3store

import (
	"bytes"
	"context"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/looprig/s3store/internal/testserver"
	"github.com/looprig/storage"
	"github.com/looprig/storage/storetest"
)

// TestBlobsConformanceWithoutCallerDeadlines runs Storage's shared suite with
// every caller deadline stripped (context.WithoutCancel, the shape host
// v0.5.0 hands its store), so each call runs under the default bound alone.
func TestBlobsConformanceWithoutCallerDeadlines(t *testing.T) {
	storetest.TestBlobs(t, func(t *testing.T) storage.Blobs {
		server := testserver.New()
		t.Cleanup(server.Close)
		return undatedBlobs{store: newIntegrationStore(t, server, nil)}
	})
}

type undatedBlobs struct{ store *Store }

func (u undatedBlobs) Put(ctx context.Context, key string, source io.Reader) error {
	return u.store.Put(context.WithoutCancel(ctx), key, source)
}

func (u undatedBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return u.store.Get(context.WithoutCancel(ctx), key)
}

func (u undatedBlobs) Delete(ctx context.Context, key string) error {
	return u.store.Delete(context.WithoutCancel(ctx), key)
}

func (u undatedBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	return u.store.List(context.WithoutCancel(ctx), prefix)
}

// TestDeadlineFreeCallsSucceed is D2's positive case: the exact shape host
// v0.5.0 produces (context.WithoutCancel of a long-lived context) reaches the
// service and succeeds.
func TestDeadlineFreeCallsSucceed(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx := context.WithoutCancel(context.Background())
	key := "blobs/undated"
	// Large enough that the body is still on the wire when Get returns, so a
	// bound released at Get's return (rather than at stream end) is visible.
	body := bytes.Repeat([]byte("undated"), 1<<20)
	if err := store.Put(ctx, key, bytes.NewReader(body)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := readIntegrationBlob(t, ctx, store, key); !bytes.Equal(got, body) {
		t.Fatalf("Get returned %d bytes, want %d", len(got), len(body))
	}
	if listed, err := store.List(ctx, "blobs/"); err != nil || !slices.Equal(listed, []string{key}) {
		t.Fatalf("List = (%v, %v)", listed, err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// TestDeadlineFreeStreamIsBoundedByTheDefault: a Get without a deadline hands
// back a stream the default bound still covers, so a payload stalled on the
// network cannot pin a transfer slot forever.
func TestDeadlineFreeStreamIsBoundedByTheDefault(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	const bound = 500 * time.Millisecond
	store := newIntegrationStore(t, server, func(options *Options) { options.DefaultOperationTimeout = bound })
	key := "blobs/stalled"
	if err := store.Put(context.Background(), key, bytes.NewReader(bytes.Repeat([]byte("s"), 64<<10))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	server.StallNextPayloadRead(16)
	started := time.Now()
	reader, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer reader.Close()
	done := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(reader)
		done <- readErr
	}()
	select {
	case readErr := <-done:
		if readErr == nil {
			t.Fatal("a stalled stream completed without error")
		}
		if elapsed := time.Since(started); elapsed < bound/2 {
			t.Fatalf("stream failed after %v, before the %v bound", elapsed, bound)
		}
	case <-time.After(bound + 5*time.Second):
		t.Fatal("a deadline-free stalled stream outlived the default bound")
	}
}
