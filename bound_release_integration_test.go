//go:build integration

package s3store

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/looprig/s3store/internal/testserver"
)

// opaqueContext is a cancelable context the context package cannot see
// through. context.WithTimeout on such a parent starts one goroutine per child
// that lives until the child is cancelled, so a default bound whose cancel is
// never called is directly countable, long before its timer would fire.
type opaqueContext struct {
	done chan struct{}
	once sync.Once
}

func newOpaqueContext() *opaqueContext { return &opaqueContext{done: make(chan struct{})} }

func (*opaqueContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *opaqueContext) Done() <-chan struct{}     { return c.done }
func (c *opaqueContext) Value(any) any             { return nil }
func (c *opaqueContext) cancel()                   { c.once.Do(func() { close(c.done) }) }
func (c *opaqueContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// TestDefaultBoundIsReleasedAfterEveryOperation witnesses that each
// deadline-free operation cancels its default bound when it ends, and that a
// Get's bound is released when its stream ends, by EOF or by Close.
func TestDefaultBoundIsReleasedAfterEveryOperation(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	parent := newOpaqueContext()
	t.Cleanup(parent.cancel)
	var ctx context.Context = parent

	const rounds = 50
	body := []byte("released")
	if err := store.Put(ctx, "blobs/warm", bytes.NewReader(body)); err != nil {
		t.Fatalf("warm-up Put: %v", err)
	}
	baseline := settledGoroutines()

	for i := 0; i < rounds; i++ {
		key := "blobs/" + strconv.Itoa(i)
		if err := store.Put(ctx, key, bytes.NewReader(body)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		// Stream ended by EOF, never closed.
		reader, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if _, err := io.ReadAll(reader); err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		// Stream ended by Close before any read.
		reader, err = store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := store.List(ctx, "blobs/"); err != nil {
			t.Fatalf("List: %v", err)
		}
		if err := store.Delete(ctx, key); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}

	if after := settledGoroutines(); after > baseline+rounds/2 {
		t.Fatalf("goroutines %d -> %d after %d rounds: a default bound was not released", baseline, after, rounds)
	}
}

func settledGoroutines() int {
	count := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		time.Sleep(10 * time.Millisecond)
		next := runtime.NumGoroutine()
		if next == count {
			return next
		}
		count = next
	}
	return count
}
