//go:build integration

package s3store

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/looprig/s3store/internal/testserver"
	"github.com/looprig/storage"
)

// sessionStoreShapedKey is a logical key the length of a SessionStore object
// key (about 235 bytes), which v0.1.1 could not store on MinIO.
func sessionStoreShapedKey(suffix string) string {
	return "tenants/" + strings.Repeat("t", 52) + "/sessions/" + strings.Repeat("s", 52) +
		"/blobs/v1/checkpoint/" + strings.Repeat("0", 64) + "/" + suffix
}

func TestLongLogicalKeyUsesBoundedSegmentsEndToEnd(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := sessionStoreShapedKey("000000000000000000000000000000000001")
	if len(key) < 192 {
		t.Fatalf("fixture key is %d bytes; it must exceed v0.1.1's 191-byte limit", len(key))
	}
	if err := store.Put(ctx, key, bytes.NewReader([]byte("long"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for _, stored := range server.Keys("looprig-test") {
		for _, segment := range strings.Split(stored, "/") {
			if len(segment) > maxObjectKeySegmentBytes {
				t.Fatalf("stored key has a %d-byte segment: %q", len(segment), stored)
			}
		}
	}
	if got := readIntegrationBlob(t, ctx, store, key); string(got) != "long" {
		t.Fatalf("Get = %q, want long", got)
	}
	listed, err := store.List(ctx, "tenants/")
	if err != nil || !slices.Equal(listed, []string{key}) {
		t.Fatalf("List = (%v, %v), want [%s]", listed, err, key)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, key); !isNotFound(err) {
		t.Fatalf("Get after Delete = %v, want BlobNotFoundError", err)
	}
}

// TestLongLogicalKeyOnASegmentLimitedService emulates MinIO in-process: every
// request naming an over-long segment is refused 400, including the lookups
// of v0.1.1's legacy encoding, which must read as "no legacy row" there.
func TestLongLogicalKeyOnASegmentLimitedService(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	server.RefuseSegmentsOver(maxObjectKeySegmentBytes)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := sessionStoreShapedKey("limited")
	if err := store.Put(ctx, key, bytes.NewReader([]byte("limited"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Put(ctx, key, bytes.NewReader([]byte("limited"))); err != nil {
		t.Fatalf("idempotent Put: %v", err)
	}
	if got := readIntegrationBlob(t, ctx, store, key); string(got) != "limited" {
		t.Fatalf("Get = %q", got)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, key); !isNotFound(err) {
		t.Fatalf("Get after Delete = %v, want BlobNotFoundError", err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete of an absent key: %v", err)
	}
}

// TestManifestHeadBadRequestIsNotAbsence pins the other side of the legacy
// 400 rule: for a key this release writes, a 400 is a backend failure and
// must never read as "not found", which would let a Put overwrite and a Get
// report a present blob as missing.
func TestManifestHeadBadRequestIsNotAbsence(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.FailNext("HeadManifest", 1, 400, "BadRequest")
	_, err := store.Get(ctx, "blobs/short")
	var backend *BackendError
	if !errors.As(err, &backend) {
		t.Fatalf("Get after a 400 manifest HEAD = %T %v, want *BackendError", err, err)
	}
}

// TestV011LongKeyRowStaysReadableAndImmutable reproduces an object v0.1.1
// wrote above 191 bytes on a service without a segment limit, by relocating
// the manifest to the single-segment key that release derived.
func TestV011LongKeyRowStaysReadableAndImmutable(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := sessionStoreShapedKey("legacy")
	if err := store.Put(ctx, key, bytes.NewReader([]byte("legacy bytes"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	current, err := manifestObjectKey(store.options.deploymentPrefix, key)
	if err != nil {
		t.Fatal(err)
	}
	legacy, differs := legacyManifestObjectKey(store.options.deploymentPrefix, key)
	if !differs || !server.MoveRaw("looprig-test", current, legacy) {
		t.Fatal("could not reproduce the v0.1.1 row")
	}

	if got := readIntegrationBlob(t, ctx, store, key); string(got) != "legacy bytes" {
		t.Fatalf("Get legacy row = %q", got)
	}
	listed, err := store.List(ctx, "")
	if err != nil || !slices.Equal(listed, []string{key}) {
		t.Fatalf("List = (%v, %v), want exactly [%s]", listed, err, key)
	}
	if err := store.Put(ctx, key, bytes.NewReader([]byte("legacy bytes"))); err != nil {
		t.Fatalf("idempotent Put over legacy row: %v", err)
	}
	var conflict *storage.BlobConflictError
	if err := store.Put(ctx, key, bytes.NewReader([]byte("different"))); !errors.As(err, &conflict) {
		t.Fatalf("Put of different bytes over legacy row = %T %v, want *storage.BlobConflictError", err, err)
	}
	for _, stored := range server.Keys("looprig-test") {
		if stored == current {
			t.Fatal("a Put over a legacy row published a second manifest under the new encoding")
		}
	}
	if got := readIntegrationBlob(t, ctx, store, key); string(got) != "legacy bytes" {
		t.Fatalf("Get after refused overwrite = %q, want the original bytes", got)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete legacy row: %v", err)
	}
	if _, err := store.Get(ctx, key); !isNotFound(err) {
		t.Fatalf("Get after Delete = %v, want BlobNotFoundError", err)
	}
	if keys := server.Keys("looprig-test"); len(keys) != 0 {
		t.Fatalf("objects left after Delete: %v", keys)
	}
}

func isNotFound(err error) bool {
	var notFound *storage.BlobNotFoundError
	return errors.As(err, &notFound)
}
