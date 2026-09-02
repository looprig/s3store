//go:build integration

package s3store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/looprig/s3store/internal/testserver"
	"github.com/looprig/storage"
)

// collidingSessionID and collidingObjectID are the identifiers both tenants use
// in every test in this file. They are shared constants rather than per-tenant
// values so the collision is constructed, not merely permitted: if the tenant
// component stopped separating the two stores, every key below would be the
// same key.
const (
	collidingSessionID = "s-1701"
	collidingObjectID  = "o-42"
)

func collidingLogicalKey() string {
	return "sessions/" + collidingSessionID + "/blobs/" + collidingObjectID
}

// TestTenantDeploymentsShareOneBucketWithoutCrossing puts two tenants in ONE
// bucket, each with its own deployment prefix, and gives them the same
// SessionID and the same ObjectID. Every assertion is over observed behaviour —
// a write that must not conflict, a read that must return its own bytes, a list
// that must not name the neighbour, a delete that must not reach across — plus
// a final disjointness check over the backend keys the fixture actually holds.
//
// It deliberately does not assert that a key "contains the tenant". A shape
// assertion passes on a derivation that formats the tenant in and then ignores
// it; the conflict, read, list, and delete rows below do not.
func TestTenantDeploymentsShareOneBucketWithoutCrossing(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	alphaRoot, betaRoot := "tenants/alpha", "tenants/beta"
	alpha := newIntegrationStore(t, server, func(options *Options) { options.DeploymentPrefix = alphaRoot })
	beta := newIntegrationStore(t, server, func(options *Options) { options.DeploymentPrefix = betaRoot })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	key := collidingLogicalKey()
	if err := alpha.Put(ctx, key, strings.NewReader("alpha-bytes")); err != nil {
		t.Fatalf("alpha Put(%q): %v", key, err)
	}
	// The write path: different bytes under an identical logical key must
	// commit, because the two tenants are not writing the same object. A shared
	// derivation surfaces here as *storage.BlobConflictError from the immutable
	// conditional create.
	if err := beta.Put(ctx, key, strings.NewReader("beta-bytes")); err != nil {
		var conflict *storage.BlobConflictError
		if errors.As(err, &conflict) {
			t.Fatalf("beta Put(%q) conflicted with the alpha tenant's object: %v", key, err)
		}
		t.Fatalf("beta Put(%q): %v", key, err)
	}

	// The read path.
	if got := string(readIntegrationBlob(t, ctx, alpha, key)); got != "alpha-bytes" {
		t.Errorf("alpha Get(%q) = %q, want %q", key, got, "alpha-bytes")
	}
	if got := string(readIntegrationBlob(t, ctx, beta, key)); got != "beta-bytes" {
		t.Errorf("beta Get(%q) = %q, want %q", key, got, "beta-bytes")
	}

	// The list path. Each tenant sees one key and it is its own; a crossing
	// listing would report the neighbour's identical logical key twice or
	// report a foreign row.
	for _, listing := range []struct {
		name  string
		store *Store
	}{{"alpha", alpha}, {"beta", beta}} {
		keys, err := listing.store.List(ctx, "")
		if err != nil {
			t.Fatalf("%s List: %v", listing.name, err)
		}
		if !reflect.DeepEqual(keys, []string{key}) {
			t.Errorf("%s List = %v, want exactly %v", listing.name, keys, []string{key})
		}
	}

	// The delete path. Deleting one tenant's object must not remove, or make
	// unreadable, the other tenant's identically named object.
	if err := alpha.Delete(ctx, key); err != nil {
		t.Fatalf("alpha Delete(%q): %v", key, err)
	}
	if _, err := alpha.Get(ctx, key); err == nil {
		t.Errorf("alpha Get(%q) after Delete returned a reader, want not found", key)
	} else {
		var notFound *storage.BlobNotFoundError
		if !errors.As(err, &notFound) {
			t.Errorf("alpha Get(%q) after Delete = %v, want *storage.BlobNotFoundError", key, err)
		}
	}
	if got := string(readIntegrationBlob(t, ctx, beta, key)); got != "beta-bytes" {
		t.Errorf("beta Get(%q) after the alpha tenant deleted its own object = %q, want %q", key, got, "beta-bytes")
	}
	keys, err := beta.List(ctx, "")
	if err != nil {
		t.Fatalf("beta List after alpha Delete: %v", err)
	}
	if !reflect.DeepEqual(keys, []string{key}) {
		t.Errorf("beta List after alpha Delete = %v, want exactly %v", keys, []string{key})
	}

	// Backend disjointness. Every surviving object belongs to exactly one
	// tenant root, and both tenants own at least one object, so the check is
	// not vacuously satisfied by an empty bucket.
	stored := server.Keys("looprig-test")
	var alphaCount, betaCount int
	for _, backendKey := range stored {
		inAlpha := strings.HasPrefix(backendKey, alphaRoot+"/")
		inBeta := strings.HasPrefix(backendKey, betaRoot+"/")
		if inAlpha == inBeta {
			t.Errorf("backend key %q belongs to %d tenant roots, want exactly 1", backendKey, boolCount(inAlpha, inBeta))
		}
		if inAlpha {
			alphaCount++
		}
		if inBeta {
			betaCount++
		}
	}
	if betaCount == 0 {
		t.Fatalf("beta tenant owns no backend object among %d; the disjointness check would be vacuous", len(stored))
	}
	// alpha deleted its logical object above, so only its payload may remain.
	if alphaCount+betaCount != len(stored) {
		t.Fatalf("backend keys = %v, want every key attributed to one tenant root", stored)
	}
}

// TestTenantsInOneDeploymentPrefixDoNotCross is the same collision one position
// over: ONE bucket, ONE deployment prefix, two tenants separated only by the
// leading component of the logical key, again with identical SessionID and
// ObjectID. This is the arrangement in which the deployment prefix cannot help,
// so it isolates the exact-key and exact-prefix behaviour of Get, List, and
// Delete rather than the derivation's tenant component.
func TestTenantsInOneDeploymentPrefixDoNotCross(t *testing.T) {
	server := testserver.New()
	t.Cleanup(server.Close)
	store := newIntegrationStore(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	suffix := collidingLogicalKey()
	alphaKey := "tenants/alpha/" + suffix
	betaKey := "tenants/beta/" + suffix
	if err := store.Put(ctx, alphaKey, strings.NewReader("alpha-bytes")); err != nil {
		t.Fatalf("Put(%q): %v", alphaKey, err)
	}
	if err := store.Put(ctx, betaKey, strings.NewReader("beta-bytes")); err != nil {
		t.Fatalf("Put(%q): %v", betaKey, err)
	}
	if got := string(readIntegrationBlob(t, ctx, store, alphaKey)); got != "alpha-bytes" {
		t.Errorf("Get(%q) = %q, want %q", alphaKey, got, "alpha-bytes")
	}
	if got := string(readIntegrationBlob(t, ctx, store, betaKey)); got != "beta-bytes" {
		t.Errorf("Get(%q) = %q, want %q", betaKey, got, "beta-bytes")
	}

	for _, listing := range []struct {
		prefix string
		want   []string
	}{
		{"tenants/alpha/", []string{alphaKey}},
		{"tenants/beta/", []string{betaKey}},
		{"", []string{alphaKey, betaKey}},
	} {
		keys, err := store.List(ctx, listing.prefix)
		if err != nil {
			t.Fatalf("List(%q): %v", listing.prefix, err)
		}
		if !reflect.DeepEqual(keys, listing.want) {
			t.Errorf("List(%q) = %v, want exactly %v", listing.prefix, keys, listing.want)
		}
	}

	if err := store.Delete(ctx, alphaKey); err != nil {
		t.Fatalf("Delete(%q): %v", alphaKey, err)
	}
	if got := string(readIntegrationBlob(t, ctx, store, betaKey)); got != "beta-bytes" {
		t.Errorf("Get(%q) after deleting the other tenant's object = %q, want %q", betaKey, got, "beta-bytes")
	}
	keys, err := store.List(ctx, "tenants/beta/")
	if err != nil {
		t.Fatalf(`List("tenants/beta/") after Delete: %v`, err)
	}
	if !reflect.DeepEqual(keys, []string{betaKey}) {
		t.Errorf(`List("tenants/beta/") after Delete = %v, want exactly %v`, keys, []string{betaKey})
	}
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}
