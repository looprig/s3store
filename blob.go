package s3store

import (
	"context"
	"io"

	"github.com/looprig/s3store/internal/guard"
)

// Put is the P2.1 scaffold for immutable upload. P2.2 adds streaming,
// digest verification, conditional creation, and multipart behavior.
func (s *Store) Put(ctx context.Context, key string, _ io.Reader) error {
	if err := guard.RequireDeadline(ctx, "Blobs.Put"); err != nil {
		return err
	}
	if err := validateBlobKey(key); err != nil {
		return err
	}
	return guard.NotImplemented("Blobs.Put")
}

// Get returns no reader with its typed scaffold error, so absence cannot be
// mistaken for a successful not-found result.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := guard.RequireDeadline(ctx, "Blobs.Get"); err != nil {
		return nil, err
	}
	if err := validateBlobKey(key); err != nil {
		return nil, err
	}
	return nil, guard.NotImplemented("Blobs.Get")
}

// Delete fails closed until P2.2 implements Storage's idempotent delete
// semantics. It never reports a destructive operation as successful here.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := guard.RequireDeadline(ctx, "Blobs.Delete"); err != nil {
		return err
	}
	if err := validateBlobKey(key); err != nil {
		return err
	}
	return guard.NotImplemented("Blobs.Delete")
}

// List returns no keys with its typed scaffold error, preventing an empty
// result from licensing cleanup decisions before listing exists.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	if err := guard.RequireDeadline(ctx, "Blobs.List"); err != nil {
		return nil, err
	}
	if err := validateListPrefix(prefix); err != nil {
		return nil, err
	}
	return nil, guard.NotImplemented("Blobs.List")
}
