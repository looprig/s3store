package s3store

import (
	"context"

	"github.com/looprig/s3store/internal/guard"
)

// acquireTransfer reserves one Store-wide transfer slot and is the contract
// P2.2 must honour before starting any SDK transfer:
//
//   - it requires a caller deadline, like every other operation;
//   - it fails closed with *UnconstructedStoreError on a Store that did not
//     come from newStore, rather than blocking forever on a nil channel;
//   - it waits in a select against ctx.Done(), so a saturated gate surfaces as
//     the caller's context error and never as an unbounded hang.
//
// Every successful acquireTransfer must be paired with a deferred
// releaseTransfer. Without this pairing the MaxConcurrentTransfers factor in
// the configuration-time transfer-memory arithmetic has no runtime meaning.
func (s *Store) acquireTransfer(ctx context.Context, operation string) error {
	if err := guard.RequireDeadline(ctx, operation); err != nil {
		return err
	}
	if s.transferSlots == nil {
		return &guard.UnconstructedStoreError{Operation: operation}
	}
	select {
	case s.transferSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseTransfer returns a slot taken by a successful acquireTransfer. It is
// never called on an unconstructed Store because acquireTransfer fails there.
func (s *Store) releaseTransfer() {
	<-s.transferSlots
}
