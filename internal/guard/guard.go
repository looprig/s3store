// Package guard contains the shared operation policy and typed scaffold errors.
package guard

import (
	"context"
	"strconv"
	"time"
)

// DeadlineRequiredError reports an operation invoked with a nil context, or
// (inside the package, after Bound) an internal step reached without a
// deadline. A caller that merely omits a deadline no longer receives it: Bound
// applies the Store's default operation timeout instead.
type DeadlineRequiredError struct {
	Operation string
}

func (e *DeadlineRequiredError) Error() string {
	return "s3store: operation " + strconv.Quote(e.Operation) + " requires a caller context deadline"
}

// NotImplementedError marks the P2.1/P2.2 seam. It prevents an absent reader,
// empty listing, or no-op mutation from being mistaken for success.
type NotImplementedError struct {
	Operation string
}

func (e *NotImplementedError) Error() string {
	return "s3store: operation " + strconv.Quote(e.Operation) + " is not implemented"
}

// RequireDeadline rejects nil and unbounded contexts before an operation can
// perform SDK or network work.
func RequireDeadline(ctx context.Context, operation string) error {
	if ctx == nil {
		return &DeadlineRequiredError{Operation: operation}
	}
	if _, ok := ctx.Deadline(); !ok {
		return &DeadlineRequiredError{Operation: operation}
	}
	return nil
}

// Bound returns the context an operation runs under. A caller deadline is used
// unchanged, whether shorter or longer than timeout. A context without one
// gets timeout as a child deadline, so the caller's cancellation still reaches
// the operation. The returned cancel must be called when the operation (for
// Get, the returned stream) ends. A nil context is refused.
func Bound(ctx context.Context, operation string, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, &DeadlineRequiredError{Operation: operation}
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}, nil
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	return bounded, cancel, nil
}

// NotImplemented returns the typed scaffold result after boundary guards pass.
func NotImplemented(operation string) error {
	return &NotImplementedError{Operation: operation}
}

// UnconstructedStoreError reports a Store value that was not produced by the
// package constructor and therefore has no transfer gate. It exists so the
// first acquisition fails closed with a typed error instead of blocking
// forever on a nil channel.
type UnconstructedStoreError struct {
	Operation string
}

func (e *UnconstructedStoreError) Error() string {
	return "s3store: operation " + strconv.Quote(e.Operation) +
		" used a Store that was not built by the package constructor"
}
