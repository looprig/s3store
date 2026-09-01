package s3store

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestStoreIsBuiltOnlyByItsConstructor enforces the invariant that makes the
// transfer gate usable: a Store built anywhere but newStore has a nil
// transferSlots channel, and a send on a nil channel is a silent permanent
// hang rather than an error.
//
// The guard covers every way to obtain a zero-value Store, not just composite
// literals: new(Store) and a var declaration of Store produce exactly the same
// nil channel and would otherwise walk straight past a literal-only check.
// TestAcquireTransferFailsClosedOnUnconstructedStore is the one sanctioned
// exception, because building an unconstructed Store is its whole subject.
func TestStoreIsBuiltOnlyByItsConstructor(t *testing.T) {
	t.Parallel()
	sanctioned := map[string]bool{
		"newStore": true,
		"TestAcquireTransferFailsClosedOnUnconstructedStore": true,
	}
	fileSet := token.NewFileSet()
	constructions := 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && entry.Name() == ".git" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				form := storeConstructionForm(node)
				if form == "" {
					return true
				}
				constructions++
				if !sanctioned[function.Name.Name] {
					position := fileSet.Position(node.Pos())
					t.Errorf("%s:%d %s builds an unconstructed Store via %s; only newStore may, or transferSlots is nil and the gate hangs",
						path, position.Line, function.Name.Name, form)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk package files: %v", err)
	}
	if constructions != 2 {
		t.Fatalf("direct Store constructions = %d, want exactly newStore's literal and the unconstructed-Store fixture", constructions)
	}
}

// storeConstructionForm names the syntax by which node obtains a Store value
// directly, or returns "" if it does not. A *Store variable is not a
// construction: it is nil until something assigns to it.
func storeConstructionForm(node ast.Node) string {
	isStore := func(expr ast.Expr) bool {
		identifier, ok := expr.(*ast.Ident)
		return ok && identifier.Name == "Store"
	}
	switch typed := node.(type) {
	case *ast.CompositeLit:
		if isStore(typed.Type) {
			return "a composite literal"
		}
	case *ast.CallExpr:
		callee, ok := typed.Fun.(*ast.Ident)
		if ok && callee.Name == "new" && len(typed.Args) == 1 && isStore(typed.Args[0]) {
			return "new(Store)"
		}
	case *ast.ValueSpec:
		if typed.Type != nil && isStore(typed.Type) {
			return "a var declaration"
		}
	}
	return ""
}

func TestAcquireTransferRequiresDeadline(t *testing.T) {
	t.Parallel()
	store := newStore(nil, nil, resolvedOptions{maxConcurrentTransfers: 1})
	err := store.acquireTransfer(context.Background(), "Blobs.Put")
	var deadlineErr *DeadlineRequiredError
	if !errors.As(err, &deadlineErr) {
		t.Fatalf("acquireTransfer error = %T %v, want *DeadlineRequiredError", err, err)
	}
}

// TestAcquireTransferFailsClosedOnUnconstructedStore is the hang guard: it
// must return, and it must return an error rather than block on a nil channel.
func TestAcquireTransferFailsClosedOnUnconstructedStore(t *testing.T) {
	t.Parallel()
	unconstructed := new(Store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- unconstructed.acquireTransfer(ctx, "Blobs.Put") }()
	select {
	case err := <-done:
		var unconstructedErr *UnconstructedStoreError
		if !errors.As(err, &unconstructedErr) {
			t.Fatalf("acquireTransfer error = %T %v, want *UnconstructedStoreError", err, err)
		}
		if unconstructedErr.Operation != "Blobs.Put" {
			t.Errorf("operation = %q, want %q", unconstructedErr.Operation, "Blobs.Put")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquireTransfer blocked on a nil transferSlots channel instead of failing closed")
	}
}

func TestAcquireTransferBoundsConcurrentTransfers(t *testing.T) {
	t.Parallel()
	const slots = 2
	store := newStore(nil, nil, resolvedOptions{maxConcurrentTransfers: slots})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < slots; i++ {
		if err := store.acquireTransfer(ctx, "Blobs.Put"); err != nil {
			t.Fatalf("acquireTransfer %d: %v", i, err)
		}
	}

	// The gate is full. The next acquisition must wait and then surface the
	// caller's context error, never an unbounded hang.
	saturated, cancelSaturated := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelSaturated()
	blocked := make(chan error, 1)
	go func() { blocked <- store.acquireTransfer(saturated, "Blobs.Put") }()
	select {
	case err := <-blocked:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("saturated acquireTransfer error = %T %v, want context.DeadlineExceeded", err, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("saturated acquireTransfer ignored ctx.Done and hung")
	}

	// Releasing one slot admits exactly one more transfer.
	store.releaseTransfer()
	if err := store.acquireTransfer(ctx, "Blobs.Put"); err != nil {
		t.Fatalf("acquireTransfer after release: %v", err)
	}
	if length := len(store.transferSlots); length != slots {
		t.Errorf("held slots = %d, want %d", length, slots)
	}
}
