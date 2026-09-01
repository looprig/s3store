package s3store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"testing"
)

func TestBlobOperationMethodsCallScaffoldGuards(t *testing.T) {
	t.Parallel()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "blob.go", nil, 0)
	if err != nil {
		t.Fatalf("parse blob.go: %v", err)
	}
	imports := importNames(t, file)
	operations := 0
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil || !function.Name.IsExported() || function.Body == nil || !firstParameterIsContext(function, imports) {
			continue
		}
		operations++
		for _, guardName := range []string{"RequireDeadline", "NotImplemented"} {
			if !callsGuard(function.Body, guardName) {
				position := fileSet.Position(function.Pos())
				t.Errorf("blob.go:%d %s does not call guard.%s", position.Line, function.Name.Name, guardName)
			}
		}
	}
	if operations != 4 {
		t.Fatalf("exported context-taking blob operations = %d, want 4", operations)
	}
}

func importNames(t *testing.T, file *ast.File) map[string]string {
	t.Helper()
	imports := make(map[string]string)
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("unquote import: %v", err)
		}
		name := filepath.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = path
	}
	return imports
}

func firstParameterIsContext(function *ast.FuncDecl, imports map[string]string) bool {
	if function.Type.Params == nil || len(function.Type.Params.List) == 0 {
		return false
	}
	selector, ok := function.Type.Params.List[0].Type.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Context" {
		return false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	return ok && imports[qualifier.Name] == "context"
}

func callsGuard(body *ast.BlockStmt, method string) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != method {
			return true
		}
		qualifier, ok := selector.X.(*ast.Ident)
		if ok && qualifier.Name == "guard" {
			found = true
		}
		return true
	})
	return found
}
