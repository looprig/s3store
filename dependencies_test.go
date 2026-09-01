package s3store

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"io/fs"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type goModEditJSON struct {
	Require []struct {
		Path     string
		Version  string
		Indirect bool
	}
	Replace []json.RawMessage
}

func TestDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "mod", "edit", "-json")
	command.Env = append(command.Environ(), "GOWORK=off")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go mod edit -json: %v", err)
	}
	var mod goModEditJSON
	if err := json.Unmarshal(output, &mod); err != nil {
		t.Fatalf("decode go.mod: %v", err)
	}
	if len(mod.Replace) != 0 {
		t.Fatalf("go.mod has %d replace directives, want none", len(mod.Replace))
	}
	var direct []string
	versions := make(map[string]string)
	for _, requirement := range mod.Require {
		versions[requirement.Path] = requirement.Version
		if !requirement.Indirect {
			direct = append(direct, requirement.Path)
		}
	}
	slices.Sort(direct)
	wantDirect := []string{
		"github.com/aws/aws-sdk-go-v2",
		"github.com/aws/aws-sdk-go-v2/config",
		"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager",
		"github.com/aws/aws-sdk-go-v2/service/s3",
		"github.com/looprig/storage",
	}
	if !slices.Equal(direct, wantDirect) {
		t.Fatalf("direct modules = %v, want %v", direct, wantDirect)
	}
	if versions["github.com/looprig/storage"] != "v0.6.0" {
		t.Errorf("Storage version = %q, want v0.6.0", versions["github.com/looprig/storage"])
	}
	if versions["github.com/aws/aws-sdk-go-v2/credentials"] == "" {
		t.Error("AWS credentials module is not pinned")
	}

	parsed := 0
	err = filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && entry.Name() == ".git" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		parsed++
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if importPath == "log" || importPath == "log/slog" {
				t.Errorf("%s imports logging package %q; credentials and presigned URLs must not reach logs", path, importPath)
			}
			if strings.Contains(importPath, ".") &&
				!strings.HasPrefix(importPath, "github.com/aws/aws-sdk-go-v2") &&
				importPath != "github.com/looprig/storage" &&
				!strings.HasPrefix(importPath, "github.com/looprig/s3store/internal/") {
				t.Errorf("%s imports unapproved package %q", path, importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production files: %v", err)
	}
	if parsed == 0 {
		t.Fatal("no production Go files parsed")
	}
}
