package neutroncli

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
)

// typeCheckGoSource type-checks a standalone generated file with real stdlib
// imports resolved, catching the "undefined: json" class the NA-10 guard
// exists for (a generated struct that references json.RawMessage without the
// import fails here exactly as it would under the compiler).
func typeCheckGoSource(src string) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "generated.go", src, parser.AllErrors)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	var firstErr error
	conf := types.Config{
		Importer: importer.ForCompiler(fset, "source", nil),
		Error: func(err error) {
			if firstErr == nil {
				firstErr = err
			}
		},
	}
	_, _ = conf.Check("model", fset, []*ast.File{file}, nil)
	return firstErr
}
