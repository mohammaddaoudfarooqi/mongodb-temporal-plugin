package tools

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestTxnCallbacks_DoNotTranslate guards the rule that a WithTxn callback
// must return driver errors untranslated (use internalmongo.Wrap) so the
// driver's retry loop can read TransientTransactionError. Translating
// inside the callback replaces the labelled error with a serviceerror and
// silently disables transaction retries.
func TestTxnCallbacks_DoNotTranslate(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal")
	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isWithTxnCall(call.Fun) {
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.FuncLit)
				if !ok {
					continue
				}
				ast.Inspect(lit.Body, func(m ast.Node) bool {
					if c, ok := m.(*ast.CallExpr); ok && isSelector(c.Fun, "Translate") {
						violations = append(violations, fset.Position(c.Pos()).String())
					}
					return true
				})
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range violations {
		t.Errorf("Translate inside WithTxn callback at %s; use internalmongo.Wrap", v)
	}
}

// isWithTxnCall matches WithTxn[T](...) and internalmongo.WithTxn[T](...).
func isWithTxnCall(fun ast.Expr) bool {
	if idx, ok := fun.(*ast.IndexExpr); ok {
		fun = idx.X
	}
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name == "WithTxn"
	case *ast.SelectorExpr:
		return f.Sel.Name == "WithTxn"
	}
	return false
}

func isSelector(fun ast.Expr, name string) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name == name
	case *ast.SelectorExpr:
		return f.Sel.Name == name
	}
	return false
}
