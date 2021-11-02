package mask

// import (
// 	"fmt"

// 	"github.com/pingcap/parser/ast"
// )

// type ColVisitor struct {
// }

// func (v *ColVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
// 	if _, ok := in.(*ast.SelectStmt); ok {
// 		fmt.Println("select")
// 	}
// 	if col, ok := in.(*ast.ColumnNameExpr); ok {
// 		fmt.Println("col name expr", col.Name.String())
// 	}
// 	if tab, ok := in.(*ast.TableName); ok {
// 		fmt.Printf("table name %+v\n", tab)
// 	}
// 	return in, false
// }

// func (v *ColVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
// 	return in, true
// }
