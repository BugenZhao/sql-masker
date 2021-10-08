package tidb

import (
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type DebugVisitor struct {
	depth    int
	colNames []string
}

func (v *DebugVisitor) Log(in ast.Node) {
	indent := strings.Repeat("  ", v.depth)
	fmt.Printf("%v%T: %v, %v\n", indent, in, in.Text(), in)
}

func (v *DebugVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
	v.depth += 1
	v.Log(in)
	if name, ok := in.(*ast.ColumnName); ok {
		v.colNames = append(v.colNames, name.OrigColName())
	}
	return in, false
}

func (v *DebugVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
	v.depth -= 1
	return in, true
}

func NewReplaceVisitor() *ReplaceVisitor {
	return &ReplaceVisitor{
		next: 1000001,
	}
}

type ReplaceVisitor struct {
	next int
}

func (v *ReplaceVisitor) nextValueExpr() ast.ValueExpr {
	expr := ast.NewValueExpr(v.next, "", "")
	v.next += 1
	return expr
}

func (v *ReplaceVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
	return in, false
}

func (v *ReplaceVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
	if _, ok := in.(*driver.ValueExpr); ok {
		replacedExpr := v.nextValueExpr()
		return replacedExpr, true
	}
	return in, true
}
