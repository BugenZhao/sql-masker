package tidb

import (
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
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
		next:         1001,
		OriginDatums: make(map[int64]types.Datum),
	}
}

type ReplaceVisitor struct {
	next         int64
	OriginDatums map[int64]types.Datum
}

func (v *ReplaceVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
	return in, false
}

func (v *ReplaceVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
	if expr, ok := in.(*driver.ValueExpr); ok {
		replacedExpr := ast.NewValueExpr(v.next, "", "")
		v.OriginDatums[v.next] = expr.Datum
		v.next += 1
		return replacedExpr, true
	}
	return in, true
}

func NewRestoreVisitor(originDatums map[int64]types.Datum, targetTypes map[int64]types.EvalType) *RestoreVisitor {
	return &RestoreVisitor{
		originDatums,
		targetTypes,
	}
}

type RestoreVisitor struct {
	originDatums map[int64]types.Datum
	targetTypes  map[int64]types.EvalType
}

func (v *RestoreVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
	return in, false
}

func (v *RestoreVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
	if expr, ok := in.(*driver.ValueExpr); ok {
		i := expr.Datum.GetInt64()
		targetType, ok := v.targetTypes[i]
		if !ok {
			return in, true
		}
		restoredExpr := ast.NewValueExpr(EvalTypeToString(targetType), "", "")
		return restoredExpr, true
	}
	return in, true
}
