package tidb

import (
	"fmt"
	"strings"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
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

type ExprMap = map[int64]*driver.ValueExpr
type TypeMap = map[int64]*types.FieldType

func NewReplaceVisitor() *ReplaceVisitor {
	return &ReplaceVisitor{
		next:        1001,
		OriginExprs: make(ExprMap),
	}
}

type ReplaceVisitor struct {
	next        int64
	OriginExprs ExprMap
}

func (v *ReplaceVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
	return in, false
}

func (v *ReplaceVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
	if expr, ok := in.(*driver.ValueExpr); ok {
		replacedExpr := ast.NewValueExpr(v.next, "", "")
		v.OriginExprs[v.next] = expr
		v.next += 1
		return replacedExpr, true
	}
	return in, true
}

func NewRestoreVisitor(originExprs ExprMap, targetTypes TypeMap, maskFunc mask.MaskFunc) *RestoreVisitor {
	sc := stmtctx.StatementContext{}
	sc.IgnoreTruncate = true // todo: what's this ?

	return &RestoreVisitor{
		originExprs,
		targetTypes,
		&sc,
		maskFunc,
	}
}

type RestoreVisitor struct {
	originExprs ExprMap
	targetTypes TypeMap
	stmtContext *stmtctx.StatementContext
	maskFunc    mask.MaskFunc
}

func (v *RestoreVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
	return in, false
}

func (v *RestoreVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
	if expr, ok := in.(*driver.ValueExpr); ok {
		i := expr.Datum.GetInt64()
		originExpr := v.originExprs[i]
		targetType, ok := v.targetTypes[i]
		if !ok {
			return originExpr, true
		}
		castedDatum, err := originExpr.Datum.ConvertTo(v.stmtContext, targetType)
		if err != nil {
			return originExpr, true
		}

		maskedDatum, err := v.maskFunc(castedDatum, targetType)
		if err != nil {
			return originExpr, true
		}

		restoredExpr := ast.NewValueExpr(maskedDatum.GetValue(), "", "")
		restoredExpr.SetType(targetType)
		return restoredExpr, true
	}
	return in, true
}
