package mask

import (
	"fmt"
	"strings"

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

func NewRestoreVisitor(originExprs ExprMap, inferredTypes TypeMap, maskFunc MaskFunc) *RestoreVisitor {
	sc := stmtctx.StatementContext{}
	sc.IgnoreTruncate = true // todo: what's this ?

	return &RestoreVisitor{
		originExprs,
		inferredTypes,
		&sc,
		maskFunc,
		nil,
	}
}

type RestoreVisitor struct {
	originExprs   ExprMap
	inferredTypes TypeMap
	stmtContext   *stmtctx.StatementContext
	maskFunc      MaskFunc
	err           error
}

func (v *RestoreVisitor) appendError(err error) {
	if v.err == nil {
		v.err = err
	} else {
		v.err = fmt.Errorf("%v; %w", err, v.err)
	}
}

func (v *RestoreVisitor) Enter(in ast.Node) (_ ast.Node, skipChilren bool) {
	return in, false
}

func (v *RestoreVisitor) Leave(in ast.Node) (_ ast.Node, ok bool) {
	if expr, ok := in.(*driver.ValueExpr); ok {
		i := expr.Datum.GetInt64()
		originExpr, ok := v.originExprs[i]
		if !ok {
			v.appendError(fmt.Errorf("no replace record found for `%v`", expr.Datum))
			return in, false
		}
		inferredType, ok := v.inferredTypes[i]
		if !ok {
			v.appendError(fmt.Errorf("type for `%v` not inferred", originExpr.Datum))
			return originExpr, true
		}
		castedDatum, err := originExpr.Datum.ConvertTo(v.stmtContext, inferredType)
		if err != nil {
			v.appendError(fmt.Errorf("cannot cast `%v` to type `%v`; %w", originExpr.Datum, inferredType, err))
			return originExpr, false
		}

		maskedDatum, maskedType, err := v.maskFunc(castedDatum, inferredType)
		if err != nil {
			v.appendError(fmt.Errorf("failed to mask `%v`; %w", castedDatum, err))
			return originExpr, false
		}
		if maskedType == nil {
			maskedType = inferredType
		}

		restoredExpr := ast.NewValueExpr(maskedDatum.GetValue(), "", "")
		restoredExpr.SetType(maskedType)
		return restoredExpr, true
	}
	return in, true
}
