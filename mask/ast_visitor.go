package mask

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type ExprMap = map[int64]*driver.ValueExpr
type TypeMap = map[int64]*types.FieldType

func NewReplaceVisitor() *ReplaceVisitor {
	return &ReplaceVisitor{
		next:        1001,
		OriginExprs: make(ExprMap),
	}
}

var replaceMarkerStep int64 = 1000

type ReplaceVisitor struct {
	next        int64
	OriginExprs ExprMap
}

func (v *ReplaceVisitor) nextMarker() int64 {
	n := v.next
	v.next += replaceMarkerStep
	return n
}

func (v *ReplaceVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
	return in, false
}

func (v *ReplaceVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
	if expr, ok := in.(*driver.ValueExpr); ok {
		n := v.nextMarker()
		replacedExpr := ast.NewValueExpr(n, "", "")
		v.OriginExprs[n] = expr
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
		v.err = fmt.Errorf("%w; %v", v.err, err)
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
			guessI := i*2 + replaceMarkerStep // hack for handle `a + b`
			guessedType, ok := v.inferredTypes[guessI]
			if ok {
				v.appendError(fmt.Errorf("type for `%v` is guessed", originExpr.Datum))
				inferredType = guessedType
			} else {
				v.appendError(fmt.Errorf("type for `%v` not inferred", originExpr.Datum))
				return originExpr, true
			}
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
