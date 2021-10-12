package mask

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type ReplaceMarker int64
type ExprMap = map[ReplaceMarker]*driver.ValueExpr
type ExprOffsetMap = map[ReplaceMarker]int
type TypeMap = map[ReplaceMarker]*types.FieldType

type ReplaceMode int

const (
	ReplaceModeValue ReplaceMode = iota
	ReplaceModeParamMarker
)

const replaceMarkerStep ReplaceMarker = 1000

func (m ReplaceMarker) IntValue() int64 {
	return int64(m)
}

func NewReplaceVisitor(mode ReplaceMode) *ReplaceVisitor {
	return &ReplaceVisitor{
		mode:        mode,
		next:        1001,
		OriginExprs: make(ExprMap),
	}
}

type ReplaceVisitor struct {
	mode        ReplaceMode
	next        ReplaceMarker
	OriginExprs ExprMap
	Offsets     ExprOffsetMap
}

func (v *ReplaceVisitor) nextMarker() ReplaceMarker {
	n := v.next
	v.next += replaceMarkerStep
	return n
}

func (v *ReplaceVisitor) Enter(in ast.Node) (node ast.Node, skipChilren bool) {
	return in, false
}

func (v *ReplaceVisitor) Leave(in ast.Node) (node ast.Node, ok bool) {
	switch v.mode {
	case ReplaceModeValue:
		if expr, ok := in.(*driver.ValueExpr); ok {
			n := v.nextMarker()
			replacedExpr := ast.NewValueExpr(n.IntValue(), "", "")
			v.OriginExprs[n] = expr
			return replacedExpr, true
		}
	case ReplaceModeParamMarker:
		if expr, ok := in.(*driver.ParamMarkerExpr); ok {
			n := v.nextMarker()
			replacedExpr := ast.NewValueExpr(n.IntValue(), "", "")
			v.Offsets[n] = expr.Offset
			return replacedExpr, true
		}
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
		i := ReplaceMarker(expr.Datum.GetInt64())
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

		maskedDatum, maskedType, err := ConvertAndMask(v.stmtContext, originExpr.Datum, inferredType, v.maskFunc)
		if err != nil {
			v.appendError(err)
			return originExpr, false
		}

		restoredExpr := ast.NewValueExpr(maskedDatum.GetValue(), "", "")
		restoredExpr.SetType(maskedType)
		return restoredExpr, true
	}
	return in, true
}
