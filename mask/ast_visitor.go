package mask

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type InferredType struct {
	Ft *types.FieldType
}

func NewIntHandleInferredType() *InferredType {
	ft := types.NewFieldType(mysql.TypeLonglong) // todo: is this type ok?
	ft.Flag |= mysql.PriKeyFlag
	return NewInferredType(ft)
}

func NewInferredType(ft *types.FieldType) *InferredType {
	return &InferredType{
		ft,
	}
}

func (it InferredType) IsPrimaryKey() bool {
	return mysql.HasPriKeyFlag(it.Ft.Flag)
}

type ReplaceMarker int64
type ExprMap = map[ReplaceMarker]*driver.ValueExpr
type ExprOffsetMap = map[ReplaceMarker]int
type TypeMap = map[ReplaceMarker]*InferredType

type ReplaceMode int

const (
	ReplaceModeValue ReplaceMode = iota
	ReplaceModeParamMarker
)

const replaceMarkerStep ReplaceMarker = 1000

func (m ReplaceMarker) IntValue() int64 {
	return int64(m)
}

func isCountOne(in *ast.AggregateFuncExpr) bool {
	if in.F == ast.AggFuncCount && len(in.Args) == 1 {
		arg := in.Args[0]
		if expr, ok := arg.(*driver.ValueExpr); ok {
			return expr.Datum.GetInt64() == 1 && expr.Datum.Kind() == types.KindInt64
		}
	}
	return false
}

func enterMayIgnoreSubtree(in ast.Node) (node ast.Node, skipChilren bool) {
	switch in := in.(type) {
	case *ast.Limit:
		return in, true
	case *ast.AggregateFuncExpr:
		return in, isCountOne(in)
	default:
		return in, false
	}
}

func NewReplaceVisitor(mode ReplaceMode) *ReplaceVisitor {
	return &ReplaceVisitor{
		mode:        mode,
		next:        replaceMarkerStep + 1,
		OriginExprs: make(ExprMap),
		Offsets:     make(ExprOffsetMap),
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
	return enterMayIgnoreSubtree(in)
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
		if _, ok := in.(*driver.ValueExpr); ok {
			// HACK: replace all constants with `1` for better inference even after plan rewriting
			//       this is ok since we do not restore `PREPARE` statements
			replacedExpr := ast.NewValueExpr(1, "", "")
			return replacedExpr, true
		}
	}

	return in, true
}

type RestoreMode int

const (
	RestoreModeNameValue RestoreMode = iota
	RestoreModeNameOnly
)

func NewRestoreVisitor(originExprs ExprMap, inferredTypes TypeMap, maskFunc MaskFunc, nameMap *NameMap, ignoreIntPK bool) *RestoreVisitor {
	sc := stmtctx.StatementContext{}
	sc.IgnoreTruncate = true // todo: what's this ?

	return &RestoreVisitor{
		mode:          RestoreModeNameValue,
		originExprs:   originExprs,
		inferredTypes: inferredTypes,
		stmtContext:   &sc,
		maskFunc:      maskFunc,
		nameMap:       nameMap,
		ignoreIntPK:   ignoreIntPK,
		success:       0,
		err:           nil,
	}
}

func NewNameOnlyRestoreVisitor(nameMap *NameMap) *RestoreVisitor {
	sc := stmtctx.StatementContext{}
	sc.IgnoreTruncate = true // todo: what's this ?

	return &RestoreVisitor{
		mode:        RestoreModeNameOnly,
		stmtContext: &sc,
		nameMap:     nameMap,
		success:     0,
		err:         nil,
	}
}

type RestoreVisitor struct {
	mode          RestoreMode
	originExprs   ExprMap
	inferredTypes TypeMap
	stmtContext   *stmtctx.StatementContext
	maskFunc      MaskFunc
	nameMap       *NameMap
	ignoreIntPK   bool
	success       int
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
	return enterMayIgnoreSubtree(in)
}

func (v *RestoreVisitor) Leave(in ast.Node) (_ ast.Node, ok bool) {
	if v.nameMap != nil {
		if col, ok := in.(*ast.ColumnName); ok {
			col = v.nameMap.ColumnName(col)
			return col, true
		}
		if tab, ok := in.(*ast.TableName); ok {
			tab = v.nameMap.TableName(tab)
			return tab, true
		}
		if hint, ok := in.(*ast.TableOptimizerHint); ok {
			newHintTables := []ast.HintTable{}
			for _, table := range hint.Tables {
				table.DBName = model.NewCIStr(v.nameMap.DB(table.DBName.L))
				table.TableName = model.NewCIStr(v.nameMap.Table(table.TableName.L))
				newHintTables = append(newHintTables, table)
			}
			hint.Tables = newHintTables
			return hint, true
		}
	}
	if v.mode == RestoreModeNameOnly {
		return in, true
	}

	if expr, ok := in.(*driver.ValueExpr); ok {
		m := ReplaceMarker(expr.Datum.GetInt64())
		originExpr, ok := v.originExprs[m]
		if !ok {
			v.appendError(fmt.Errorf("no replace record found for `%v`", expr.Datum))
			return in, false
		}
		inferredType, ok := v.inferredTypes[m]
		if !ok {
			// // DIRTY HACK: handle `a + b`
			// guessI := m*2 + replaceMarkerStep
			// guessedType, ok := v.inferredTypes[guessI]
			// if ok {
			// 	v.appendError(fmt.Errorf("type for `%v` is guessed", originExpr.Datum))
			// 	inferredType = guessedType
			// } else {
			v.appendError(fmt.Errorf("type for `%v` not inferred", originExpr.Datum))
			return originExpr, true
			// }
		}

		var maskedDatum types.Datum
		var maskedType *types.FieldType
		var err error

		if inferredType.IsPrimaryKey() && v.ignoreIntPK {
			// use original datum if int pk is ignored
			maskedDatum, maskedType = originExpr.Datum, &originExpr.Type
		} else {
			maskedDatum, maskedType, err = ConvertAndMask(v.stmtContext, originExpr.Datum, inferredType.Ft, v.maskFunc)
		}

		if err != nil {
			v.appendError(err)
			return originExpr, false
		}

		restoredExpr := ast.NewValueExpr(maskedDatum.GetValue(), "", "")
		restoredExpr.SetType(maskedType)
		v.success += 1
		return restoredExpr, true
	}

	return in, true
}
