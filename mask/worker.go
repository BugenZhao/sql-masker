package mask

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	ptypes "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/zyguan/mysql-replay/event"
)

type Prepared struct {
	sql           string
	typeMap       TypeMap
	sortedMarkers []ReplaceMarker
}

type PreparedMap = map[uint64]Prepared

type Worker struct {
	db            *tidb.Instance
	maskFunc      MaskFunc
	preparedStmts PreparedMap
	All           uint64
	Problematic   uint64
	Success       uint64
}

func NewWorker(db *tidb.Instance, maskFunc MaskFunc) *Worker {
	return &Worker{
		db:            db,
		maskFunc:      maskFunc,
		preparedStmts: make(PreparedMap),
	}
}

func (w *Worker) replaceValue(sql string) (ast.StmtNode, ExprMap, error) {
	node, err := w.db.ParseOne(sql)
	if err != nil {
		return nil, nil, err
	}
	v := NewReplaceVisitor(ReplaceModeValue)
	newNode, _ := node.Accept(v)

	return newNode.(ast.StmtNode), v.OriginExprs, nil
}

func (w *Worker) replaceParamMarker(sql string) (ast.StmtNode, []ReplaceMarker, error) {
	node, err := w.db.ParseOne(sql)
	if err != nil {
		return nil, nil, err
	}
	v := NewReplaceVisitor(ReplaceModeParamMarker)
	newNode, _ := node.Accept(v)

	markers := make([]ReplaceMarker, 0, len(v.Offsets))
	for k := range v.Offsets {
		markers = append(markers, k)
	}
	sort.Slice(markers, func(i, j int) bool { return v.Offsets[markers[i]] < v.Offsets[markers[j]] })

	return newNode.(ast.StmtNode), markers, nil
}

func (w *Worker) restore(stmtNode ast.StmtNode, originExprs ExprMap, inferredTypes TypeMap) (string, error) {
	v := NewRestoreVisitor(originExprs, inferredTypes, w.maskFunc)
	newNode, ok := stmtNode.Accept(v)
	if !ok {
		return "", v.err
	}

	buf := &strings.Builder{}
	restoreFlags := format.DefaultRestoreFlags | format.RestoreStringWithoutDefaultCharset
	restoreCtx := format.NewRestoreCtx(restoreFlags, buf)
	err := newNode.Restore(restoreCtx)
	if err != nil {
		return "", err
	}

	newSQL := buf.String()
	return newSQL, v.err
}

func (w *Worker) infer(stmtNode ast.StmtNode) (TypeMap, error) {
	execStmt, err := w.db.CompileStmtNode(stmtNode)
	if err != nil {
		return nil, err
	}

	b := NewCastGraphBuilder()
	err = b.Build(execStmt.Plan)
	if err != nil {
		return nil, err
	}

	inferredTypes := make(TypeMap)
	for _, c := range b.Constants {
		tp := b.Graph.InferType(c)

		s, err := c.Value.ToString()
		if err != nil {
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			continue
		}
		inferredTypes[ReplaceMarker(f)] = tp
	}
	for _, h := range b.Handles {
		switch h := h.(type) {
		case kv.IntHandle:
			inferredTypes[ReplaceMarker(h.IntValue())] = ptypes.NewFieldType(mysql.TypeLong) // todo: which tp ?
		default:
			// ignore common handle for clustered index
		}
	}

	return inferredTypes, nil
}

func (w *Worker) MaskOneQuery(sql string) (string, error) {
	w.All += 1

	replacedStmtNode, originExprs, err := w.replaceValue(sql)
	if err != nil {
		return sql, err
	}

	inferredTypes, err := w.infer(replacedStmtNode)
	if err != nil {
		return sql, err
	}

	newSQL, err := w.restore(replacedStmtNode, originExprs, inferredTypes)
	if err != nil {
		if newSQL == "" {
			return sql, err
		} else {
			w.Problematic += 1
			return newSQL, err
		}
	}

	w.Success += 1
	return newSQL, nil
}

func (w *Worker) PrepareOne(stmtID uint64, sql string) error {
	replacedStmtNode, sortedMarkers, err := w.replaceParamMarker(sql)
	if err != nil {
		return err
	}
	inferredTypes, err := w.infer(replacedStmtNode)
	if err != nil {
		return err
	}

	w.preparedStmts[stmtID] = Prepared{
		sql, inferredTypes, sortedMarkers,
	}
	return nil
}

func (w *Worker) MaskOneExecute(stmtID uint64, params []interface{}) ([]interface{}, error) {
	p, ok := w.preparedStmts[stmtID]
	if !ok {
		return params, fmt.Errorf("no prepared query found for stmt id `%d`", stmtID)
	}

	if len(p.sortedMarkers) != len(params) {
		return params, fmt.Errorf("mismatched length of inferred markers and params for stmt `%s`", p.sql)
	}

	sc := &stmtctx.StatementContext{}
	maskedParams := []interface{}{}

	for i, param := range params {
		originDatum := types.NewDatum(param)

		marker := p.sortedMarkers[i]
		possibleMarkers := []ReplaceMarker{
			marker,
			marker - 1,
			marker + 1,
		}

		var tp *types.FieldType
		for _, marker := range possibleMarkers {
			tp, ok = p.typeMap[marker]
			if ok {
				break
			}
		}
		if tp == nil {
			return params, fmt.Errorf("type for `%v` not inferred", originDatum)
		}

		maskedDatum, _, err := ConvertAndMask(sc, originDatum, tp, w.maskFunc)
		if err != nil {
			return params, err
		}
		maskedParams = append(maskedParams, maskedDatum.GetValue())
	}

	return maskedParams, nil
}

func (w *Worker) MaskEvent(ev event.MySQLEvent) (event.MySQLEvent, error) {
	switch ev.Type {
	case event.EventHandshake:
		w.preparedStmts = make(PreparedMap)

	case event.EventQuery:
		maskedQuery, err := w.MaskOneQuery(ev.Query)
		if err != nil {
			return ev, err
		}
		ev.Query = maskedQuery

	case event.EventStmtPrepare:
		err := w.PrepareOne(ev.StmtID, ev.Query)
		if err != nil {
			return ev, err
		}

	case event.EventStmtExecute:
		maskedParams, err := w.MaskOneExecute(ev.StmtID, ev.Params)
		if err != nil {
			return ev, err
		}
		ev.Params = maskedParams

	default:
	}

	return ev, nil
}
