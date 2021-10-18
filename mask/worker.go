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
)

type Stats struct {
	All         uint64
	Problematic uint64
	Success     uint64
}

func (s Stats) String() string {
	return fmt.Sprintf("all %d, success %d, problematic %d, failed %d", s.All, s.Success, s.Problematic, s.Failed())
}

func (s Stats) Failed() uint64 {
	return s.All - s.Problematic - s.Success
}

func (s Stats) PrintSummary() {
	fmt.Printf(`

====Summary====
Success      %d
Problematic  %d
Failed       %d
Total        %d
	`,
		s.Success, s.Problematic, s.Failed(), s.All)
}

type worker struct {
	Stats    Stats
	db       *tidb.Context
	maskFunc MaskFunc
}

func newWorker(db *tidb.Context, maskFunc MaskFunc) *worker {
	return &worker{
		db:       db,
		maskFunc: maskFunc,
	}
}

func (w *worker) replaceValue(node ast.StmtNode) (ast.StmtNode, ExprMap, error) {
	v := NewReplaceVisitor(ReplaceModeValue)
	newNode, _ := node.Accept(v)

	return newNode.(ast.StmtNode), v.OriginExprs, nil
}

func (w *worker) replaceParamMarker(sql string) (ast.StmtNode, []ReplaceMarker, error) {
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

func (w *worker) restore(stmtNode ast.StmtNode, originExprs ExprMap, inferredTypes TypeMap) (string, error) {
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

func (w *worker) infer(stmtNode ast.StmtNode) (TypeMap, error) {
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

func (w *worker) mayExecute(node ast.StmtNode) (bool, error) {
	switch node := node.(type) {
	case *ast.SetStmt, ast.DDLNode:
		_, err := w.db.ExecuteOneStmt(node)
		return true, err

	default:
		return false, nil
	}
}

func (w *worker) maskOneQuery(sql string) (string, error) {
	node, err := w.db.ParseOne(sql)
	if err != nil {
		return "", err
	}

	executed, err := w.mayExecute(node) // todo: add a flag
	if executed {
		if err != nil {
			return "", fmt.Errorf("error when trying to execute `%s`; %w", sql, err)
		} else {
			return sql, nil
		}
	}

	replacedStmtNode, originExprs, err := w.replaceValue(node)
	if err != nil {
		return "", err
	}

	inferredTypes, err := w.infer(replacedStmtNode)
	if err != nil {
		return "", err
	}

	newSQL, err := w.restore(replacedStmtNode, originExprs, inferredTypes)
	if err != nil && newSQL != "" { // problematic
		newSQL = fmt.Sprintf("/* PROBLEMATIC: %v */ %s", err, newSQL)
	}

	return newSQL, err
}
