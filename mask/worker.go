package mask

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	plannercore "github.com/pingcap/tidb/planner/core"
)

type Worker struct {
	db          *tidb.Instance
	maskFunc    MaskFunc
	All         uint64
	Problematic uint64
	Success     uint64
}

func NewWorker(db *tidb.Instance, maskFunc MaskFunc) *Worker {
	return &Worker{
		db:       db,
		maskFunc: maskFunc,
	}
}

func (w *Worker) replace(sql string) (ast.StmtNode, ExprMap, error) {
	node, err := w.db.ParseOne(sql)
	if err != nil {
		return nil, nil, err
	}
	v := NewReplaceVisitor()
	newNode, _ := node.Accept(v)

	return newNode.(ast.StmtNode), v.OriginExprs, nil
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
	switch plan := execStmt.Plan.(type) {
	case plannercore.PhysicalPlan:
		b.Visit(plan)
	case *plannercore.Update:
		b.VisitUpdate(*plan)
	case *plannercore.Delete:
		b.VisitDelete(*plan)
	default:
		return nil, fmt.Errorf("unrecognized plan `%T` :(", plan)
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
		inferredTypes[int64(f)] = tp
	}

	return inferredTypes, nil
}

func (w *Worker) MaskOne(sql string) (string, error) {
	w.All += 1

	replacedStmtNode, originExprs, err := w.replace(sql)
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
