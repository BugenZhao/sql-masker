package tidb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
)

type Inferer struct {
	db *TiDBInstance
}

func NewInferer(db *TiDBInstance) *Inferer {
	return &Inferer{
		db,
	}
}

func (i *Inferer) replace(sql string) (ast.StmtNode, map[int64]types.Datum, error) {
	node, err := i.db.ParseOne(sql)
	if err != nil {
		return nil, nil, err
	}
	v := NewReplaceVisitor()
	newNode, _ := node.Accept(v)

	return newNode.(ast.StmtNode), v.OriginDatums, nil
}

func (i *Inferer) restore(stmtNode ast.StmtNode, originDatums map[int64]types.Datum, targetTypes map[int64]types.EvalType) (string, error) {
	v := NewRestoreVisitor(originDatums, targetTypes)
	newNode, _ := stmtNode.Accept(v)

	buf := &strings.Builder{}
	restoreFlags := format.DefaultRestoreFlags | format.RestoreStringWithoutDefaultCharset
	restoreCtx := format.NewRestoreCtx(restoreFlags, buf)
	err := newNode.Restore(restoreCtx)
	if err != nil {
		return "", err
	}

	newSQL := buf.String()
	return newSQL, nil
}

func (i *Inferer) Infer(sql string) error {
	stmtNode, originDatums, err := i.replace(sql)
	if err != nil {
		return err
	}
	execStmt, err := i.db.CompileStmtNode(stmtNode)
	if err != nil {
		return err
	}
	plan, ok := execStmt.Plan.(plannercore.PhysicalPlan)
	if !ok {
		return fmt.Errorf("not a physical plan for sql `%s`", sql)
	}

	v := NewPlanVisitor()
	v.Visit(plan)

	targetTypes := make(map[int64]types.EvalType)
	for _, c := range v.Constants {
		tp := v.Graph.InferType(c)

		s, err := c.Value.ToString()
		if err != nil {
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			continue
		}
		targetTypes[int64(f)] = tp

		originDatum := originDatums[int64(f)]
		fmt.Printf("`%s` is %s\n", originDatum.String(), EvalTypeToString(tp))
	}

	newSQL, err := i.restore(stmtNode, originDatums, targetTypes)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", newSQL)

	return nil
}
