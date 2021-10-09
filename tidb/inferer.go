package tidb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	plannercore "github.com/pingcap/tidb/planner/core"
)

type Inferer struct {
	db *TiDBInstance
}

func NewInferer(db *TiDBInstance) *Inferer {
	return &Inferer{
		db,
	}
}

func (i *Inferer) replace(sql string) (ast.StmtNode, ExprMap, error) {
	node, err := i.db.ParseOne(sql)
	if err != nil {
		return nil, nil, err
	}
	v := NewReplaceVisitor()
	newNode, _ := node.Accept(v)

	return newNode.(ast.StmtNode), v.OriginExprs, nil
}

func (i *Inferer) restore(stmtNode ast.StmtNode, originExprs ExprMap, targetTypes TypeMap) (string, error) {
	v := NewRestoreVisitor(originExprs, targetTypes, mask.IdenticalMask)
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
	stmtNode, originExprs, err := i.replace(sql)
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

	targetTypes := make(TypeMap)
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

		originDatum := originExprs[int64(f)].Datum
		fmt.Printf("`%s` is %s\n", originDatum.String(), tp.String())
	}

	newSQL, err := i.restore(stmtNode, originExprs, targetTypes)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", newSQL)

	return nil
}
