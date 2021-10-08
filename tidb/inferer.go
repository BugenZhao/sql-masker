package tidb

import (
	"fmt"
	"strings"

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

func (i *Inferer) replace(sql string) (string, error) {
	v := NewReplaceVisitor()

	node, err := i.db.ParseOne(sql)
	if err != nil {
		return sql, err
	}
	newNode, _ := node.Accept(v)

	buf := &strings.Builder{}
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutDefaultCharset, buf)
	err = newNode.Restore(restoreCtx)
	if err != nil {
		return sql, err
	}

	newSQL := buf.String()
	fmt.Println(newSQL)

	return newSQL, nil
}

func (i *Inferer) Infer(sql string) error {
	sql, err := i.replace(sql)

	if err != nil {
		return err
	}

	execStmt, err := i.db.Compile(sql)
	if err != nil {
		return err
	}

	plan, ok := execStmt.Plan.(plannercore.PhysicalPlan)
	if !ok {
		return fmt.Errorf("not a physical plan for sql `%s`", sql)
	}

	v := NewPlanVisitor()
	v.Visit(plan)

	for _, c := range v.Constants {
		t := v.Graph.InferType(c)
		fmt.Printf("`%s` is %s\n", c.Value.String(), EvalTypeToString(t))
	}

	return nil
}
