package tidb

import (
	"fmt"

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

func (i *Inferer) replace(sql string) string {
	return sql
}

func (i *Inferer) Infer(sql string) error {
	sql = i.replace(sql)

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
