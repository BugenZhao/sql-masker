package mask

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
)

type Expr = expression.Expression

type CastGraph struct {
	Adj map[Node]([]Node)
}

type Node interface {
}

var _ Node = CastNode{}
var _ Node = NormalNode{}

type CastNode struct {
	Node
	left  *InferredType
	right *InferredType
}

type NormalNode struct {
	Node
	expr expression.Expression
}

func NewGraph() *CastGraph {
	return &CastGraph{
		Adj: make(map[Node]([]Node)),
	}
}

func (g *CastGraph) Add(a Expr, b Expr) bool {
	asNode := func(e Expr) Node {
		switch e := e.(type) {
		case *expression.ScalarFunction:
			if e.FuncName.L == "cast" {
				t1 := e.GetArgs()[0].GetType()
				t2 := e.GetType()
				return CastNode{
					left: NewInferredType(t1), right: NewInferredType(t2),
				}
			}
		}
		return NormalNode{expr: e}
	}

	g.add(asNode(a), asNode(b))
	return true
}

func (g *CastGraph) add(a Node, b Node) {
	g.Adj[a] = append(g.Adj[a], b)
	g.Adj[b] = append(g.Adj[b], a)
}

func (g *CastGraph) doInfer(u Node, currType *InferredType, visited map[Node]bool) []*InferredType {
	visited[u] = true
	defer func() { visited[u] = false }()

	possibleTypes := []*InferredType{}
	for _, v := range g.Adj[u] {
		if visited[v] {
			continue
		}
		switch v := v.(type) {
		case CastNode:
			if currType.Ft.EvalType() == v.left.Ft.EvalType() {
				possibleTypes = append(possibleTypes, g.doInfer(v, v.right, visited)...)
			} else if currType.Ft.EvalType() == v.right.Ft.EvalType() {
				possibleTypes = append(possibleTypes, g.doInfer(v, v.left, visited)...)
			}
		case NormalNode:
			if currType.Ft.EvalType() == v.expr.GetType().EvalType() {
				possibleTypes = append(possibleTypes, NewInferredType(v.expr.GetType()))
			} else if column, ok := v.expr.(*expression.Column); ok {
				possibleTypes = append(possibleTypes, NewInferredType(column.GetType()))
			}
		default:
		}
	}

	if len(possibleTypes) == 0 {
		possibleTypes = append(possibleTypes, currType)
	}
	return possibleTypes
}

func (g *CastGraph) InferType(c *expression.Constant) *InferredType {
	u := NormalNode{expr: c}
	t := c.GetType()
	visited := make(map[Node]bool)

	possibleTypes := g.doInfer(u, NewInferredType(t), visited)
	return possibleTypes[0]
}

type CastGraphBuilder struct {
	Constants []*expression.Constant
	Handles   []kv.Handle
	Graph     *CastGraph
}

func NewCastGraphBuilder() *CastGraphBuilder {
	return &CastGraphBuilder{
		Graph: NewGraph(),
	}
}

func (v *CastGraphBuilder) visitUpdate(update plannercore.Update) {
	v.visitPhysicalPlan(update.SelectPlan)
	for _, assignment := range update.OrderedList {
		v.Graph.Add(assignment.Col, assignment.Expr)
		v.visitExpr(assignment.Expr)
	}
}

func (v *CastGraphBuilder) visitDelete(delete plannercore.Delete) {
	v.visitPhysicalPlan(delete.SelectPlan)
}

func (v *CastGraphBuilder) visitInsert(insert plannercore.Insert) {
	v.visitPhysicalPlan(insert.SelectPlan)

	columnMap := make(map[int]*expression.Column)
	for i, colName := range insert.Columns {
		lowerName := colName.Name.L
		for _, col := range insert.Table.Cols() {
			if lowerName == col.Name.L {
				columnMap[i] = &expression.Column{
					RetType: &col.FieldType,
				}
				break
			}
		}
	}

	for _, list := range insert.Lists {
		if len(list) != len(insert.Columns) {
			continue
		}
		for i, expr := range list {
			if col, ok := columnMap[i]; ok {
				v.Graph.Add(col, expr)
				v.visitExpr(expr)
			}
		}
	}
}

func (b *CastGraphBuilder) Build(plan plannercore.Plan) error {
	switch plan := plan.(type) {
	case plannercore.PhysicalPlan:
		b.visitPhysicalPlan(plan)
	case *plannercore.Update:
		b.visitUpdate(*plan)
	case *plannercore.Delete:
		b.visitDelete(*plan)
	case *plannercore.Insert:
		b.visitInsert(*plan)
	case *plannercore.Execute:
		_ = b.Build(plan.Plan)
	case *plannercore.Simple:
	default:
		return fmt.Errorf("unrecognized plan `%T` :(", plan)
	}
	return nil
}

func (v *CastGraphBuilder) visitPhysicalPlan(plans ...plannercore.PhysicalPlan) {
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		v.visitPhysicalPlan(plan.Children()...)

		switch p := plan.(type) {
		case *plannercore.PhysicalTableReader:
			v.visitPhysicalPlan(p.TablePlans...)
		case *plannercore.PhysicalSelection:
			v.visitExpr(p.Conditions...)
		case *plannercore.PhysicalTableScan:
			v.visitExpr(p.AccessCondition...)
		case *plannercore.PhysicalProjection:
			v.visitExpr(p.Exprs...)
		case *plannercore.PointGetPlan:
			v.visitExpr(p.AccessConditions...)
			v.Handles = append(v.Handles, p.Handle)
		case *plannercore.BatchPointGetPlan:
			v.visitExpr(p.AccessConditions...)
			v.Handles = append(v.Handles, p.Handles...)
		case *plannercore.PhysicalStreamAgg:
			v.visitExpr(p.GroupByItems...)
		case *plannercore.PhysicalHashAgg:
			v.visitExpr(p.GroupByItems...)
		default:
		}
	}
}

func (v *CastGraphBuilder) visitExpr(exprs ...Expr) {
	for _, expr := range exprs {
		switch e := expr.(type) {
		case *expression.ScalarFunction:
			args := e.GetArgs()
			if e.FuncName.L == ast.Cast {
				v.Graph.Add(args[0], expr)
			} else if len(args) == 2 {
				left, right := args[0], args[1]
				if left.GetType().EvalType() == right.GetType().EvalType() {
					v.Graph.Add(left, right)
				}
			}
			for _, expr := range args {
				v.visitExpr(expr)
			}
		case *expression.Constant:
			v.Constants = append(v.Constants, e)
		}
	}
}
