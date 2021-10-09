package mask

import (
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
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
	left  *types.FieldType
	right *types.FieldType
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
					left: t1, right: t2,
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

func (g *CastGraph) doInfer(u Node, currType *types.FieldType, visited map[Node]bool) []*types.FieldType {
	visited[u] = true
	defer func() { visited[u] = false }()

	possibleTypes := []*types.FieldType{}
	for _, v := range g.Adj[u] {
		if visited[v] {
			continue
		}
		switch v := v.(type) {
		case CastNode:
			if currType.EvalType() == v.left.EvalType() {
				possibleTypes = append(possibleTypes, g.doInfer(v, v.right, visited)...)
			} else if currType.EvalType() == v.right.EvalType() {
				possibleTypes = append(possibleTypes, g.doInfer(v, v.left, visited)...)
			}
		case NormalNode:
			if currType.EvalType() == v.expr.GetType().EvalType() {
				possibleTypes = append(possibleTypes, v.expr.GetType())
			} else if column, ok := v.expr.(*expression.Column); ok {
				possibleTypes = append(possibleTypes, column.GetType())
			}
		default:
		}
	}

	if len(possibleTypes) == 0 {
		possibleTypes = append(possibleTypes, currType)
	}
	return possibleTypes
}

func (g *CastGraph) InferType(c *expression.Constant) *types.FieldType {
	u := NormalNode{expr: c}
	t := c.GetType()
	visited := make(map[Node]bool)

	possibleTypes := g.doInfer(u, t, visited)
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

func (v *CastGraphBuilder) VisitUpdate(update plannercore.Update) {
	v.Visit(update.SelectPlan)
	for _, assignment := range update.OrderedList {
		v.Graph.Add(assignment.Col, assignment.Expr)
		v.VisitExpr(assignment.Expr)
	}
}

func (v *CastGraphBuilder) VisitDelete(delete plannercore.Delete) {
	v.Visit(delete.SelectPlan)
}

func (v *CastGraphBuilder) Visit(plan plannercore.PhysicalPlan) {
	for _, child := range plan.Children() {
		v.Visit(child)
	}

	switch p := plan.(type) {
	case *plannercore.PhysicalTableReader:
		for _, plan := range p.TablePlans {
			v.Visit(plan)
		}
	case *plannercore.PhysicalSelection:
		for _, expr := range p.Conditions {
			v.VisitExpr(expr)
		}
	case *plannercore.PointGetPlan:
		handle := p.Handle
		v.Handles = append(v.Handles, handle)
	default:
	}
}

func (v *CastGraphBuilder) VisitExpr(expr Expr) {
	switch e := expr.(type) {
	case *expression.ScalarFunction:
		args := e.GetArgs()
		if e.FuncName.L == "cast" {
			v.Graph.Add(args[0], expr)
		} else if len(args) == 2 {
			left, right := args[0], args[1]
			if left.GetType().EvalType() == right.GetType().EvalType() {
				v.Graph.Add(left, right)
			}
		}
		for _, expr := range args {
			v.VisitExpr(expr)
		}
	case *expression.Constant:
		v.Constants = append(v.Constants, e)
	}
}

func (v *CastGraphBuilder) Print() {
	for _, c := range v.Constants {
		fmt.Printf("%T(%v) ", c, c)
	}
	fmt.Println()
}
