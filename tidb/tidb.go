package tidb

import (
	"context"
	"fmt"

	"github.com/pingcap/parser/ast"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
)

const (
	DefaultSocket   = "/tmp/sql-masker-tidb-socket"
	DefaultConnOpts = "charset=utf8mb4"
)

func ParseOne(ctx context.Context, qc *server.TiDBContext, sql string) (ast.StmtNode, error) {
	stmts, err := qc.Parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("multiple stmt found")
	}
	return stmts[0], nil
}

func Execute(ctx context.Context, qc *server.TiDBContext, sql string) (server.ResultSet, error) {
	stmt, err := ParseOne(ctx, qc, sql)
	if err != nil {
		return nil, err
	}
	return qc.ExecuteStmt(ctx, stmt)
}

func Compile(ctx context.Context, qc *server.TiDBContext, sql string) (*executor.ExecStmt, error) {
	stmt, err := ParseOne(ctx, qc, sql)
	if err != nil {
		return nil, err
	}
	compiler := executor.Compiler{Ctx: qc.Session}
	execStmt, err := compiler.Compile(ctx, stmt)
	if err != nil {
		return nil, err
	}
	return execStmt, nil
}

func Play() error {
	storage, err := mockstore.NewMockStore()
	if err != nil {
		return err
	}
	session.DisableStats4Test()
	session.BootstrapSession(storage)
	driver := server.NewTiDBDriver(storage)

	qctx, err := driver.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	if err != nil {
		return err
	}

	ctx := context.Background()

	executeSqls := []string{
		"use test;",
		"create table test.t(id int primary key, name varchar(24), birth datetime, deci decimal(6,2));",
		"insert into test.t values (1, '233', '2021-09-30 12:34:56', 12.34);",
	}

	for _, sql := range executeSqls {
		_, err = Execute(ctx, qctx, sql)
		if err != nil {
			return err
		}
	}

	// reader := bufio.NewReader(os.Stdin)

	compileSqls := []string{
		// "select * from t where name = 233;", // table reader
		"select * from t where birth between 2021 and '2022'",
		// "select * from t where name between 200 and 300;",
		"select * from t where deci >= 1234",
		// "select * from t where id = '23';",                // point get
		// "select * from t where id = '23' and name = 233;", // selection with child (point get)
	}

	for _, sql := range compileSqls {
		// sql, err := reader.ReadString('\n')
		// if err != nil {
		// 	return err
		// }
		fmt.Println()
		fmt.Println(sql)
		execStmt, err := Compile(ctx, qctx, sql)
		if err != nil {
			fmt.Println(err)
			continue
		}
		plan := execStmt.Plan.(plannercore.PhysicalPlan)

		v := NewMyVisitor()
		v.Visit(plan)
		// v.Print()
		for _, c := range v.Constants {
			t := v.Graph.InferType(c)
			// fmt.Println(c.Value, t)
			fmt.Printf("`%s` is %s\n", c.Value.String(), EvalTypeToString(t))
		}
	}

	return nil
}

func EvalTypeToString(t types.EvalType) string {
	switch t {
	case types.ETInt:
		return "Int"
	case types.ETReal:
		return "Real"
	case types.ETDecimal:
		return "Decimal"
	case types.ETString:
		return "String"
	case types.ETDatetime:
		return "Datetime"
	case types.ETTimestamp:
		return "Timestamp"
	case types.ETDuration:
		return "Duration"
	case types.ETJson:
		return "Json"
	default:
		return ""
	}
}

type Expr = expression.Expression

type Graph struct {
	Adj map[Node]([]Node)
}

type Node interface {
}

type CastNode struct {
	Node
	left  types.EvalType
	right types.EvalType
}

type NormalNode struct {
	Node
	expr expression.Expression
}

func NewGraph() Graph {
	return Graph{
		Adj: make(map[Node]([]Node)),
	}
}

func (g *Graph) Add(a Expr, b Expr) bool {
	asNode := func(e Expr) Node {
		switch e := e.(type) {
		case *expression.ScalarFunction:
			if e.FuncName.L == "cast" {
				t1 := e.GetArgs()[0].GetType().EvalType()
				t2 := e.GetType().EvalType()
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

func (g *Graph) add(a Node, b Node) {
	g.Adj[a] = append(g.Adj[a], b)
	g.Adj[b] = append(g.Adj[b], a)
}

func (g *Graph) doInfer(u Node, currentT types.EvalType, visited map[Node]bool) []types.EvalType {
	visited[u] = true
	defer func() { visited[u] = false }()

	possibleTypes := []types.EvalType{}
	for _, v := range g.Adj[u] {
		if visited[v] {
			continue
		}
		switch v := v.(type) {
		case CastNode:
			if currentT == v.left {
				possibleTypes = append(possibleTypes, g.doInfer(v, v.right, visited)...)
			} else if currentT == v.right {
				possibleTypes = append(possibleTypes, g.doInfer(v, v.left, visited)...)
			}
		default:
		}
	}

	if len(possibleTypes) == 0 {
		possibleTypes = append(possibleTypes, currentT)
	}
	return possibleTypes
}

func (g *Graph) InferType(c *expression.Constant) types.EvalType {
	u := NormalNode{expr: c}
	t := c.GetType().EvalType()
	visited := make(map[Node]bool)

	possibleTypes := g.doInfer(u, t, visited)
	return possibleTypes[0]
}

type MyVisitor struct {
	Constants []*expression.Constant
	Handles   []interface{}
	Graph     Graph
}

func NewMyVisitor() MyVisitor {
	return MyVisitor{
		Graph: NewGraph(),
	}
}

func (v *MyVisitor) Visit(plan plannercore.PhysicalPlan) {
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

func (v *MyVisitor) VisitExpr(expr Expr) {
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

func (v *MyVisitor) Print() {
	for _, c := range v.Constants {
		fmt.Printf("%T(%v) ", c, c)
	}
	fmt.Println()
}
