package main

import (
	"fmt"
	"strings"

	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

type MyVisitor struct {
	depth    int
	colNames []string
}

func (v *MyVisitor) Log(in ast.Node) {
	indent := strings.Repeat("  ", v.depth)
	fmt.Printf("%v%T: %v, %v\n", indent, in, in.Text(), in)
}

func (v *MyVisitor) Enter(in ast.Node) (ast.Node, bool) {
	v.depth += 1
	v.Log(in)
	if name, ok := in.(*ast.ColumnName); ok {
		v.colNames = append(v.colNames, name.OrigColName())
	}
	return in, false
}

func (v *MyVisitor) Leave(in ast.Node) (ast.Node, bool) {
	v.depth -= 1
	return in, true
}

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

func extract(rootNode *ast.StmtNode) []string {
	v := MyVisitor{}
	(*rootNode).Accept(&v)
	return v.colNames
}

func main() {
	err := tidb.Play()
	if err != nil {
		panic(err)
	}
	return

	sqls := []string{
		"select grade g from student S where S.name = 'bugen'",
		"UPDATE warehouse SET w_ytd = w_ytd + 'some amount' WHERE w_id = 'some id'",
	}
	for _, sql := range sqls {
		fmt.Println(sql)
		node, err := parse(sql)
		if err != nil {
			fmt.Printf("parse error: %v\n", err.Error())
		}
		names := extract(node)
		fmt.Println(names)
	}
}
