package main

import (
	"bufio"
	"io"
	"os"
	"strings"

	"github.com/BugenZhao/sql-masker/mask"
	"github.com/BugenZhao/sql-masker/tidb"
	"github.com/pingcap/parser/ast"
)

type TaskResult struct {
	from  string
	to    string
	stats *mask.Stats
	err   error
}

func ReadSQLs(out chan<- string, sqlPaths ...string) {
	defer close(out)

	for _, path := range sqlPaths {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		for {
			sql, err := reader.ReadString(';')
			if err == io.EOF {
				break
			} else if err != nil {
				return
			}
			out <- strings.TrimSpace(sql)
		}
	}
}

var (
	_ tidb.StmtTransform = filterOutConstraints
)

func filterOutConstraints(s ast.StmtNode) ast.StmtNode {
	if s, ok := s.(*ast.CreateTableStmt); ok {
		s.Constraints = []*ast.Constraint{}
		// todo: check whether required to filter out column options like `primary key`, since we may be able to handle it ?

		// for _, col := range s.Cols {
		// 	options := []*ast.ColumnOption{}
		// 	for _, option := range col.Options {
		// 		switch option.Tp {
		// 		case ast.ColumnOptionUniqKey, ast.ColumnOptionPrimaryKey:
		// 		default:
		// 			options = append(options, option)
		// 		}
		// 	}
		// 	col.Options = options
		// }
	}
	return s
}
